"""GameIngester — fetch and parse play-by-play data concurrently."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from nhlposts.api.client import NHLClient, NotFoundError
from nhlposts.api.endpoints import NHLEndpoints
from nhlposts.cache.store import CacheStore
from nhlposts.models.event import PostShotEvent, POST_REASONS

logger = logging.getLogger(__name__)

# situationCode positions
# [0] away_goalie  [1] away_skaters  [2] home_skaters  [3] home_goalie
_TYPECODE_MISSED_SHOT = 507


def _parse_situation(code: str, shooter_is_home: bool) -> tuple[str, str]:
    """
    Parse a 4-char situationCode into (strength, strength_state).

    Examples:
        "1551" -> 5v5 EV
        "1541" -> 5v4 PP (for home shooter) or 4v5 PK (for away shooter)
    """
    if not code or len(code) != 4:
        return "5v5", "EV"

    try:
        away_goalie = int(code[0])
        away_sk = int(code[1])
        home_sk = int(code[2])
        home_goalie = int(code[3])
    except ValueError:
        return "5v5", "EV"

    if shooter_is_home:
        shooter_sk, opp_sk = home_sk, away_sk
        opp_goalie = away_goalie
    else:
        shooter_sk, opp_sk = away_sk, home_sk
        opp_goalie = home_goalie

    strength = f"{shooter_sk}v{opp_sk}"

    if opp_goalie == 0:
        state = "EN"
    elif shooter_sk > opp_sk:
        state = "PP"
    elif shooter_sk < opp_sk:
        state = "PK"
    else:
        state = "EV"

    return strength, state


def _event_game_seconds(period: int, time_in_period: str, period_type: str) -> int:
    """Convert period + time into total game seconds elapsed."""
    try:
        mins_str, secs_str = time_in_period.split(":")
        period_secs = int(mins_str) * 60 + int(secs_str)
    except (ValueError, AttributeError):
        return 0

    if period_type == "OT":
        # Regulation = 3 * 20 min
        return 3 * 20 * 60 + period_secs
    elif period_type == "SO":
        return 3 * 20 * 60 + 5 * 60 + period_secs

    return max(0, period - 1) * 20 * 60 + period_secs


def _parse_post_shots(pbp: dict[str, Any]) -> list[PostShotEvent]:
    """
    Extract post/crossbar shot events from a play-by-play response.

    Returns a list of PostShotEvent objects.
    """
    events: list[PostShotEvent] = []

    game_id = pbp.get("id", 0)
    home_team = pbp.get("homeTeam", {})
    away_team = pbp.get("awayTeam", {})
    home_team_id = home_team.get("id", 0)
    away_team_id = away_team.get("id", 0)

    # Derive season from game_id
    season_year = str(game_id)[:4]
    season = season_year + str(int(season_year) + 1)

    # Game date from game object (may be in plays or top-level)
    game_date = pbp.get("gameDate", "")[:10]

    for play in pbp.get("plays", []):
        # Filter to missed shots only
        if play.get("typeCode") != _TYPECODE_MISSED_SHOT:
            continue

        details = play.get("details", {})
        reason = details.get("reason", "")

        if reason not in POST_REASONS:
            continue

        period_desc = play.get("periodDescriptor", {})
        period = period_desc.get("number", 1)
        period_type = period_desc.get("periodType", "REG")
        time_in_period = play.get("timeInPeriod", "0:00")
        time_seconds = _event_game_seconds(period, time_in_period, period_type)

        situation_code = play.get("situationCode", "1551")
        shooter_id = details.get("shootingPlayerId", 0)
        event_owner_team_id = details.get("eventOwnerTeamId", 0)
        is_home = event_owner_team_id == home_team_id

        strength, strength_state = _parse_situation(situation_code, is_home)

        # Parse situation code for raw skater/goalie counts
        sc = situation_code if (situation_code and len(situation_code) == 4) else "1551"
        try:
            away_goalie_in_net = int(sc[0]) == 1
            away_skaters = int(sc[1])
            home_skaters = int(sc[2])
            home_goalie_in_net = int(sc[3]) == 1
        except (ValueError, IndexError):
            away_goalie_in_net = True
            away_skaters = 5
            home_skaters = 5
            home_goalie_in_net = True

        events.append(
            PostShotEvent(
                event_id=play.get("eventId", 0),
                game_id=game_id,
                season=season,
                game_date=game_date,
                period=period,
                period_type=period_type,
                time_in_period=time_in_period,
                time_seconds=time_seconds,
                reason=reason,
                shot_type=details.get("shotType", ""),
                x_coord=details.get("xCoord"),
                y_coord=details.get("yCoord"),
                zone_code=details.get("zoneCode", ""),
                away_skaters=away_skaters,
                home_skaters=home_skaters,
                away_goalie_in_net=away_goalie_in_net,
                home_goalie_in_net=home_goalie_in_net,
                strength=strength,
                strength_state=strength_state,
                shooting_player_id=shooter_id,
                goalie_in_net_id=details.get("goalieInNetId"),
                event_owner_team_id=event_owner_team_id,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                is_home=is_home,
            )
        )

    return events


@dataclass
class IngestResult:
    games_processed: int = 0
    games_skipped: int = 0
    games_failed: int = 0
    post_shots_found: int = 0


class GameIngester:
    """
    Fetches play-by-play for batches of games, parses post shot events,
    and stores them in the cache.

    Uses a thread pool for concurrent fetching with a rate-limit semaphore.
    """

    def __init__(
        self,
        store: CacheStore,
        client: NHLClient,
        workers: int = 4,
    ) -> None:
        self._store = store
        self._ep = NHLEndpoints(client)
        self._workers = workers
        # Semaphore ensures the rate limit is respected across all threads
        # (NHLClient already has its own lock, but we add an outer bound)
        self._rate_sem = threading.BoundedSemaphore(workers)

    def ingest_game(self, game_id: int, game_date: str = "", season: str = "") -> int:
        """
        Ingest a single game. Returns count of post shots found.
        Skips if already ingested.
        """
        from nhlposts.models.game import Game

        cache_key = f"pbp:{game_id}"

        # Check if already ingested via a targeted query
        conn = self._store.get_connection()
        row = conn.execute(
            "SELECT ingested FROM games WHERE game_id=?", (game_id,)
        ).fetchone()
        if row and row["ingested"]:
            logger.debug("Game %d already ingested, skipping", game_id)
            return 0

        # Try to get from raw cache first
        pbp = self._store.get_raw(cache_key)

        if pbp is None:
            try:
                pbp = self._ep.get_play_by_play(game_id)
                # Completed games are cached forever (ttl=0)
                game_state = pbp.get("gameState", "LIVE")
                ttl = 0 if game_state in ("OFF", "FINAL") else 1
                self._store.put_raw(cache_key, pbp, ttl_hours=ttl)
            except NotFoundError:
                logger.warning("Game %d not found", game_id)
                return 0
            except Exception as exc:
                logger.warning("Error fetching game %d: %s", game_id, exc)
                return 0

        # Ensure the game row exists (needed for FK and for mark_game_ingested)
        home = pbp.get("homeTeam", {})
        away = pbp.get("awayTeam", {})
        gid_str = str(game_id)
        season_year = gid_str[:4]
        g_season = season or (season_year + str(int(season_year) + 1))
        g_type = int(gid_str[4:6]) if len(gid_str) >= 6 else 2
        g_date = game_date or pbp.get("gameDate", "")[:10]
        g_state = pbp.get("gameState", "OFF")

        from nhlposts.models.game import Game
        self._store.upsert_game(Game(
            game_id=game_id,
            season=g_season,
            game_type=g_type,
            game_date=g_date,
            home_team_id=home.get("id", 0),
            home_team_abbrev=home.get("abbrev", ""),
            away_team_id=away.get("id", 0),
            away_team_abbrev=away.get("abbrev", ""),
            game_state=g_state,
        ))

        events = _parse_post_shots(pbp)
        self._store.bulk_upsert_post_shots(events)
        self._store.mark_game_ingested(game_id)

        logger.debug("Game %d: %d post shots", game_id, len(events))
        return len(events)

    def ingest_batch(
        self,
        game_ids: list[int],
        progress_callback: Any | None = None,
    ) -> IngestResult:
        """
        Ingest a batch of games concurrently.

        Args:
            game_ids: List of game IDs to process.
            progress_callback: Optional callable(game_id, post_shots_found)
                               called after each game completes.

        Returns:
            IngestResult summary.
        """
        result = IngestResult()

        if not game_ids:
            return result

        def _worker(gid: int) -> tuple[int, int]:
            """Returns (game_id, post_shots_count) or raises."""
            return gid, self.ingest_game(gid)

        with ThreadPoolExecutor(max_workers=self._workers) as executor:
            futures = {executor.submit(_worker, gid): gid for gid in game_ids}

            for future in as_completed(futures):
                gid = futures[future]
                try:
                    _, shots = future.result()
                    result.games_processed += 1
                    result.post_shots_found += shots
                    if progress_callback:
                        progress_callback(gid, shots)
                except Exception as exc:
                    result.games_failed += 1
                    logger.warning("Failed to ingest game %d: %s", gid, exc)

        return result
