"""PlayerResolver — fetch rosters and resolve player names."""

from __future__ import annotations

import logging
from typing import Any

from nhlposts.api.client import NHLClient, NotFoundError
from nhlposts.api.endpoints import NHLEndpoints
from nhlposts.cache.store import CacheStore
from nhlposts.models.player import Player
from nhlposts.ingestion.season import ALL_TEAMS

logger = logging.getLogger(__name__)

# NHL roster position types
_POSITION_SECTIONS = ("forwards", "defensemen", "goalies")


def _position_group(section: str) -> str:
    if section == "forwards":
        return "F"
    if section == "defensemen":
        return "D"
    return "G"


def _parse_roster(
    data: dict[str, Any],
    team_abbrev: str,
    team_id: int,
) -> list[Player]:
    players: list[Player] = []
    for section in _POSITION_SECTIONS:
        group = _position_group(section)
        for entry in data.get(section, []):
            try:
                pid = entry["id"]
                first = entry.get("firstName", {}).get("default", "")
                last = entry.get("lastName", {}).get("default", "")
                pos = entry.get("positionCode", group[0])
                shoots = entry.get("shootsCatches", "")
                players.append(
                    Player(
                        player_id=pid,
                        first_name=first,
                        last_name=last,
                        position_code=pos,
                        position_group=group,
                        team_abbrev=team_abbrev,
                        team_id=team_id,
                        shoots=shoots,
                    )
                )
            except (KeyError, TypeError) as exc:
                logger.debug("Skipping roster entry: %s (%s)", entry, exc)
    return players


class PlayerResolver:
    """
    Fetches team rosters and provides player name lookup.

    Uses rapidfuzz for fuzzy name matching when the user types
    a partial or misspelled name.
    """

    def __init__(self, store: CacheStore, client: NHLClient) -> None:
        self._store = store
        self._ep = NHLEndpoints(client)
        self._name_cache: dict[int, Player] | None = None

    def fetch_all_rosters(self, season: str) -> int:
        """
        Fetch rosters for all 32 teams and populate the players table.

        Returns the number of players added/updated.
        """
        cache_key = f"rosters:{season}"
        if self._store.is_cached(cache_key, ttl_hours=24):
            logger.info("Rosters for %s already cached", season)
            return 0

        all_players: dict[int, Player] = {}

        for team in ALL_TEAMS:
            try:
                data = self._ep.get_roster(team, season)
            except NotFoundError:
                logger.debug("No roster for %s in %s", team, season)
                continue
            except Exception as exc:
                logger.warning("Error fetching roster for %s: %s", team, exc)
                continue

            # Try to extract team id from first player's team entry or use 0
            team_id = 0
            players = _parse_roster(data, team, team_id)
            for p in players:
                all_players[p.player_id] = p

        self._store.upsert_players(list(all_players.values()))
        self._store.put_raw(cache_key, {"count": len(all_players)}, ttl_hours=24)
        self._name_cache = None  # invalidate name cache
        logger.info("Fetched %d players for season %s", len(all_players), season)
        return len(all_players)

    def _load_name_cache(self) -> dict[int, Player]:
        if self._name_cache is None:
            rows = self._store.get_all_players()
            self._name_cache = {
                r["player_id"]: Player(
                    player_id=r["player_id"],
                    first_name=r["first_name"],
                    last_name=r["last_name"],
                    position_code=r["position_code"],
                    position_group=r["position_group"],
                    team_abbrev=r["team_abbrev"],
                    team_id=r["team_id"],
                    shoots=r["shoots"],
                )
                for r in rows
            }
        return self._name_cache

    def get_player(self, player_id: int) -> Player | None:
        cache = self._load_name_cache()
        return cache.get(player_id)

    def resolve_name(self, name: str, threshold: int = 70) -> list[Player]:
        """
        Fuzzy-match a player name against all known players.

        Args:
            name: Full or partial player name.
            threshold: Minimum rapidfuzz score (0-100).

        Returns:
            List of matching Player objects, best match first.
        """
        try:
            from rapidfuzz import process, fuzz
        except ImportError:
            logger.warning("rapidfuzz not installed; falling back to exact match")
            return self._exact_match(name)

        cache = self._load_name_cache()
        choices = {pid: p.full_name for pid, p in cache.items()}

        results = process.extract(
            name,
            choices,
            scorer=fuzz.WRatio,
            limit=10,
            score_cutoff=threshold,
        )

        players = []
        for _match_str, score, pid in results:
            p = cache.get(pid)
            if p:
                players.append((score, p))

        players.sort(key=lambda x: x[0], reverse=True)
        return [p for _, p in players]

    def _exact_match(self, name: str) -> list[Player]:
        cache = self._load_name_cache()
        name_lower = name.lower()
        return [
            p for p in cache.values()
            if name_lower in p.full_name.lower()
        ]

    def fetch_games_played_for_players(
        self,
        player_season_pairs: list[tuple[int, str]],
        game_type: int = 2,
        progress_callback=None,
    ) -> int:
        """
        Fetch the game log for each (player_id, season) pair and store it
        in the player_game_log table so Aggregator can compute accurate GP.

        Returns the number of players newly fetched.
        """
        fetched = 0
        total = len(player_season_pairs)
        for i, (player_id, season) in enumerate(player_season_pairs):
            if self._store.is_player_gp_fetched(player_id, season, game_type):
                if progress_callback:
                    progress_callback(i + 1, total)
                continue
            try:
                data = self._ep.get_player_game_log(player_id, season, game_type)
                games = data.get("gameLog", [])
                rows = [
                    (player_id, g["gameId"], g["gameDate"], season, game_type,
                     int(g.get("shots", 0)))
                    for g in games
                    if "gameId" in g and "gameDate" in g
                ]
                self._store.bulk_upsert_player_game_log(rows)
                self._store.mark_player_gp_fetched(player_id, season, game_type)
                fetched += 1
            except Exception as exc:
                logger.warning(
                    "Could not fetch game log for player %d season %s: %s",
                    player_id, season, exc,
                )
            if progress_callback:
                progress_callback(i + 1, total)
        return fetched

    def ensure_player_known(self, player_id: int, season: str) -> Player | None:
        """
        If a player is not in the DB, fetch their profile from the API.
        Used when play-by-play references a player not in any cached roster.
        """
        if self.get_player(player_id):
            return self.get_player(player_id)

        cache_key = f"player:{player_id}"
        raw = self._store.get_raw(cache_key)
        if raw is None:
            try:
                raw = self._ep.get_player_landing(player_id)
                self._store.put_raw(cache_key, raw, ttl_hours=24)
            except Exception as exc:
                logger.warning("Could not fetch player %d: %s", player_id, exc)
                return None

        try:
            first = raw.get("firstName", {}).get("default", "")
            last = raw.get("lastName", {}).get("default", "")
            pos = raw.get("position", "")
            shoots = raw.get("shootsCatches", "")
            from nhlposts.models.player import _POSITION_GROUP
            group = _POSITION_GROUP.get(pos.upper(), "F")
            team_info = raw.get("currentTeamAbbrev", "")

            player = Player(
                player_id=player_id,
                first_name=first,
                last_name=last,
                position_code=pos,
                position_group=group,
                team_abbrev=team_info,
                team_id=raw.get("currentTeamId", 0),
                shoots=shoots,
            )
            self._store.upsert_player(player)
            self._name_cache = None
            return player
        except Exception as exc:
            logger.warning("Could not parse player %d landing: %s", player_id, exc)
            return None
