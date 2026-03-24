"""CacheStore: read/write/TTL operations on the SQLite database."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .db import get_connection

if TYPE_CHECKING:
    from nhlposts.models.event import PostShotEvent
    from nhlposts.models.game import Game
    from nhlposts.models.player import Player


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CacheStore:
    """
    Thread-safe cache store backed by SQLite.

    All public methods acquire a lock so they are safe to call
    from the ThreadPoolExecutor in GameIngester.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn = get_connection(db_path)
        self._lock = threading.Lock()

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ #
    # Raw JSON blob cache                                                  #
    # ------------------------------------------------------------------ #

    def is_cached(self, key: str, ttl_hours: int | None = None) -> bool:
        """Return True if key exists and has not expired."""
        with self._lock:
            row = self._conn.execute(
                "SELECT fetched_at, ttl_hours FROM cache_metadata WHERE key=?", (key,)
            ).fetchone()
        if row is None:
            return False
        if row["ttl_hours"] == 0:
            return True  # forever
        if ttl_hours is not None:
            effective_ttl = ttl_hours
        else:
            effective_ttl = row["ttl_hours"]
        if effective_ttl == 0:
            return True
        fetched = datetime.fromisoformat(row["fetched_at"])
        age_hours = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
        return age_hours < effective_ttl

    def get_raw(self, key: str) -> dict[str, Any] | None:
        """Return cached JSON payload or None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT payload FROM raw_cache WHERE key=?", (key,)
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["payload"])

    def put_raw(self, key: str, payload: dict[str, Any], ttl_hours: int) -> None:
        """Store a raw JSON payload with a TTL."""
        now = _now_iso()
        text = json.dumps(payload)
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO raw_cache(key, payload, fetched_at) VALUES(?,?,?)",
                (key, text, now),
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO cache_metadata(key, fetched_at, ttl_hours) VALUES(?,?,?)",
                (key, now, ttl_hours),
            )
            self._conn.commit()

    # ------------------------------------------------------------------ #
    # Games                                                                #
    # ------------------------------------------------------------------ #

    def upsert_game(self, game: "Game") -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO games
                    (game_id, season, game_type, game_date,
                     home_team_id, home_team_abbrev,
                     away_team_id, away_team_abbrev, game_state)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    game.game_id, game.season, game.game_type, game.game_date,
                    game.home_team_id, game.home_team_abbrev,
                    game.away_team_id, game.away_team_abbrev, game.game_state,
                ),
            )
            self._conn.commit()

    def upsert_games(self, games: list["Game"]) -> None:
        rows = [
            (
                g.game_id, g.season, g.game_type, g.game_date,
                g.home_team_id, g.home_team_abbrev,
                g.away_team_id, g.away_team_abbrev, g.game_state,
            )
            for g in games
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO games
                    (game_id, season, game_type, game_date,
                     home_team_id, home_team_abbrev,
                     away_team_id, away_team_abbrev, game_state)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            self._conn.commit()

    def mark_game_ingested(self, game_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE games SET ingested=1 WHERE game_id=?", (game_id,)
            )
            self._conn.commit()

    def get_pending_games(
        self,
        season: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[sqlite3.Row]:
        """Return games not yet ingested, optionally filtered."""
        clauses = ["ingested=0", "game_state='OFF'"]
        params: list[Any] = []
        if season:
            clauses.append("season=?")
            params.append(season)
        if date_from:
            clauses.append("game_date>=?")
            params.append(date_from)
        if date_to:
            clauses.append("game_date<=?")
            params.append(date_to)
        sql = f"SELECT * FROM games WHERE {' AND '.join(clauses)} ORDER BY game_date"
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def get_all_games(
        self,
        season: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[sqlite3.Row]:
        """Return all games (ingested or not), optionally filtered."""
        clauses: list[str] = []
        params: list[Any] = []
        if season:
            clauses.append("season=?")
            params.append(season)
        if date_from:
            clauses.append("game_date>=?")
            params.append(date_from)
        if date_to:
            clauses.append("game_date<=?")
            params.append(date_to)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM games {where} ORDER BY game_date"
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def season_stats(self, season: str) -> dict[str, int]:
        """Return ingestion statistics for a season."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(ingested) as ingested,
                    COUNT(*)-SUM(ingested) as pending
                FROM games WHERE season=?
                """,
                (season,),
            ).fetchone()
        return dict(row) if row else {"total": 0, "ingested": 0, "pending": 0}

    # ------------------------------------------------------------------ #
    # Players                                                              #
    # ------------------------------------------------------------------ #

    def upsert_player(self, player: "Player") -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO players
                    (player_id, first_name, last_name, position_code,
                     position_group, team_abbrev, team_id, shoots)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    player.player_id, player.first_name, player.last_name,
                    player.position_code, player.position_group,
                    player.team_abbrev, player.team_id, player.shoots,
                ),
            )
            self._conn.commit()

    def upsert_players(self, players: list["Player"]) -> None:
        rows = [
            (
                p.player_id, p.first_name, p.last_name,
                p.position_code, p.position_group,
                p.team_abbrev, p.team_id, p.shoots,
            )
            for p in players
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT OR REPLACE INTO players
                    (player_id, first_name, last_name, position_code,
                     position_group, team_abbrev, team_id, shoots)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            self._conn.commit()

    def get_player(self, player_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM players WHERE player_id=?", (player_id,)
            ).fetchone()

    def get_all_players(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute("SELECT * FROM players").fetchall()

    # ------------------------------------------------------------------ #
    # Post shots                                                           #
    # ------------------------------------------------------------------ #

    def bulk_upsert_post_shots(self, events: list["PostShotEvent"]) -> None:
        if not events:
            return
        rows = [
            (
                e.event_id, e.game_id, e.season, e.game_date,
                e.period, e.period_type, e.time_in_period, e.time_seconds,
                e.reason, e.shot_type, e.x_coord, e.y_coord, e.zone_code,
                e.away_skaters, e.home_skaters,
                int(e.away_goalie_in_net), int(e.home_goalie_in_net),
                e.strength, e.strength_state,
                e.shooting_player_id, e.goalie_in_net_id,
                e.event_owner_team_id, e.home_team_id, e.away_team_id,
                int(e.is_home),
            )
            for e in events
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO post_shots
                    (event_id, game_id, season, game_date,
                     period, period_type, time_in_period, time_seconds,
                     reason, shot_type, x_coord, y_coord, zone_code,
                     away_skaters, home_skaters,
                     away_goalie_in_net, home_goalie_in_net,
                     strength, strength_state,
                     shooting_player_id, goalie_in_net_id,
                     event_owner_team_id, home_team_id, away_team_id,
                     is_home)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            self._conn.commit()

    # ------------------------------------------------------------------ #
    # Player game log (actual GP per player per season)                   #
    # ------------------------------------------------------------------ #

    def bulk_upsert_player_game_log(
        self, rows: list[tuple[int, int, str, str, int, int]]
    ) -> None:
        """Upsert game log rows. rows: (player_id, game_id, game_date, season, game_type, shots)"""
        if not rows:
            return
        with self._lock:
            self._conn.executemany(
                """
                INSERT OR REPLACE INTO player_game_log
                    (player_id, game_id, game_date, season, game_type, shots)
                VALUES (?,?,?,?,?,?)
                """,
                rows,
            )
            self._conn.commit()

    def is_player_gp_fetched(self, player_id: int, season: str, game_type: int) -> bool:
        key = f"gp:{player_id}:{season}:{game_type}"
        return self.is_cached(key, ttl_hours=24)

    def mark_player_gp_fetched(self, player_id: int, season: str, game_type: int) -> None:
        key = f"gp:{player_id}:{season}:{game_type}"
        self.put_raw(key, {}, ttl_hours=24)

    def get_distinct_player_seasons(
        self,
        season: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[tuple[int, str]]:
        """Return (player_id, season) pairs that have post shots, for GP fetching."""
        clauses: list[str] = []
        params: list[Any] = []
        if season:
            clauses.append("season=?")
            params.append(season)
        if date_from:
            clauses.append("game_date>=?")
            params.append(date_from)
        if date_to:
            clauses.append("game_date<=?")
            params.append(date_to)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT DISTINCT shooting_player_id, season FROM post_shots {where}"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [(r[0], r[1]) for r in rows]

    def get_connection(self) -> sqlite3.Connection:
        """Expose raw connection for read-only pandas queries."""
        return self._conn

    # ------------------------------------------------------------------ #
    # Cache management                                                     #
    # ------------------------------------------------------------------ #

    def clear_season(self, season: str) -> None:
        """Remove all cached data for a season."""
        with self._lock:
            self._conn.execute("DELETE FROM post_shots WHERE season=?", (season,))
            self._conn.execute("DELETE FROM games WHERE season=?", (season,))
            # Remove raw_cache entries for PBP of games in this season
            self._conn.execute(
                "DELETE FROM raw_cache WHERE key LIKE ?", (f"pbp:{season[:4]}%",)
            )
            self._conn.execute(
                "DELETE FROM cache_metadata WHERE key LIKE ?", (f"pbp:{season[:4]}%",)
            )
            self._conn.commit()

    def clear_all(self) -> None:
        with self._lock:
            for table in ("post_shots", "games", "players", "raw_cache", "cache_metadata"):
                self._conn.execute(f"DELETE FROM {table}")
            self._conn.commit()

    def cache_summary(self) -> list[dict[str, Any]]:
        """Return per-season summary of cached data."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT season,
                       COUNT(*) as total_games,
                       SUM(ingested) as ingested_games
                FROM games
                GROUP BY season
                ORDER BY season
                """
            ).fetchall()
            shot_rows = self._conn.execute(
                """
                SELECT season, COUNT(*) as post_shots
                FROM post_shots
                GROUP BY season
                ORDER BY season
                """
            ).fetchall()
        shot_map = {r["season"]: r["post_shots"] for r in shot_rows}
        result = []
        for r in rows:
            result.append({
                "season": r["season"],
                "total_games": r["total_games"],
                "ingested_games": r["ingested_games"],
                "post_shots": shot_map.get(r["season"], 0),
            })
        return result
