"""Aggregator — runs SQL queries and returns pandas DataFrames."""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd

from nhlposts.cache.store import CacheStore
from nhlposts.analysis.filters import FilterSpec, build_where_clause

_BASE_JOIN = """
    FROM post_shots ps
    LEFT JOIN players p ON p.player_id = ps.shooting_player_id
    LEFT JOIN games g ON g.game_id = ps.game_id
"""


def _query(conn: sqlite3.Connection, sql: str, params: list[Any]) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def _shots_subquery(spec: FilterSpec) -> tuple[str, list[Any]]:
    """
    Correlated subquery summing a player's total shots on goal from
    player_game_log, respecting the same season/date/game-type filters.
    Returns NULL when no game log data exists (shots = 0 → NULLIF).
    """
    clauses = ["pgl.player_id = ps.shooting_player_id"]
    params: list[Any] = []

    if spec.seasons:
        placeholders = ",".join("?" * len(spec.seasons))
        clauses.append(f"pgl.season IN ({placeholders})")
        params.extend(spec.seasons)
    if spec.date_from:
        clauses.append("pgl.game_date >= ?")
        params.append(spec.date_from)
    if spec.date_to:
        clauses.append("pgl.game_date <= ?")
        params.append(spec.date_to)
    if spec.season_type is not None:
        clauses.append("pgl.game_type = ?")
        params.append(spec.season_type)

    where = " AND ".join(clauses)
    sql = f"(SELECT NULLIF(SUM(pgl.shots), 0) FROM player_game_log pgl WHERE {where})"
    return sql, params


def _gp_subquery(spec: FilterSpec) -> tuple[str, list[Any]]:
    """
    Build a correlated subquery that counts the player's actual games played
    from player_game_log, respecting season / date / game-type filters.

    References ps.shooting_player_id from the outer query.
    Returns 0 (→ NULL via NULLIF) when no game log has been fetched yet.
    """
    clauses = ["pgl.player_id = ps.shooting_player_id"]
    params: list[Any] = []

    if spec.seasons:
        placeholders = ",".join("?" * len(spec.seasons))
        clauses.append(f"pgl.season IN ({placeholders})")
        params.extend(spec.seasons)
    if spec.date_from:
        clauses.append("pgl.game_date >= ?")
        params.append(spec.date_from)
    if spec.date_to:
        clauses.append("pgl.game_date <= ?")
        params.append(spec.date_to)
    if spec.season_type is not None:
        clauses.append("pgl.game_type = ?")
        params.append(spec.season_type)

    where = " AND ".join(clauses)
    sql = f"(SELECT COUNT(*) FROM player_game_log pgl WHERE {where})"
    return sql, params


class Aggregator:
    """Runs analytical queries against the SQLite cache."""

    def __init__(self, store: CacheStore) -> None:
        self._store = store
        self._conn = store.get_connection()

    def player_summary(self, spec: FilterSpec) -> pd.DataFrame:
        """
        Per-player post shot leaderboard.

        Columns: player_id, player_name, team, position, games,
                 post_shots, post_per_game, crossbar, left_post, right_post,
                 crossbar_pct, left_pct, right_pct,
                 ev, pp, pk, en, wrist, slap, snap, tip_in, backhand
        """
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""

        gp_subquery, gp_params = _gp_subquery(spec)
        shots_subquery, shots_params = _shots_subquery(spec)

        # Build HAVING clause: min_events and optional min_games_played
        having_parts = []
        having_params: list[Any] = []
        if spec.min_events > 1:
            having_parts.append(f"COUNT(*) >= {spec.min_events}")
        if spec.min_games_played > 0:
            having_parts.append(f"{gp_subquery} >= ?")
            having_params.extend(gp_params)
            having_params.append(spec.min_games_played)
        having = f"HAVING {' AND '.join(having_parts)}" if having_parts else ""

        # Parameter order matches subquery appearance order in the SQL:
        #   gp x2 (SELECT games_played, post_per_game)
        #   shots x1 (SELECT post_pct_of_shots)
        #   WHERE params
        #   HAVING params (gp x1 + threshold when min_games_played set)
        all_params = gp_params + gp_params + shots_params + params + having_params

        sql = f"""
        SELECT
            ps.shooting_player_id                          AS player_id,
            COALESCE(p.first_name || ' ' || p.last_name,
                     CAST(ps.shooting_player_id AS TEXT))  AS player_name,
            COALESCE(p.team_abbrev, '')                    AS team,
            COALESCE(p.position_code, '')                  AS position,
            COALESCE(p.position_group, '')                 AS pos_group,
            NULLIF({gp_subquery}, 0)                       AS games_played,
            COUNT(DISTINCT ps.game_id)                     AS games_with_post,
            COUNT(*)                                       AS post_shots,
            ROUND(
                CAST(COUNT(*) AS FLOAT) / NULLIF({gp_subquery}, 0),
                3
            )                                              AS post_per_game,
            ROUND(
                CAST(COUNT(*) AS FLOAT) / {shots_subquery} * 100,
                2
            )                                              AS post_pct_of_shots,
            SUM(CASE WHEN ps.reason='hit-crossbar'   THEN 1 ELSE 0 END)  AS crossbar,
            SUM(CASE WHEN ps.reason='hit-left-post'  THEN 1 ELSE 0 END)  AS left_post,
            SUM(CASE WHEN ps.reason='hit-right-post' THEN 1 ELSE 0 END)  AS right_post,
            SUM(CASE WHEN ps.strength_state='EV' THEN 1 ELSE 0 END)      AS ev,
            SUM(CASE WHEN ps.strength_state='PP' THEN 1 ELSE 0 END)      AS pp,
            SUM(CASE WHEN ps.strength_state='PK' THEN 1 ELSE 0 END)      AS pk,
            SUM(CASE WHEN ps.strength_state='EN' THEN 1 ELSE 0 END)      AS en,
            SUM(CASE WHEN ps.shot_type='wrist'   THEN 1 ELSE 0 END)      AS wrist,
            SUM(CASE WHEN ps.shot_type='slap'    THEN 1 ELSE 0 END)      AS slap,
            SUM(CASE WHEN ps.shot_type='snap'    THEN 1 ELSE 0 END)      AS snap,
            SUM(CASE WHEN ps.shot_type='tip-in'  THEN 1 ELSE 0 END)      AS tip_in,
            SUM(CASE WHEN ps.shot_type='backhand' THEN 1 ELSE 0 END)     AS backhand,
            SUM(CASE WHEN ps.is_home=1 THEN 1 ELSE 0 END)                AS home_shots,
            SUM(CASE WHEN ps.is_home=0 THEN 1 ELSE 0 END)                AS away_shots
        {_BASE_JOIN}
        {where_clause}
        GROUP BY ps.shooting_player_id
        {having}
        """
        df = _query(self._conn, sql, all_params)
        if not df.empty:
            df["crossbar_pct"] = (df["crossbar"] / df["post_shots"] * 100).round(1)
            df["left_pct"] = (df["left_post"] / df["post_shots"] * 100).round(1)
            df["right_pct"] = (df["right_post"] / df["post_shots"] * 100).round(1)
        return df

    def team_summary(self, spec: FilterSpec) -> pd.DataFrame:
        """Per-team post shot breakdown."""
        where, params = build_where_clause(spec)
        having = f"HAVING COUNT(*) >= {spec.min_events}" if spec.min_events > 1 else ""
        where_clause = f"WHERE {where}" if where else ""

        sql = f"""
        SELECT
            ps.event_owner_team_id                         AS team_id,
            COALESCE(
                CASE WHEN ps.is_home=1 THEN g.home_team_abbrev
                     ELSE g.away_team_abbrev END,
                CAST(ps.event_owner_team_id AS TEXT)
            )                                              AS team,
            COUNT(DISTINCT ps.game_id)                     AS games,
            COUNT(*)                                       AS post_shots,
            ROUND(CAST(COUNT(*) AS FLOAT) / COUNT(DISTINCT ps.game_id), 3)
                                                           AS post_per_game,
            SUM(CASE WHEN ps.reason='hit-crossbar'   THEN 1 ELSE 0 END)  AS crossbar,
            SUM(CASE WHEN ps.reason='hit-left-post'  THEN 1 ELSE 0 END)  AS left_post,
            SUM(CASE WHEN ps.reason='hit-right-post' THEN 1 ELSE 0 END)  AS right_post,
            SUM(CASE WHEN ps.strength_state='EV' THEN 1 ELSE 0 END)      AS ev,
            SUM(CASE WHEN ps.strength_state='PP' THEN 1 ELSE 0 END)      AS pp,
            SUM(CASE WHEN ps.strength_state='PK' THEN 1 ELSE 0 END)      AS pk,
            SUM(CASE WHEN ps.strength_state='EN' THEN 1 ELSE 0 END)      AS en
        {_BASE_JOIN}
        {where_clause}
        GROUP BY ps.event_owner_team_id
        {having}
        ORDER BY post_shots DESC
        """
        return _query(self._conn, sql, params)

    def by_shot_type(self, spec: FilterSpec) -> pd.DataFrame:
        """Post shot counts broken down by shot type."""
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
        SELECT
            COALESCE(NULLIF(ps.shot_type,''), 'unknown') AS shot_type,
            COUNT(*)                                      AS post_shots,
            SUM(CASE WHEN ps.reason='hit-crossbar'   THEN 1 ELSE 0 END) AS crossbar,
            SUM(CASE WHEN ps.reason='hit-left-post'  THEN 1 ELSE 0 END) AS left_post,
            SUM(CASE WHEN ps.reason='hit-right-post' THEN 1 ELSE 0 END) AS right_post
        {_BASE_JOIN}
        {where_clause}
        GROUP BY ps.shot_type
        ORDER BY post_shots DESC
        """
        return _query(self._conn, sql, params)

    def by_strength(self, spec: FilterSpec) -> pd.DataFrame:
        """Post shot counts by game situation."""
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
        SELECT
            ps.strength_state,
            ps.strength,
            COUNT(*) AS post_shots,
            SUM(CASE WHEN ps.reason='hit-crossbar'   THEN 1 ELSE 0 END) AS crossbar,
            SUM(CASE WHEN ps.reason='hit-left-post'  THEN 1 ELSE 0 END) AS left_post,
            SUM(CASE WHEN ps.reason='hit-right-post' THEN 1 ELSE 0 END) AS right_post
        {_BASE_JOIN}
        {where_clause}
        GROUP BY ps.strength_state, ps.strength
        ORDER BY post_shots DESC
        """
        return _query(self._conn, sql, params)

    def by_period(self, spec: FilterSpec) -> pd.DataFrame:
        """Post shot counts by period."""
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
        SELECT
            ps.period,
            ps.period_type,
            COUNT(*) AS post_shots,
            SUM(CASE WHEN ps.reason='hit-crossbar'   THEN 1 ELSE 0 END) AS crossbar,
            SUM(CASE WHEN ps.reason='hit-left-post'  THEN 1 ELSE 0 END) AS left_post,
            SUM(CASE WHEN ps.reason='hit-right-post' THEN 1 ELSE 0 END) AS right_post
        {_BASE_JOIN}
        {where_clause}
        GROUP BY ps.period, ps.period_type
        ORDER BY ps.period_type DESC, ps.period
        """
        return _query(self._conn, sql, params)

    def by_location(self, spec: FilterSpec) -> pd.DataFrame:
        """Post shots with coordinates for heatmap / zone analysis."""
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where} AND ps.x_coord IS NOT NULL" if where else "WHERE ps.x_coord IS NOT NULL"
        sql = f"""
        SELECT
            ps.x_coord,
            ps.y_coord,
            ps.reason,
            ps.shot_type,
            ps.zone_code,
            ps.strength_state,
            COALESCE(p.first_name || ' ' || p.last_name, '') AS player_name,
            COALESCE(CASE WHEN ps.is_home=1 THEN g.home_team_abbrev
                          ELSE g.away_team_abbrev END, '') AS team
        {_BASE_JOIN}
        {where_clause}
        """
        return _query(self._conn, sql, params)

    def home_away_splits(self, spec: FilterSpec) -> pd.DataFrame:
        """Home/away split for each player."""
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
        SELECT
            ps.shooting_player_id                           AS player_id,
            COALESCE(p.first_name || ' ' || p.last_name,
                     CAST(ps.shooting_player_id AS TEXT))   AS player_name,
            COALESCE(p.team_abbrev, '')                     AS team,
            SUM(CASE WHEN ps.is_home=1 THEN 1 ELSE 0 END)  AS home,
            SUM(CASE WHEN ps.is_home=0 THEN 1 ELSE 0 END)  AS away,
            COUNT(*)                                        AS total
        {_BASE_JOIN}
        {where_clause}
        GROUP BY ps.shooting_player_id
        HAVING COUNT(*) >= {spec.min_events}
        ORDER BY total DESC
        """
        df = _query(self._conn, sql, params)
        if not df.empty:
            df["home_pct"] = (df["home"] / df["total"] * 100).round(1)
        return df

    def season_trend(self, spec: FilterSpec, granularity: str = "month") -> pd.DataFrame:
        """
        Post shots over time.

        Args:
            granularity: "week" or "month".
        """
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""

        if granularity == "week":
            # SQLite: strftime('%Y-%W', date)
            date_trunc = "strftime('%Y-%W', ps.game_date)"
            date_label = "week"
        else:
            date_trunc = "strftime('%Y-%m', ps.game_date)"
            date_label = "month"

        sql = f"""
        SELECT
            {date_trunc} AS {date_label},
            COUNT(*)     AS post_shots,
            SUM(CASE WHEN ps.reason='hit-crossbar'   THEN 1 ELSE 0 END) AS crossbar,
            SUM(CASE WHEN ps.reason='hit-left-post'  THEN 1 ELSE 0 END) AS left_post,
            SUM(CASE WHEN ps.reason='hit-right-post' THEN 1 ELSE 0 END) AS right_post
        {_BASE_JOIN}
        {where_clause}
        GROUP BY {date_trunc}
        ORDER BY {date_trunc}
        """
        return _query(self._conn, sql, params)

    def player_detail(self, player_id: int, spec: FilterSpec) -> pd.DataFrame:
        """All post shot events for a single player (raw rows)."""
        # Override player filter
        spec.player_ids = [player_id]
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
        SELECT
            ps.game_date,
            g.home_team_abbrev || ' vs ' || g.away_team_abbrev AS matchup,
            ps.period,
            ps.period_type,
            ps.time_in_period,
            ps.reason,
            ps.shot_type,
            ps.strength_state,
            ps.strength,
            ps.x_coord,
            ps.y_coord,
            ps.zone_code
        {_BASE_JOIN}
        {where_clause}
        ORDER BY ps.game_date, ps.time_seconds
        """
        return _query(self._conn, sql, params)

    def summary_stats(self, spec: FilterSpec) -> dict[str, Any]:
        """High-level summary counts for a filter scope."""
        where, params = build_where_clause(spec)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
        SELECT
            COUNT(*)                                        AS total_post_shots,
            COUNT(DISTINCT ps.shooting_player_id)           AS unique_players,
            COUNT(DISTINCT ps.game_id)                      AS games_with_posts,
            COUNT(DISTINCT ps.event_owner_team_id)          AS unique_teams,
            SUM(CASE WHEN ps.reason='hit-crossbar'   THEN 1 ELSE 0 END) AS crossbar,
            SUM(CASE WHEN ps.reason='hit-left-post'  THEN 1 ELSE 0 END) AS left_post,
            SUM(CASE WHEN ps.reason='hit-right-post' THEN 1 ELSE 0 END) AS right_post,
            SUM(CASE WHEN ps.strength_state='EV' THEN 1 ELSE 0 END)     AS ev,
            SUM(CASE WHEN ps.strength_state='PP' THEN 1 ELSE 0 END)     AS pp,
            SUM(CASE WHEN ps.strength_state='PK' THEN 1 ELSE 0 END)     AS pk
        {_BASE_JOIN}
        {where_clause}
        """
        df = _query(self._conn, sql, params)
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
