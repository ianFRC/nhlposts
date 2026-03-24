"""SQLite schema creation and connection management."""

from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS games (
    game_id          INTEGER PRIMARY KEY,
    season           TEXT    NOT NULL,
    game_type        INTEGER NOT NULL,
    game_date        TEXT    NOT NULL,
    home_team_id     INTEGER NOT NULL,
    home_team_abbrev TEXT    NOT NULL,
    away_team_id     INTEGER NOT NULL,
    away_team_abbrev TEXT    NOT NULL,
    game_state       TEXT    NOT NULL,
    ingested         INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_games_season   ON games(season);
CREATE INDEX IF NOT EXISTS idx_games_date     ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_ingested ON games(ingested);

CREATE TABLE IF NOT EXISTS players (
    player_id      INTEGER PRIMARY KEY,
    first_name     TEXT    NOT NULL,
    last_name      TEXT    NOT NULL,
    position_code  TEXT    NOT NULL,
    position_group TEXT    NOT NULL,
    team_abbrev    TEXT    NOT NULL DEFAULT '',
    team_id        INTEGER NOT NULL DEFAULT 0,
    shoots         TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS post_shots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            INTEGER NOT NULL,
    game_id             INTEGER NOT NULL,
    season              TEXT    NOT NULL,
    game_date           TEXT    NOT NULL,

    period              INTEGER NOT NULL,
    period_type         TEXT    NOT NULL,
    time_in_period      TEXT    NOT NULL,
    time_seconds        INTEGER NOT NULL,

    reason              TEXT    NOT NULL,
    shot_type           TEXT    NOT NULL DEFAULT '',
    x_coord             REAL,
    y_coord             REAL,
    zone_code           TEXT    NOT NULL DEFAULT '',

    away_skaters        INTEGER NOT NULL DEFAULT 5,
    home_skaters        INTEGER NOT NULL DEFAULT 5,
    away_goalie_in_net  INTEGER NOT NULL DEFAULT 1,
    home_goalie_in_net  INTEGER NOT NULL DEFAULT 1,
    strength            TEXT    NOT NULL DEFAULT '5v5',
    strength_state      TEXT    NOT NULL DEFAULT 'EV',

    shooting_player_id  INTEGER NOT NULL,
    goalie_in_net_id    INTEGER,
    event_owner_team_id INTEGER NOT NULL,
    home_team_id        INTEGER NOT NULL,
    away_team_id        INTEGER NOT NULL,
    is_home             INTEGER NOT NULL DEFAULT 0,

    UNIQUE(event_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_ps_player   ON post_shots(shooting_player_id);
CREATE INDEX IF NOT EXISTS idx_ps_season   ON post_shots(season);
CREATE INDEX IF NOT EXISTS idx_ps_date     ON post_shots(game_date);
CREATE INDEX IF NOT EXISTS idx_ps_team     ON post_shots(event_owner_team_id);
CREATE INDEX IF NOT EXISTS idx_ps_strength ON post_shots(strength_state);
CREATE INDEX IF NOT EXISTS idx_ps_reason   ON post_shots(reason);

CREATE TABLE IF NOT EXISTS player_game_log (
    player_id  INTEGER NOT NULL,
    game_id    INTEGER NOT NULL,
    game_date  TEXT    NOT NULL,
    season     TEXT    NOT NULL,
    game_type  INTEGER NOT NULL,
    shots      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (player_id, game_id)
);

CREATE INDEX IF NOT EXISTS idx_pgl_player_season ON player_game_log(player_id, season);
CREATE INDEX IF NOT EXISTS idx_pgl_date          ON player_game_log(game_date);

CREATE TABLE IF NOT EXISTS raw_cache (
    key        TEXT PRIMARY KEY,
    payload    TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cache_metadata (
    key        TEXT PRIMARY KEY,
    fetched_at TEXT NOT NULL,
    ttl_hours  INTEGER NOT NULL
);
"""


def get_connection(db_path: Path) -> sqlite3.Connection:
    """
    Open (and initialize if needed) the SQLite database.

    Returns a connection with row_factory=sqlite3.Row set.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    # Add shots column to existing databases that predate this column
    cols = {r[1] for r in conn.execute("PRAGMA table_info(player_game_log)")}
    if "shots" not in cols:
        conn.execute("ALTER TABLE player_game_log ADD COLUMN shots INTEGER NOT NULL DEFAULT 0")
    conn.commit()
    return conn
