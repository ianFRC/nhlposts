"""PostShotEvent dataclass — the central analytical unit."""

from __future__ import annotations

from dataclasses import dataclass


POST_REASONS = frozenset({"hit-crossbar", "hit-left-post", "hit-right-post"})


@dataclass(slots=True)
class PostShotEvent:
    # Identity
    event_id: int
    game_id: int
    season: str           # "20242025"
    game_date: str        # "2024-11-15"

    # Timing
    period: int
    period_type: str      # "REG" | "OT"
    time_in_period: str   # "14:32"
    time_seconds: int     # total game seconds elapsed

    # Shot
    reason: str           # "hit-crossbar" | "hit-left-post" | "hit-right-post"
    shot_type: str        # "wrist" | "snap" | "slap" | "tip-in" | "backhand" | "poke" | ""
    x_coord: float | None
    y_coord: float | None
    zone_code: str        # "O" | "D" | "N" | ""

    # Situation
    away_skaters: int
    home_skaters: int
    away_goalie_in_net: bool
    home_goalie_in_net: bool
    strength: str         # "5v5", "5v4", "4v5", etc.
    strength_state: str   # "EV" | "PP" | "PK" | "EN"

    # Players & teams
    shooting_player_id: int
    goalie_in_net_id: int | None
    event_owner_team_id: int
    home_team_id: int
    away_team_id: int
    is_home: bool
