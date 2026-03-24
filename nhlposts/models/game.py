"""Game and related dataclasses."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Game:
    game_id: int
    season: str           # "20242025"
    game_type: int        # 2=regular, 3=playoff
    game_date: str        # "2024-11-15"
    home_team_id: int
    home_team_abbrev: str
    away_team_id: int
    away_team_abbrev: str
    game_state: str       # "OFF" | "FINAL" | "FUT" | "LIVE"
