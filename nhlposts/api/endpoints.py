"""Typed wrappers around NHL API endpoints."""

from __future__ import annotations

from typing import Any

from .client import NHLClient


class NHLEndpoints:
    """
    Typed endpoint helpers. Each method calls client.get() and returns
    the raw response dict. No parsing logic lives here.
    """

    def __init__(self, client: NHLClient) -> None:
        self._c = client

    def get_play_by_play(self, game_id: int) -> dict[str, Any]:
        """Full play-by-play for a game."""
        return self._c.get(f"/gamecenter/{game_id}/play-by-play")

    def get_schedule_for_date(self, date: str) -> dict[str, Any]:
        """Weekly schedule starting from `date` (YYYY-MM-DD)."""
        return self._c.get(f"/schedule/{date}")

    def get_team_season_schedule(self, team_abbrev: str, season: str) -> dict[str, Any]:
        """All games for a team in a season (season = '20242025')."""
        return self._c.get(f"/club-schedule-season/{team_abbrev}/{season}")

    def get_roster(self, team_abbrev: str, season: str) -> dict[str, Any]:
        """Team roster for a season."""
        return self._c.get(f"/roster/{team_abbrev}/{season}")

    def get_player_landing(self, player_id: int) -> dict[str, Any]:
        """Player profile with career stats."""
        return self._c.get(f"/player/{player_id}/landing")

    def get_player_game_log(
        self, player_id: int, season: str, game_type: int = 2
    ) -> dict[str, Any]:
        """Player game log for a season. game_type: 2=regular, 3=playoffs."""
        return self._c.get(f"/player/{player_id}/game-log/{season}/{game_type}")

    def get_standings(self, date: str) -> dict[str, Any]:
        """Standings as of a specific date (YYYY-MM-DD)."""
        return self._c.get(f"/standings/{date}")

    def get_standings_now(self) -> dict[str, Any]:
        """Current standings."""
        return self._c.get("/standings/now")

    def get_team_stats(self, team_abbrev: str) -> dict[str, Any]:
        """Current team statistics."""
        return self._c.get(f"/club-stats/{team_abbrev}/now")

    def get_schedule_now(self) -> dict[str, Any]:
        """This week's schedule."""
        return self._c.get("/schedule/now")
