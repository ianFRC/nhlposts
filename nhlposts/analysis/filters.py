"""FilterSpec dataclass and SQL WHERE clause builder."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FilterSpec:
    """All possible filter criteria for post shot queries."""

    # Time scope
    seasons: list[str] = field(default_factory=list)      # ["20232024", "20242025"]
    date_from: str | None = None                           # "2024-01-01"
    date_to: str | None = None                             # "2024-03-15"

    # Player / team scope
    player_ids: list[int] = field(default_factory=list)
    team_ids: list[int] = field(default_factory=list)
    team_abbrevs: list[str] = field(default_factory=list)
    position_groups: list[str] = field(default_factory=list)  # ["F", "D"]

    # Shot detail filters
    reasons: list[str] = field(default_factory=list)          # ["hit-crossbar"]
    shot_types: list[str] = field(default_factory=list)        # ["wrist", "slap"]
    strength_states: list[str] = field(default_factory=list)   # ["EV", "PP"]
    periods: list[int] = field(default_factory=list)           # [1, 2, 3]
    period_types: list[str] = field(default_factory=list)      # ["REG", "OT"]
    zone_codes: list[str] = field(default_factory=list)        # ["O"]

    # Context
    home_away: str | None = None      # "home" | "away"
    shoots: str | None = None         # "L" | "R"
    season_type: int | None = None    # 2=regular, 3=playoff
    opponent_abbrevs: list[str] = field(default_factory=list)

    # Output thresholds
    min_events: int = 1
    min_games_played: int = 0


def build_where_clause(spec: FilterSpec) -> tuple[str, list[Any]]:
    """
    Build a SQL WHERE clause fragment and parameter list from a FilterSpec.

    The clause joins post_shots (ps) with players (p) and games (g).
    Caller must set up the appropriate JOIN:
        FROM post_shots ps
        LEFT JOIN players p ON p.player_id = ps.shooting_player_id
        LEFT JOIN games g ON g.game_id = ps.game_id

    Returns:
        (where_sql, params) — where_sql may be empty string if no filters.
    """
    clauses: list[str] = []
    params: list[Any] = []

    def _in(col: str, values: list[Any]) -> None:
        if values:
            placeholders = ",".join("?" * len(values))
            clauses.append(f"{col} IN ({placeholders})")
            params.extend(values)

    # Season / date
    if spec.seasons:
        _in("ps.season", spec.seasons)
    if spec.date_from:
        clauses.append("ps.game_date >= ?")
        params.append(spec.date_from)
    if spec.date_to:
        clauses.append("ps.game_date <= ?")
        params.append(spec.date_to)

    # Player / team
    if spec.player_ids:
        _in("ps.shooting_player_id", spec.player_ids)
    if spec.team_ids:
        _in("ps.event_owner_team_id", spec.team_ids)
    if spec.team_abbrevs:
        _in("p.team_abbrev", spec.team_abbrevs)
    if spec.position_groups:
        _in("p.position_group", spec.position_groups)
    if spec.shoots:
        clauses.append("p.shoots = ?")
        params.append(spec.shoots)

    # Shot details
    if spec.reasons:
        _in("ps.reason", spec.reasons)
    if spec.shot_types:
        _in("ps.shot_type", spec.shot_types)
    if spec.strength_states:
        _in("ps.strength_state", spec.strength_states)
    if spec.periods:
        _in("ps.period", spec.periods)
    if spec.period_types:
        _in("ps.period_type", spec.period_types)
    if spec.zone_codes:
        _in("ps.zone_code", spec.zone_codes)

    # Context
    if spec.home_away == "home":
        clauses.append("ps.is_home = 1")
    elif spec.home_away == "away":
        clauses.append("ps.is_home = 0")

    if spec.season_type is not None:
        clauses.append("g.game_type = ?")
        params.append(spec.season_type)

    if spec.opponent_abbrevs:
        # Opponent is the other team (not the event owner)
        opp_placeholders = ",".join("?" * len(spec.opponent_abbrevs))
        clauses.append(
            f"""(
                (ps.is_home = 1 AND (
                    SELECT abbrev FROM (
                        SELECT g2.away_team_abbrev AS abbrev FROM games g2
                        WHERE g2.game_id = ps.game_id
                    )
                ) IN ({opp_placeholders}))
                OR
                (ps.is_home = 0 AND (
                    SELECT abbrev FROM (
                        SELECT g2.home_team_abbrev AS abbrev FROM games g2
                        WHERE g2.game_id = ps.game_id
                    )
                ) IN ({opp_placeholders}))
            )"""
        )
        params.extend(spec.opponent_abbrevs)
        params.extend(spec.opponent_abbrevs)

    where_sql = " AND ".join(clauses) if clauses else ""
    return where_sql, params
