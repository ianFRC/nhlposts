"""Player dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field


_POSITION_GROUP = {
    "C": "F", "L": "F", "R": "F", "W": "F",
    "LW": "F", "RW": "F",
    "D": "D",
    "G": "G",
}


@dataclass(slots=True)
class Player:
    player_id: int
    first_name: str
    last_name: str
    position_code: str   # "C" | "L" | "R" | "D" | "G" etc.
    position_group: str  # "F" | "D" | "G"
    team_abbrev: str
    team_id: int
    shoots: str          # "L" | "R"

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"

    @classmethod
    def from_roster_entry(
        cls,
        player_id: int,
        first_name: str,
        last_name: str,
        position_code: str,
        team_abbrev: str,
        team_id: int,
        shoots: str = "",
    ) -> "Player":
        group = _POSITION_GROUP.get(position_code.upper(), "F")
        return cls(
            player_id=player_id,
            first_name=first_name,
            last_name=last_name,
            position_code=position_code,
            position_group=group,
            team_abbrev=team_abbrev,
            team_id=team_id,
            shoots=shoots,
        )
