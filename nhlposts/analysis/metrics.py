"""Pure metric calculation functions."""

from __future__ import annotations

import math


def per_60(events: int, toi_seconds: int) -> float:
    """Post shots per 60 minutes of TOI."""
    if toi_seconds <= 0:
        return 0.0
    return (events / toi_seconds) * 3600


def per_game(events: int, games: int) -> float:
    """Post shots per game played."""
    if games <= 0:
        return 0.0
    return events / games


def post_pct_of_shots(post_shots: int, total_shots: int) -> float:
    """Fraction of shot attempts (on goal + missed) that hit iron."""
    if total_shots <= 0:
        return 0.0
    return post_shots / total_shots * 100


def post_pct_of_missed(post_shots: int, total_missed: int) -> float:
    """Fraction of missed shots that hit iron (precision indicator)."""
    if total_missed <= 0:
        return 0.0
    return post_shots / total_missed * 100


def shot_distance(x_coord: float | None, y_coord: float | None) -> float | None:
    """
    Euclidean distance from the net.

    The NHL coordinate system has the goal at approximately x=±89, y=0.
    We compute distance to the nearest goal (x=89 or x=-89).
    """
    if x_coord is None or y_coord is None:
        return None
    dist_right = math.sqrt((x_coord - 89) ** 2 + y_coord ** 2)
    dist_left = math.sqrt((x_coord + 89) ** 2 + y_coord ** 2)
    return min(dist_right, dist_left)


def pct(numerator: int | float, denominator: int | float) -> float:
    """Generic percentage, returns 0.0 if denominator is zero."""
    if denominator <= 0:
        return 0.0
    return numerator / denominator * 100
