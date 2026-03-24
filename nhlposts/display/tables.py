"""Rich table renderers for all analysis views."""

from __future__ import annotations

from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import box
from rich.text import Text

import sys as _sys
import io as _io
if _sys.platform == "win32" and hasattr(_sys.stdout, "buffer"):
    _utf8_stdout = _io.TextIOWrapper(_sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    console = Console(file=_utf8_stdout, highlight=False)
else:
    console = Console(highlight=False)


def _pct_str(val: float | None) -> str:
    if val is None or pd.isna(val):
        return "-"
    return f"{val:.1f}%"


def _float_str(val: float | None, decimals: int = 3) -> str:
    if val is None or pd.isna(val):
        return "-"
    return f"{val:.{decimals}f}"


def _int_str(val: Any) -> str:
    if val is None or pd.isna(val):
        return "-"
    return str(int(val))


def render_player_leaderboard(
    df: pd.DataFrame,
    title: str = "Post/Crossbar Shot Leaders",
    sort_by: str = "post_shots",
    limit: int = 25,
) -> None:
    if df.empty:
        console.print("[yellow]No data found for these filters.[/yellow]")
        return

    if sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=False)
    df = df.head(limit)

    table = Table(
        title=title,
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        title_style="bold white",
        row_styles=["", "dim"],
    )

    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Player", min_width=20)
    table.add_column("Team", width=5, justify="center")
    table.add_column("Pos", width=4, justify="center")
    table.add_column("GP", width=4, justify="right")
    table.add_column("Posts", style="cyan bold", width=6, justify="right")
    table.add_column("Post/GP", style="yellow", width=8, justify="right")
    table.add_column("Post%", style="magenta", width=7, justify="right")
    table.add_column("Crossbar", width=9, justify="right")
    table.add_column("L-Post", width=7, justify="right")
    table.add_column("R-Post", width=7, justify="right")
    table.add_column("CB%", style="green", width=6, justify="right")
    table.add_column("EV", width=5, justify="right")
    table.add_column("PP", width=5, justify="right")
    table.add_column("PK", width=5, justify="right")

    for i, row in enumerate(df.itertuples(index=False), 1):
        posts = int(getattr(row, "post_shots", 0))
        table.add_row(
            str(i),
            str(getattr(row, "player_name", "")),
            str(getattr(row, "team", "")),
            str(getattr(row, "position", "")),
            _int_str(getattr(row, "games_played", None)),
            str(posts),
            _float_str(getattr(row, "post_per_game", None), 3),
            _pct_str(getattr(row, "post_pct_of_shots", None)),
            _int_str(getattr(row, "crossbar", None)),
            _int_str(getattr(row, "left_post", None)),
            _int_str(getattr(row, "right_post", None)),
            _pct_str(getattr(row, "crossbar_pct", None)),
            _int_str(getattr(row, "ev", None)),
            _int_str(getattr(row, "pp", None)),
            _int_str(getattr(row, "pk", None)),
        )

    console.print(table)
    console.print(
        f"[dim]Showing {min(limit, len(df))} of {len(df)} players | "
        f"Sorted by: {sort_by} (desc)[/dim]"
    )


def render_team_summary(
    df: pd.DataFrame,
    title: str = "Post/Crossbar Shots by Team",
    limit: int = 32,
) -> None:
    if df.empty:
        console.print("[yellow]No data found for these filters.[/yellow]")
        return

    df = df.head(limit)

    table = Table(
        title=title,
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold white",
        row_styles=["", "dim"],
    )

    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Team", width=6, justify="center")
    table.add_column("GP", width=4, justify="right")
    table.add_column("Posts", style="cyan bold", width=6, justify="right")
    table.add_column("Post/GP", style="yellow", width=8, justify="right")
    table.add_column("Crossbar", width=9, justify="right")
    table.add_column("L-Post", width=7, justify="right")
    table.add_column("R-Post", width=7, justify="right")
    table.add_column("EV", width=5, justify="right")
    table.add_column("PP", width=5, justify="right")
    table.add_column("PK", width=5, justify="right")

    for i, row in enumerate(df.itertuples(index=False), 1):
        table.add_row(
            str(i),
            str(getattr(row, "team", "")),
            _int_str(getattr(row, "games", None)),
            str(int(getattr(row, "post_shots", 0))),
            _float_str(getattr(row, "post_per_game", None), 3),
            _int_str(getattr(row, "crossbar", None)),
            _int_str(getattr(row, "left_post", None)),
            _int_str(getattr(row, "right_post", None)),
            _int_str(getattr(row, "ev", None)),
            _int_str(getattr(row, "pp", None)),
            _int_str(getattr(row, "pk", None)),
        )

    console.print(table)


def render_shot_types(df: pd.DataFrame, title: str = "Post Shots by Shot Type") -> None:
    if df.empty:
        console.print("[yellow]No data found.[/yellow]")
        return

    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan")
    table.add_column("Shot Type", min_width=12)
    table.add_column("Posts", style="cyan bold", justify="right")
    table.add_column("Crossbar", justify="right")
    table.add_column("L-Post", justify="right")
    table.add_column("R-Post", justify="right")

    for row in df.itertuples(index=False):
        table.add_row(
            str(getattr(row, "shot_type", "")),
            str(int(getattr(row, "post_shots", 0))),
            _int_str(getattr(row, "crossbar", None)),
            _int_str(getattr(row, "left_post", None)),
            _int_str(getattr(row, "right_post", None)),
        )
    console.print(table)


def render_by_strength(df: pd.DataFrame, title: str = "Post Shots by Game Situation") -> None:
    if df.empty:
        console.print("[yellow]No data found.[/yellow]")
        return

    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan")
    table.add_column("State", width=6, justify="center")
    table.add_column("Strength", width=8, justify="center")
    table.add_column("Posts", style="cyan bold", justify="right")
    table.add_column("Crossbar", justify="right")
    table.add_column("L-Post", justify="right")
    table.add_column("R-Post", justify="right")

    for row in df.itertuples(index=False):
        table.add_row(
            str(getattr(row, "strength_state", "")),
            str(getattr(row, "strength", "")),
            str(int(getattr(row, "post_shots", 0))),
            _int_str(getattr(row, "crossbar", None)),
            _int_str(getattr(row, "left_post", None)),
            _int_str(getattr(row, "right_post", None)),
        )
    console.print(table)


def render_by_period(df: pd.DataFrame, title: str = "Post Shots by Period") -> None:
    if df.empty:
        console.print("[yellow]No data found.[/yellow]")
        return

    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan")
    table.add_column("Period", width=7, justify="center")
    table.add_column("Type", width=5, justify="center")
    table.add_column("Posts", style="cyan bold", justify="right")
    table.add_column("Crossbar", justify="right")
    table.add_column("L-Post", justify="right")
    table.add_column("R-Post", justify="right")

    for row in df.itertuples(index=False):
        table.add_row(
            _int_str(getattr(row, "period", None)),
            str(getattr(row, "period_type", "")),
            str(int(getattr(row, "post_shots", 0))),
            _int_str(getattr(row, "crossbar", None)),
            _int_str(getattr(row, "left_post", None)),
            _int_str(getattr(row, "right_post", None)),
        )
    console.print(table)


def render_home_away(
    df: pd.DataFrame,
    title: str = "Home/Away Post Shot Splits",
    limit: int = 25,
) -> None:
    if df.empty:
        console.print("[yellow]No data found.[/yellow]")
        return

    df = df.head(limit)
    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan")
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Player", min_width=20)
    table.add_column("Team", width=5, justify="center")
    table.add_column("Total", style="cyan bold", justify="right")
    table.add_column("Home", justify="right")
    table.add_column("Away", justify="right")
    table.add_column("Home%", style="green", justify="right")

    for i, row in enumerate(df.itertuples(index=False), 1):
        table.add_row(
            str(i),
            str(getattr(row, "player_name", "")),
            str(getattr(row, "team", "")),
            str(int(getattr(row, "total", 0))),
            _int_str(getattr(row, "home", None)),
            _int_str(getattr(row, "away", None)),
            _pct_str(getattr(row, "home_pct", None)),
        )
    console.print(table)


def render_trend(
    df: pd.DataFrame,
    granularity: str = "month",
    title: str | None = None,
) -> None:
    if df.empty:
        console.print("[yellow]No trend data found.[/yellow]")
        return

    label = granularity.capitalize()
    title = title or f"Post Shots by {label}"
    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan")
    table.add_column(label, min_width=10)
    table.add_column("Posts", style="cyan bold", justify="right")
    table.add_column("Crossbar", justify="right")
    table.add_column("L-Post", justify="right")
    table.add_column("R-Post", justify="right")

    col = granularity if granularity in df.columns else df.columns[0]
    for row in df.itertuples(index=False):
        table.add_row(
            str(getattr(row, col, "")),
            str(int(getattr(row, "post_shots", 0))),
            _int_str(getattr(row, "crossbar", None)),
            _int_str(getattr(row, "left_post", None)),
            _int_str(getattr(row, "right_post", None)),
        )
    console.print(table)


def render_player_detail(
    df: pd.DataFrame,
    player_name: str,
) -> None:
    if df.empty:
        console.print(f"[yellow]No post shots found for {player_name}.[/yellow]")
        return

    title = f"Post Shots — {player_name}"
    table = Table(title=title, box=box.ROUNDED, header_style="bold cyan")
    table.add_column("Date", width=12)
    table.add_column("Matchup", min_width=16)
    table.add_column("Per", width=4, justify="center")
    table.add_column("Time", width=7, justify="center")
    table.add_column("Reason", min_width=16)
    table.add_column("Type", width=10)
    table.add_column("Sit", width=4, justify="center")
    table.add_column("Zone", width=5, justify="center")

    for row in df.itertuples(index=False):
        reason = str(getattr(row, "reason", ""))
        reason_color = {
            "hit-crossbar": "yellow",
            "hit-left-post": "cyan",
            "hit-right-post": "green",
        }.get(reason, "white")

        table.add_row(
            str(getattr(row, "game_date", "")),
            str(getattr(row, "matchup", "")),
            _int_str(getattr(row, "period", None)),
            str(getattr(row, "time_in_period", "")),
            Text(reason, style=reason_color),
            str(getattr(row, "shot_type", "")),
            str(getattr(row, "strength_state", "")),
            str(getattr(row, "zone_code", "")),
        )
    console.print(table)
    console.print(f"[dim]Total: {len(df)} post shots[/dim]")


def render_summary_stats(stats: dict[str, Any], label: str = "") -> None:
    """Print a compact summary box."""
    if not stats:
        console.print("[yellow]No data.[/yellow]")
        return

    parts = [
        f"[cyan bold]{int(stats.get('total_post_shots', 0))}[/] total post shots",
        f"[white]{int(stats.get('unique_players', 0))}[/] players",
        f"[white]{int(stats.get('games_with_posts', 0))}[/] games",
        f"CB: [yellow]{int(stats.get('crossbar', 0))}[/]",
        f"L: [cyan]{int(stats.get('left_post', 0))}[/]",
        f"R: [green]{int(stats.get('right_post', 0))}[/]",
        f"EV: {int(stats.get('ev', 0))}  PP: {int(stats.get('pp', 0))}  PK: {int(stats.get('pk', 0))}",
    ]
    prefix = f"[bold]{label}[/] | " if label else ""
    console.print(prefix + "  |  ".join(parts))
