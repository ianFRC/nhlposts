"""
nhlposts CLI — NHL post/crossbar shot analyzer.

Usage:
    nhlposts fetch season --season 20242025
    nhlposts fetch dates --from 2024-10-01 --to 2025-01-31
    nhlposts fetch game 2024020500
    nhlposts analyze players --season 20242025 --strength EV
    nhlposts analyze teams --season 20242025
    nhlposts analyze types --season 20242025
    nhlposts analyze situations --season 20242025
    nhlposts analyze periods --season 20242025
    nhlposts analyze splits --season 20242025
    nhlposts analyze trend --season 20242025
    nhlposts player "Connor McDavid" --season 20242025
    nhlposts cache status
    nhlposts cache clear --season 20242025
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn, TextColumn

from nhlposts.api.client import NHLClient
from nhlposts.cache.store import CacheStore
from nhlposts.analysis.aggregator import Aggregator
from nhlposts.analysis.filters import FilterSpec
from nhlposts.display import tables, export as exp
from nhlposts.ingestion.games import GameIngester
from nhlposts.ingestion.players import PlayerResolver
from nhlposts.ingestion.season import SeasonFetcher

import sys as _sys
import io as _io
if _sys.platform == "win32" and hasattr(_sys.stdout, "buffer"):
    _utf8_stdout = _io.TextIOWrapper(_sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
    console = Console(file=_utf8_stdout)
else:
    console = Console()

DEFAULT_DB = Path.home() / ".nhlposts" / "cache.db"


# ------------------------------------------------------------------ #
# Shared filter options (applied to all analyze + player commands)    #
# ------------------------------------------------------------------ #

def _filter_options(f: Any) -> Any:
    """Decorator that attaches all filter options to a command."""
    decorators = [
        click.option("--season", "seasons", multiple=True, metavar="SEASON",
                     help="Season code (repeatable), e.g. 20242025"),
        click.option("--from", "date_from", default=None, metavar="DATE",
                     help="Start date YYYY-MM-DD"),
        click.option("--to", "date_to", default=None, metavar="DATE",
                     help="End date YYYY-MM-DD"),
        click.option("--team", "teams", multiple=True, metavar="ABBREV",
                     help="Team abbreviation (repeatable), e.g. TOR"),
        click.option("--player", "players", multiple=True, metavar="NAME_OR_ID",
                     help="Player name or ID (repeatable)"),
        click.option("--position", "positions", multiple=True,
                     type=click.Choice(["F", "D", "G"], case_sensitive=False),
                     help="Position group (repeatable)"),
        click.option("--strength", "strengths", multiple=True,
                     type=click.Choice(["EV", "PP", "PK", "EN"], case_sensitive=False),
                     help="Game situation (repeatable)"),
        click.option("--shot-type", "shot_types", multiple=True, metavar="TYPE",
                     help="Shot type: wrist, slap, snap, tip-in, backhand (repeatable)"),
        click.option("--reason", "reasons", multiple=True,
                     type=click.Choice(
                         ["hit-crossbar", "hit-left-post", "hit-right-post"],
                         case_sensitive=False,
                     ),
                     help="Post hit reason (repeatable)"),
        click.option("--period", "periods", multiple=True, type=int,
                     help="Period number (repeatable)"),
        click.option("--home-away", default=None,
                     type=click.Choice(["home", "away"], case_sensitive=False),
                     help="Home or away games only"),
        click.option("--shoots", default=None,
                     type=click.Choice(["L", "R"], case_sensitive=False),
                     help="Player handedness"),
        click.option("--season-type", default=None,
                     type=click.Choice(["regular", "playoff"], case_sensitive=False),
                     help="regular or playoff games only"),
        click.option("--opponent", "opponents", multiple=True, metavar="ABBREV",
                     help="Filter by opponent team abbreviation (repeatable)"),
        click.option("--min-events", default=1, show_default=True, type=int,
                     help="Minimum post shot events to include in results"),
        click.option("--min-gp", "min_games_played", default=0, show_default=True, type=int,
                     help="Minimum games played to include a player (requires fetch gp)"),
        click.option("--sort", "sort_by", default="post_shots", show_default=True,
                     help="Column to sort results by"),
        click.option("--limit", default=25, show_default=True, type=int,
                     help="Maximum rows to display"),
        click.option("--format", "fmt", default="table",
                     type=click.Choice(["table", "csv", "json"], case_sensitive=False),
                     help="Output format"),
        click.option("--output", default=None, metavar="FILE",
                     help="Write output to file instead of stdout"),
    ]
    for dec in reversed(decorators):
        f = dec(f)
    return f


def _build_filter(
    ctx: click.Context,
    seasons: tuple[str, ...],
    date_from: str | None,
    date_to: str | None,
    teams: tuple[str, ...],
    players: tuple[str, ...],
    positions: tuple[str, ...],
    strengths: tuple[str, ...],
    shot_types: tuple[str, ...],
    reasons: tuple[str, ...],
    periods: tuple[int, ...],
    home_away: str | None,
    shoots: str | None,
    season_type: str | None,
    opponents: tuple[str, ...],
    min_events: int,
    min_games_played: int,
    store: CacheStore,
) -> FilterSpec:
    """Build a FilterSpec from CLI arguments, resolving player names to IDs."""
    player_ids: list[int] = []
    if players:
        resolver = PlayerResolver(store, ctx.obj["client"])
        for name_or_id in players:
            if name_or_id.isdigit():
                player_ids.append(int(name_or_id))
            else:
                matches = resolver.resolve_name(name_or_id)
                if not matches:
                    console.print(f"[yellow]Warning: no player found matching '{name_or_id}'[/yellow]")
                elif len(matches) > 1:
                    console.print(
                        f"[yellow]Multiple matches for '{name_or_id}': "
                        f"{', '.join(m.full_name for m in matches[:3])}. "
                        f"Using best match: {matches[0].full_name}[/yellow]"
                    )
                    player_ids.append(matches[0].player_id)
                else:
                    player_ids.append(matches[0].player_id)

    return FilterSpec(
        seasons=list(seasons),
        date_from=date_from,
        date_to=date_to,
        team_abbrevs=list(teams),
        player_ids=player_ids,
        position_groups=[p.upper() for p in positions],
        strength_states=[s.upper() for s in strengths],
        shot_types=list(shot_types),
        reasons=list(reasons),
        periods=list(periods),
        home_away=home_away,
        shoots=shoots.upper() if shoots else None,
        season_type={"regular": 2, "playoff": 3}.get(season_type) if season_type else None,
        opponent_abbrevs=list(opponents),
        min_events=min_events,
        min_games_played=min_games_played,
    )


def _render_or_export(
    df: Any,
    fmt: str,
    output: str | None,
    render_fn: Any,
    **render_kwargs: Any,
) -> None:
    if fmt == "table":
        render_fn(df, **render_kwargs)
    else:
        exp.export_dataframe(df, fmt, output)


# ------------------------------------------------------------------ #
# Main group                                                           #
# ------------------------------------------------------------------ #

@click.group()
@click.option("--db", "db_path", default=str(DEFAULT_DB), show_default=True,
              envvar="NHLPOSTS_DB", help="Path to SQLite cache database")
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def main(ctx: click.Context, db_path: str, verbose: bool) -> None:
    """NHL post/crossbar shot analyzer.

    Fetch play-by-play data from the NHL API and analyze shots that hit
    the post or crossbar. Data is cached locally in SQLite.
    """
    ctx.ensure_object(dict)

    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db = Path(db_path)
    store = CacheStore(db)
    client = NHLClient(rate_limit=0.5)

    ctx.obj["store"] = store
    ctx.obj["client"] = client
    ctx.call_on_close(store.close)
    ctx.call_on_close(client.close)


# ------------------------------------------------------------------ #
# fetch group                                                          #
# ------------------------------------------------------------------ #

@main.group()
def fetch() -> None:
    """Fetch and cache NHL play-by-play data."""


@fetch.command("season")
@click.option("--season", "seasons", multiple=True, required=True,
              help="Season code (repeatable), e.g. 20242025")
@click.option("--playoffs", is_flag=True, default=False,
              help="Fetch playoffs instead of regular season")
@click.option("--workers", default=4, show_default=True, type=int,
              help="Concurrent HTTP workers")
@click.option("--rosters/--no-rosters", default=True,
              help="Also fetch team rosters for player lookup")
@click.pass_context
def fetch_season(
    ctx: click.Context,
    seasons: tuple[str, ...],
    playoffs: bool,
    workers: int,
    rosters: bool,
) -> None:
    """Fetch all games for one or more seasons."""
    store: CacheStore = ctx.obj["store"]
    client: NHLClient = ctx.obj["client"]
    game_type = 3 if playoffs else 2

    fetcher = SeasonFetcher(store, client)
    ingester = GameIngester(store, client, workers=workers)

    for season in seasons:
        console.print(f"[bold]Fetching season {season}...[/bold]")

        if rosters:
            resolver = PlayerResolver(store, client)
            with console.status(f"Fetching rosters for {season}..."):
                n = resolver.fetch_all_rosters(season)
                if n:
                    console.print(f"  Loaded {n} players")

        with console.status(f"Discovering games for {season}..."):
            games = fetcher.fetch_season(season, game_type=game_type)

        completed_games = [g for g in games if g.game_state in ("OFF", "FINAL")]
        pending = store.get_pending_games(season=season)
        pending_ids = [r["game_id"] for r in pending]

        console.print(
            f"  {len(games)} games found | "
            f"{len(completed_games)} completed | "
            f"{len(pending_ids)} need ingestion"
        )

        if not pending_ids:
            console.print(f"  [green]Season {season} fully ingested.[/green]")
            continue

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            TextColumn("[cyan]{task.fields[shots]} posts"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Ingesting {season}", total=len(pending_ids), shots=0
            )
            total_shots = 0

            def _cb(gid: int, shots: int) -> None:
                nonlocal total_shots
                total_shots += shots
                progress.advance(task)
                progress.update(task, shots=total_shots)

            result = ingester.ingest_batch(pending_ids, progress_callback=_cb)

        console.print(
            f"  [green]Done:[/green] {result.games_processed} games, "
            f"[cyan]{result.post_shots_found}[/cyan] post shots, "
            f"{result.games_failed} failed"
        )

        # Resolve any players not yet in the players table
        _resolve_unknown_players(store, client, season)

        # Auto-fetch player game logs for accurate GP
        _fetch_player_gp(store, client, season=season, game_type=game_type)


@fetch.command("dates")
@click.option("--from", "date_from", required=True, metavar="DATE",
              help="Start date YYYY-MM-DD")
@click.option("--to", "date_to", required=True, metavar="DATE",
              help="End date YYYY-MM-DD")
@click.option("--workers", default=4, show_default=True, type=int)
@click.pass_context
def fetch_dates(
    ctx: click.Context,
    date_from: str,
    date_to: str,
    workers: int,
) -> None:
    """Fetch all games in a date range."""
    store: CacheStore = ctx.obj["store"]
    client: NHLClient = ctx.obj["client"]

    fetcher = SeasonFetcher(store, client)
    ingester = GameIngester(store, client, workers=workers)

    console.print(f"[bold]Fetching games {date_from} → {date_to}[/bold]")

    with console.status("Discovering games..."):
        games = fetcher.fetch_date_range(date_from, date_to)

    pending = store.get_pending_games(date_from=date_from, date_to=date_to)
    pending_ids = [r["game_id"] for r in pending]

    console.print(
        f"  {len(games)} games found | {len(pending_ids)} need ingestion"
    )

    if not pending_ids:
        console.print("  [green]All games already ingested.[/green]")
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TextColumn("[cyan]{task.fields[shots]} posts"),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"Ingesting {date_from}–{date_to}", total=len(pending_ids), shots=0
        )
        total_shots = 0

        def _cb(gid: int, shots: int) -> None:
            nonlocal total_shots
            total_shots += shots
            progress.advance(task)
            progress.update(task, shots=total_shots)

        result = ingester.ingest_batch(pending_ids, progress_callback=_cb)

    console.print(
        f"  [green]Done:[/green] {result.games_processed} games, "
        f"[cyan]{result.post_shots_found}[/cyan] post shots"
    )


@fetch.command("gp")
@click.option("--season", "seasons", multiple=True, required=True,
              help="Season code (repeatable), e.g. 20242025")
@click.option("--playoffs", is_flag=True, default=False,
              help="Fetch playoffs game logs instead of regular season")
@click.pass_context
def fetch_gp(ctx: click.Context, seasons: tuple[str, ...], playoffs: bool) -> None:
    """Fetch per-player game logs to compute accurate games-played counts."""
    store: CacheStore = ctx.obj["store"]
    client: NHLClient = ctx.obj["client"]
    game_type = 3 if playoffs else 2

    for season in seasons:
        _fetch_player_gp(store, client, season=season, game_type=game_type)


@fetch.command("game")
@click.argument("game_id", type=int)
@click.pass_context
def fetch_game(ctx: click.Context, game_id: int) -> None:
    """Fetch and ingest a single game by ID."""
    store: CacheStore = ctx.obj["store"]
    client: NHLClient = ctx.obj["client"]

    ingester = GameIngester(store, client)

    with console.status(f"Ingesting game {game_id}..."):
        shots = ingester.ingest_game(game_id)

    console.print(f"Game {game_id}: [cyan]{shots}[/cyan] post shots found")


# ------------------------------------------------------------------ #
# analyze group                                                        #
# ------------------------------------------------------------------ #

@main.group()
def analyze() -> None:
    """Run analysis queries and display results."""


@analyze.command("players")
@_filter_options
@click.pass_context
def analyze_players(ctx: click.Context, **kwargs: Any) -> None:
    """Per-player post shot leaderboard."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.player_summary(spec)

    title = _make_title("Post/Crossbar Shot Leaders", spec)
    _render_or_export(
        df, kwargs["fmt"], kwargs["output"],
        tables.render_player_leaderboard,
        title=title,
        sort_by=kwargs["sort_by"],
        limit=kwargs["limit"],
    )


@analyze.command("teams")
@_filter_options
@click.pass_context
def analyze_teams(ctx: click.Context, **kwargs: Any) -> None:
    """Per-team post shot breakdown."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.team_summary(spec)

    title = _make_title("Post/Crossbar Shots by Team", spec)
    _render_or_export(
        df, kwargs["fmt"], kwargs["output"],
        tables.render_team_summary,
        title=title,
        limit=kwargs["limit"],
    )


@analyze.command("types")
@_filter_options
@click.pass_context
def analyze_types(ctx: click.Context, **kwargs: Any) -> None:
    """Breakdown by shot type (wrist, slap, snap, etc.)."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.by_shot_type(spec)

    _render_or_export(
        df, kwargs["fmt"], kwargs["output"],
        tables.render_shot_types,
        title=_make_title("Post Shots by Shot Type", spec),
    )


@analyze.command("situations")
@_filter_options
@click.pass_context
def analyze_situations(ctx: click.Context, **kwargs: Any) -> None:
    """Breakdown by game situation (EV/PP/PK/EN)."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.by_strength(spec)

    _render_or_export(
        df, kwargs["fmt"], kwargs["output"],
        tables.render_by_strength,
        title=_make_title("Post Shots by Game Situation", spec),
    )


@analyze.command("periods")
@_filter_options
@click.pass_context
def analyze_periods(ctx: click.Context, **kwargs: Any) -> None:
    """Breakdown by period."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.by_period(spec)

    _render_or_export(
        df, kwargs["fmt"], kwargs["output"],
        tables.render_by_period,
        title=_make_title("Post Shots by Period", spec),
    )


@analyze.command("splits")
@_filter_options
@click.pass_context
def analyze_splits(ctx: click.Context, **kwargs: Any) -> None:
    """Home/away post shot splits per player."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.home_away_splits(spec)

    _render_or_export(
        df, kwargs["fmt"], kwargs["output"],
        tables.render_home_away,
        title=_make_title("Home/Away Post Shot Splits", spec),
        limit=kwargs["limit"],
    )


@analyze.command("trend")
@_filter_options
@click.option("--granularity", default="month",
              type=click.Choice(["month", "week"], case_sensitive=False),
              help="Time granularity")
@click.pass_context
def analyze_trend(ctx: click.Context, granularity: str, **kwargs: Any) -> None:
    """Post shots over time (weekly or monthly trend)."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.season_trend(spec, granularity=granularity)

    _render_or_export(
        df, kwargs["fmt"], kwargs["output"],
        tables.render_trend,
        granularity=granularity,
        title=_make_title("Post Shot Trend", spec),
    )


@analyze.command("locations")
@_filter_options
@click.option("--chart", is_flag=True, help="Render a heatmap (requires [charts] extra)")
@click.option("--chart-output", default=None, metavar="FILE",
              help="Save heatmap to PNG file")
@click.pass_context
def analyze_locations(ctx: click.Context, chart: bool, chart_output: str | None, **kwargs: Any) -> None:
    """Post shot locations on the ice."""
    store: CacheStore = ctx.obj["store"]

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)
    df = agg.by_location(spec)

    if df.empty:
        console.print("[yellow]No location data found.[/yellow]")
        return

    if chart or chart_output:
        from nhlposts.display.charts import shot_heatmap
        shot_heatmap(df, output=chart_output,
                     title=_make_title("Post Shot Locations", spec))
    else:
        # Show a zone summary
        if "zone_code" in df.columns:
            zone_counts = df.groupby("zone_code").size().reset_index(name="count")
            console.print("[bold]Shots by zone:[/bold]")
            for row in zone_counts.itertuples(index=False):
                zone = {"O": "Offensive", "D": "Defensive", "N": "Neutral"}.get(row.zone_code, row.zone_code)
                console.print(f"  {zone}: {row.count}")

        if kwargs["fmt"] in ("csv", "json"):
            exp.export_dataframe(df, kwargs["fmt"], kwargs["output"])
        else:
            console.print(f"[dim]{len(df)} post shots with coordinates. Use --chart to render heatmap.[/dim]")


# ------------------------------------------------------------------ #
# player command                                                       #
# ------------------------------------------------------------------ #

@main.command("player")
@click.argument("name_or_id")
@_filter_options
@click.pass_context
def player_detail(ctx: click.Context, name_or_id: str, **kwargs: Any) -> None:
    """Deep dive on a single player's post shots."""
    store: CacheStore = ctx.obj["store"]
    client: NHLClient = ctx.obj["client"]

    # Resolve player
    resolver = PlayerResolver(store, client)
    if name_or_id.isdigit():
        player_id = int(name_or_id)
        player = resolver.get_player(player_id)
        player_name = player.full_name if player else f"Player #{player_id}"
    else:
        matches = resolver.resolve_name(name_or_id)
        if not matches:
            console.print(f"[red]No player found matching '{name_or_id}'[/red]")
            console.print("[dim]Run 'nhlposts fetch season --season SEASON' first to load rosters.[/dim]")
            sys.exit(1)
        if len(matches) > 1:
            console.print(f"[yellow]Multiple matches:[/yellow]")
            for i, m in enumerate(matches[:5], 1):
                console.print(f"  {i}. {m.full_name} ({m.team_abbrev}, {m.position_code})")
            console.print(f"[dim]Using: {matches[0].full_name}[/dim]")
        player = matches[0]
        player_id = player.player_id
        player_name = player.full_name

    spec = _build_filter(ctx, store=store, **{
        k: kwargs[k] for k in [
            "seasons", "date_from", "date_to", "teams", "players",
            "positions", "strengths", "shot_types", "reasons", "periods",
            "home_away", "shoots", "season_type", "opponents", "min_events", "min_games_played",
        ]
    })

    agg = Aggregator(store)

    # Show summary first
    spec.player_ids = [player_id]
    summary = agg.summary_stats(spec)
    if summary:
        tables.render_summary_stats(summary, label=player_name)

    # Show all events
    df = agg.player_detail(player_id, spec)

    if kwargs["fmt"] in ("csv", "json"):
        exp.export_dataframe(df, kwargs["fmt"], kwargs["output"])
    else:
        tables.render_player_detail(df, player_name)

    # Show breakdown tables
    if not df.empty and kwargs["fmt"] == "table":
        console.print()
        types_df = agg.by_shot_type(spec)
        tables.render_shot_types(types_df, title=f"{player_name} — by Shot Type")

        strength_df = agg.by_strength(spec)
        tables.render_by_strength(strength_df, title=f"{player_name} — by Situation")


# ------------------------------------------------------------------ #
# cache group                                                          #
# ------------------------------------------------------------------ #

@main.group()
def cache() -> None:
    """Manage the local data cache."""


@cache.command("status")
@click.pass_context
def cache_status(ctx: click.Context) -> None:
    """Show what data is cached."""
    store: CacheStore = ctx.obj["store"]

    summary = store.cache_summary()
    if not summary:
        console.print("[yellow]No data cached yet.[/yellow]")
        console.print("[dim]Run 'nhlposts fetch season --season 20242025' to get started.[/dim]")
        return

    from rich.table import Table
    from rich import box as rbox

    table = Table(title="Cache Status", box=rbox.ROUNDED, header_style="bold cyan")
    table.add_column("Season", width=10)
    table.add_column("Total Games", justify="right")
    table.add_column("Ingested", justify="right")
    table.add_column("Pending", justify="right")
    table.add_column("Post Shots", style="cyan bold", justify="right")

    for row in summary:
        pending = row["total_games"] - row["ingested_games"]
        table.add_row(
            row["season"],
            str(row["total_games"]),
            str(row["ingested_games"]),
            str(pending),
            str(row["post_shots"]),
        )

    console.print(table)


@cache.command("clear")
@click.option("--season", "seasons", multiple=True, help="Season to clear (repeatable)")
@click.option("--all", "clear_all", is_flag=True, help="Clear ALL cached data")
@click.confirmation_option(prompt="Are you sure you want to delete cached data?")
@click.pass_context
def cache_clear(ctx: click.Context, seasons: tuple[str, ...], clear_all: bool) -> None:
    """Delete cached data."""
    store: CacheStore = ctx.obj["store"]

    if clear_all:
        store.clear_all()
        console.print("[green]All cached data cleared.[/green]")
    elif seasons:
        for season in seasons:
            store.clear_season(season)
            console.print(f"[green]Cleared data for season {season}.[/green]")
    else:
        console.print("[yellow]Specify --season SEASON or --all[/yellow]")


@cache.command("refresh")
@click.option("--season", "seasons", multiple=True, required=True)
@click.option("--workers", default=4, show_default=True, type=int)
@click.pass_context
def cache_refresh(ctx: click.Context, seasons: tuple[str, ...], workers: int) -> None:
    """Re-fetch any non-ingested games (e.g., after a failed fetch)."""
    store: CacheStore = ctx.obj["store"]
    client: NHLClient = ctx.obj["client"]
    ingester = GameIngester(store, client, workers=workers)

    for season in seasons:
        pending = store.get_pending_games(season=season)
        pending_ids = [r["game_id"] for r in pending]
        if not pending_ids:
            console.print(f"[green]Season {season} fully ingested.[/green]")
            continue

        console.print(f"Re-ingesting {len(pending_ids)} pending games for {season}...")
        with Progress(SpinnerColumn(), BarColumn(), TaskProgressColumn(), console=console) as progress:
            task = progress.add_task(season, total=len(pending_ids))
            result = ingester.ingest_batch(
                pending_ids, progress_callback=lambda gid, shots: progress.advance(task)
            )
        console.print(f"Done: {result.games_processed} games, {result.post_shots_found} post shots")


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _resolve_unknown_players(store: CacheStore, client: NHLClient, season: str) -> None:
    """Fetch player profiles for any IDs in post_shots not yet in the players table."""
    conn = store.get_connection()
    unknown = [
        r[0] for r in conn.execute(
            """
            SELECT DISTINCT ps.shooting_player_id
            FROM post_shots ps
            LEFT JOIN players p ON p.player_id = ps.shooting_player_id
            WHERE p.player_id IS NULL AND ps.season = ?
            """,
            (season,),
        ).fetchall()
    ]
    if not unknown:
        return
    resolver = PlayerResolver(store, client)
    resolved = 0
    for pid in unknown:
        if resolver.ensure_player_known(pid, season):
            resolved += 1
    if resolved:
        console.print(f"  [green]Resolved {resolved} previously unknown player(s)[/green]")


def _fetch_player_gp(
    store: CacheStore,
    client: NHLClient,
    season: str,
    game_type: int = 2,
) -> None:
    """Fetch game logs for all players with post shots in this season."""
    pairs = store.get_distinct_player_seasons(season=season)
    # Filter to the requested game_type if needed
    pairs = [(pid, s) for pid, s in pairs if s == season]
    if not pairs:
        return
    resolver = PlayerResolver(store, client)
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"Fetching game logs for {len(pairs)} players ({season})...",
            total=len(pairs),
        )

        def _cb(done: int, total: int) -> None:
            progress.update(task, completed=done)

        n = resolver.fetch_games_played_for_players(
            pairs, game_type=game_type, progress_callback=_cb
        )
    console.print(f"  [green]GP data:[/green] {n} new game logs fetched for {season}")


def _make_title(base: str, spec: FilterSpec) -> str:
    parts = []
    if spec.seasons:
        parts.append(", ".join(spec.seasons))
    if spec.date_from or spec.date_to:
        parts.append(f"{spec.date_from or ''}–{spec.date_to or ''}")
    if spec.strength_states:
        parts.append(" ".join(spec.strength_states))
    if spec.team_abbrevs:
        parts.append(", ".join(spec.team_abbrevs))
    if spec.reasons:
        labels = {"hit-crossbar": "CB", "hit-left-post": "L", "hit-right-post": "R"}
        parts.append("+".join(labels.get(r, r) for r in spec.reasons))
    suffix = f" ({', '.join(parts)})" if parts else ""
    return base + suffix


if __name__ == "__main__":
    main()
