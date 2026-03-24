"""
NHL Post/Crossbar Shot Analyzer — Streamlit GUI
Run with:  streamlit run nhlposts/gui/app.py
"""

from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from typing import Any

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config (must be first Streamlit call) ─────────────────────────────
st.set_page_config(
    page_title="NHL Post Tracker",
    page_icon="🏒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── App-level constants ────────────────────────────────────────────────────
# Priority: NHLPOSTS_DB env var → repo data/cache.db → ~/.nhlposts/cache.db
_REPO_DB = Path(__file__).parent.parent.parent / "data" / "cache.db"
DEFAULT_DB = (
    Path(os.environ["NHLPOSTS_DB"]) if "NHLPOSTS_DB" in os.environ
    else _REPO_DB if _REPO_DB.exists()
    else Path.home() / ".nhlposts" / "cache.db"
)

REASON_LABELS = {
    "hit-crossbar": "Crossbar",
    "hit-left-post": "Left Post",
    "hit-right-post": "Right Post",
}
REASON_COLORS = {
    "hit-crossbar": "#FFD700",
    "hit-left-post": "#00BFFF",
    "hit-right-post": "#00FF7F",
}
STRENGTH_COLORS = {
    "EV": "#4C9BE8",
    "PP": "#F5A623",
    "PK": "#E84C4C",
    "EN": "#A0A0A0",
}
SHOT_COLORS = px.colors.qualitative.Pastel

# ── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .metric-card {
        background: #1e2130;
        border-radius: 8px;
        padding: 16px 20px;
        text-align: center;
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #4FC3F7; }
    .metric-label { font-size: 0.8rem; color: #9E9E9E; text-transform: uppercase; letter-spacing: 1px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { padding: 8px 18px; border-radius: 6px 6px 0 0; }
    div[data-testid="stSidebarContent"] .stSelectbox label,
    div[data-testid="stSidebarContent"] .stMultiSelect label { font-size: 0.8rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════════════════════════════════════
# Cached resource helpers
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_store(db_path: str):
    from nhlposts.cache.store import CacheStore
    return CacheStore(Path(db_path))


@st.cache_resource
def get_client():
    from nhlposts.api.client import NHLClient
    return NHLClient(rate_limit=0.5)


def _spec_key(spec) -> str:
    """Stable cache key from a FilterSpec."""
    d = {
        "seasons": sorted(spec.seasons),
        "date_from": spec.date_from,
        "date_to": spec.date_to,
        "player_ids": sorted(spec.player_ids),
        "team_abbrevs": sorted(spec.team_abbrevs),
        "position_groups": sorted(spec.position_groups),
        "reasons": sorted(spec.reasons),
        "shot_types": sorted(spec.shot_types),
        "strength_states": sorted(spec.strength_states),
        "periods": sorted(spec.periods),
        "home_away": spec.home_away,
        "shoots": spec.shoots,
        "season_type": spec.season_type,
        "min_events": spec.min_events,
        "min_games_played": spec.min_games_played,
    }
    return hashlib.md5(json.dumps(d, sort_keys=True).encode()).hexdigest()


@st.cache_data(ttl=60)
def cached_player_summary(spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).player_summary(_spec)


@st.cache_data(ttl=60)
def cached_team_summary(spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).team_summary(_spec)


@st.cache_data(ttl=60)
def cached_shot_type(spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).by_shot_type(_spec)


@st.cache_data(ttl=60)
def cached_by_strength(spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).by_strength(_spec)


@st.cache_data(ttl=60)
def cached_by_period(spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).by_period(_spec)


@st.cache_data(ttl=60)
def cached_by_location(spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).by_location(_spec)


@st.cache_data(ttl=60)
def cached_home_away(spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).home_away_splits(_spec)


@st.cache_data(ttl=60)
def cached_trend(spec_key: str, _spec, granularity: str, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).season_trend(_spec, granularity=granularity)


@st.cache_data(ttl=60)
def cached_summary_stats(spec_key: str, _spec, _db_path: str) -> dict:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).summary_stats(_spec)


@st.cache_data(ttl=60)
def cached_player_detail(player_id: int, spec_key: str, _spec, _db_path: str) -> pd.DataFrame:
    from nhlposts.analysis.aggregator import Aggregator
    store = get_store(_db_path)
    return Aggregator(store).player_detail(player_id, _spec)


@st.cache_data(ttl=300)
def get_cached_seasons(_db_path: str) -> list[str]:
    store = get_store(_db_path)
    summary = store.cache_summary()
    return sorted([r["season"] for r in summary if r["ingested_games"] > 0], reverse=True)


@st.cache_data(ttl=300)
def get_cached_teams(_db_path: str) -> list[str]:
    store = get_store(_db_path)
    rows = store.get_all_games()
    teams = set()
    for r in rows:
        teams.add(r["home_team_abbrev"])
        teams.add(r["away_team_abbrev"])
    return sorted(t for t in teams if t)


@st.cache_data(ttl=300)
def get_all_players_df(_db_path: str) -> pd.DataFrame:
    store = get_store(_db_path)
    rows = store.get_all_players()
    if not rows:
        return pd.DataFrame(columns=["player_id", "first_name", "last_name", "full_name", "team_abbrev", "position_code"])
    data = [
        {
            "player_id": r["player_id"],
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "full_name": f"{r['first_name']} {r['last_name']}",
            "team_abbrev": r["team_abbrev"],
            "position_code": r["position_code"],
        }
        for r in rows
    ]
    return pd.DataFrame(data)


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar: filters + settings
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar() -> tuple[Any, str]:
    """Render the sidebar filters. Returns (FilterSpec, db_path)."""
    from nhlposts.analysis.filters import FilterSpec

    st.sidebar.title("🏒 NHL Post Tracker")
    st.sidebar.markdown("---")

    # ── Database path ────────────────────────────────────────────────────
    with st.sidebar.expander("⚙️ Settings", expanded=False):
        db_path = st.text_input(
            "Database path",
            value=str(DEFAULT_DB),
            key="db_path",
        )
    db_path = st.session_state.get("db_path", str(DEFAULT_DB))

    # ── Season / date scope ──────────────────────────────────────────────
    st.sidebar.markdown("### 📅 Season / Date")

    available_seasons = get_cached_seasons(db_path)
    if not available_seasons:
        st.sidebar.info("No data cached yet. Use the **Data** tab to fetch.")
        available_seasons = ["20242025", "20232024", "20222023"]

    seasons = st.sidebar.multiselect(
        "Seasons",
        options=available_seasons,
        default=available_seasons[:1] if available_seasons else [],
        key="filter_seasons",
    )

    use_dates = st.sidebar.toggle("Use date range instead", key="use_dates")
    date_from = date_to = None
    if use_dates:
        col1, col2 = st.sidebar.columns(2)
        date_from = col1.text_input("From", placeholder="2024-10-01", key="date_from")
        date_to = col2.text_input("To", placeholder="2025-04-18", key="date_to")
        seasons = []  # date range overrides season

    # ── Team / player ────────────────────────────────────────────────────
    st.sidebar.markdown("### 👤 Player / Team")

    available_teams = get_cached_teams(db_path)
    teams = st.sidebar.multiselect(
        "Teams",
        options=available_teams,
        key="filter_teams",
    )

    players_df = get_all_players_df(db_path)
    player_names = sorted(players_df["full_name"].tolist()) if not players_df.empty else []
    selected_player_names = st.sidebar.multiselect(
        "Players",
        options=player_names,
        key="filter_players",
    )
    player_ids: list[int] = []
    if selected_player_names and not players_df.empty:
        player_ids = players_df[
            players_df["full_name"].isin(selected_player_names)
        ]["player_id"].tolist()

    positions = st.sidebar.multiselect(
        "Position",
        options=["F", "D", "G"],
        key="filter_positions",
    )
    shoots = st.sidebar.selectbox(
        "Shoots",
        options=["Any", "L", "R"],
        key="filter_shoots",
    )

    # ── Shot filters ─────────────────────────────────────────────────────
    st.sidebar.markdown("### 🎯 Shot Filters")

    reasons = st.sidebar.multiselect(
        "Iron type",
        options=["hit-crossbar", "hit-left-post", "hit-right-post"],
        format_func=lambda x: REASON_LABELS[x],
        key="filter_reasons",
    )

    shot_types = st.sidebar.multiselect(
        "Shot type",
        options=["wrist", "snap", "slap", "tip-in", "backhand", "poke"],
        key="filter_shot_types",
    )

    strengths = st.sidebar.multiselect(
        "Situation",
        options=["EV", "PP", "PK", "EN"],
        key="filter_strengths",
    )

    periods = st.sidebar.multiselect(
        "Period",
        options=[1, 2, 3, 4],
        format_func=lambda x: {4: "OT"}.get(x, str(x)),
        key="filter_periods",
    )

    home_away_opt = st.sidebar.radio(
        "Home / Away",
        options=["All", "Home", "Away"],
        horizontal=True,
        key="filter_home_away",
    )
    home_away = None if home_away_opt == "All" else home_away_opt.lower()

    season_type_opt = st.sidebar.radio(
        "Season type",
        options=["All", "Regular", "Playoffs"],
        horizontal=True,
        key="filter_season_type",
    )
    season_type = {"Regular": 2, "Playoffs": 3}.get(season_type_opt)

    min_events = st.sidebar.slider(
        "Min post shots (players)",
        min_value=1, max_value=20, value=1,
        key="filter_min_events",
    )

    min_games_played = st.sidebar.slider(
        "Min games played",
        min_value=0, max_value=82, value=0, step=1,
        help="Filter to players who appeared in at least N games. Requires GP data (fetch gp).",
        key="filter_min_gp",
    )

    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Clear filter cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    spec = FilterSpec(
        seasons=seasons,
        date_from=date_from or None,
        date_to=date_to or None,
        player_ids=player_ids,
        team_abbrevs=teams,
        position_groups=positions,
        reasons=reasons,
        shot_types=shot_types,
        strength_states=strengths,
        periods=[p if p < 4 else 4 for p in periods],
        home_away=home_away,
        shoots=shoots if shoots != "Any" else None,
        season_type=season_type,
        min_events=min_events,
        min_games_played=min_games_played,
    )

    return spec, db_path


# ══════════════════════════════════════════════════════════════════════════════
# Chart helpers
# ══════════════════════════════════════════════════════════════════════════════

def bar_chart(df: pd.DataFrame, x: str, y: str, color: str | None = None,
              title: str = "", color_map: dict | None = None) -> go.Figure:
    kwargs: dict[str, Any] = dict(x=x, y=y, title=title, template="plotly_dark")
    if color:
        kwargs["color"] = color
    if color_map:
        kwargs["color_discrete_map"] = color_map
    fig = px.bar(df, **kwargs)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0), legend_title_text="")
    return fig


def pie_chart(df: pd.DataFrame, names: str, values: str,
              title: str = "", color_map: dict | None = None) -> go.Figure:
    kwargs: dict[str, Any] = dict(names=names, values=values, title=title, template="plotly_dark")
    if color_map:
        kwargs["color_discrete_map"] = color_map
    fig = px.pie(df, **kwargs, hole=0.4)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0), legend_title_text="")
    return fig


def line_chart(df: pd.DataFrame, x: str, title: str = "") -> go.Figure:
    fig = go.Figure()
    colors = {"post_shots": "#4FC3F7", "crossbar": "#FFD700",
               "left_post": "#00FF7F", "right_post": "#FF6B6B"}
    labels = {"post_shots": "Total", "crossbar": "Crossbar",
               "left_post": "Left Post", "right_post": "Right Post"}
    for col in ["post_shots", "crossbar", "left_post", "right_post"]:
        if col not in df.columns:
            continue
        dash = "solid" if col == "post_shots" else "dash"
        fig.add_trace(go.Scatter(
            x=df[x], y=df[col],
            mode="lines+markers",
            name=labels[col],
            line=dict(color=colors[col], dash=dash, width=2 if col == "post_shots" else 1.5),
            marker=dict(size=6),
        ))
    fig.update_layout(
        title=title, template="plotly_dark",
        margin=dict(l=0, r=0, t=40, b=0),
        xaxis_tickangle=-45,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def rink_heatmap(df: pd.DataFrame, title: str = "Shot Locations") -> go.Figure:
    """Plotly interactive rink heatmap (offensive zone)."""
    fig = go.Figure()

    # ── Rink shapes ──────────────────────────────────────────────────────
    rink_shapes = [
        # Offensive zone boundary (simplified rectangle)
        dict(type="rect", x0=25, x1=100, y0=-42.5, y1=42.5,
             line=dict(color="white", width=2), fillcolor="rgba(30,40,60,0.0)"),
        # Goal line
        dict(type="line", x0=89, x1=89, y0=-3, y1=3,
             line=dict(color="red", width=3)),
        # Blue line
        dict(type="line", x0=25, x1=25, y0=-42.5, y1=42.5,
             line=dict(color="#4488FF", width=3)),
        # Goal crease arc (approximated)
        dict(type="circle", x0=85, x1=93, y0=-4, y1=4,
             line=dict(color="lightblue", width=1.5),
             fillcolor="rgba(100,180,255,0.15)"),
        # Center ice dot
        dict(type="circle", x0=88.5, x1=89.5, y0=-0.5, y1=0.5,
             line=dict(color="red", width=1), fillcolor="red"),
    ]
    # Faceoff circles
    for y in [-22, 22]:
        rink_shapes.append(dict(
            type="circle", x0=54, x1=84, y0=y - 15, y1=y + 15,
            line=dict(color="red", width=1.5),
        ))
        rink_shapes.append(dict(
            type="circle", x0=68.5, x1=69.5, y0=y - 0.5, y1=y + 0.5,
            line=dict(color="red", width=1), fillcolor="red",
        ))

    # ── Shot scatter traces ──────────────────────────────────────────────
    if not df.empty and "x_coord" in df.columns:
        # Normalize: all shots should attack toward +x
        df = df.copy()
        df["plot_x"] = df["x_coord"].abs()
        df["plot_y"] = df["y_coord"]

        for reason, grp in df.groupby("reason"):
            label = REASON_LABELS.get(reason, reason)
            color = REASON_COLORS.get(reason, "white")
            hover = (
                grp.get("player_name", pd.Series([""] * len(grp))).fillna("")
                + "<br>Shot: " + grp.get("shot_type", pd.Series([""] * len(grp))).fillna("")
                + "<br>Situation: " + grp.get("strength_state", pd.Series([""] * len(grp))).fillna("")
                + "<br>Zone: " + grp.get("zone_code", pd.Series([""] * len(grp))).fillna("")
            )
            fig.add_trace(go.Scatter(
                x=grp["plot_x"], y=grp["plot_y"],
                mode="markers",
                name=label,
                marker=dict(
                    color=color, size=10, opacity=0.75,
                    line=dict(color="white", width=0.5),
                    symbol="circle",
                ),
                hovertext=hover,
                hovertemplate="%{hovertext}<extra>%{fullData.name}</extra>",
            ))

    fig.update_layout(
        title=title,
        template="plotly_dark",
        xaxis=dict(range=[23, 94], showgrid=False, zeroline=False, title=""),
        yaxis=dict(range=[-44, 44], showgrid=False, zeroline=False,
                   scaleanchor="x", scaleratio=1, title=""),
        shapes=rink_shapes,
        margin=dict(l=0, r=0, t=40, b=0),
        paper_bgcolor="#0e1117",
        plot_bgcolor="#1a2035",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=420,
    )
    return fig


def metric_card(label: str, value: str, col) -> None:
    col.markdown(
        f'<div class="metric-card">'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-label">{label}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Tab renderers
# ══════════════════════════════════════════════════════════════════════════════

def tab_dashboard(spec, db_path: str) -> None:
    key = _spec_key(spec)
    stats = cached_summary_stats(key, spec, db_path)

    if not stats or stats.get("total_post_shots", 0) == 0:
        st.info("No data for these filters. Adjust filters or fetch more data on the **Data** tab.")
        return

    # ── KPI row ──────────────────────────────────────────────────────────
    cols = st.columns(7)
    metric_card("Total Posts", str(int(stats.get("total_post_shots", 0))), cols[0])
    metric_card("Players", str(int(stats.get("unique_players", 0))), cols[1])
    metric_card("Games", str(int(stats.get("games_with_posts", 0))), cols[2])
    metric_card("Crossbars", str(int(stats.get("crossbar", 0))), cols[3])
    metric_card("Left Post", str(int(stats.get("left_post", 0))), cols[4])
    metric_card("Right Post", str(int(stats.get("right_post", 0))), cols[5])
    metric_card("Even Str.", str(int(stats.get("ev", 0))), cols[6])

    st.markdown("---")

    col_left, col_right = st.columns(2)

    # ── Iron breakdown pie ───────────────────────────────────────────────
    iron_df = pd.DataFrame([
        {"type": "Crossbar", "count": int(stats.get("crossbar", 0))},
        {"type": "Left Post", "count": int(stats.get("left_post", 0))},
        {"type": "Right Post", "count": int(stats.get("right_post", 0))},
    ])
    iron_df = iron_df[iron_df["count"] > 0]
    if not iron_df.empty:
        col_left.plotly_chart(
            pie_chart(iron_df, "type", "count", "Iron Type Split",
                      color_map={"Crossbar": "#FFD700", "Left Post": "#00BFFF", "Right Post": "#00FF7F"}),
            use_container_width=True,
        )

    # ── Situation breakdown pie ──────────────────────────────────────────
    sit_df = pd.DataFrame([
        {"sit": k, "count": int(stats.get(v, 0))}
        for k, v in [("EV", "ev"), ("PP", "pp"), ("PK", "pk")]
    ])
    sit_df = sit_df[sit_df["count"] > 0]
    if not sit_df.empty:
        col_right.plotly_chart(
            pie_chart(sit_df, "sit", "count", "Situation Split", color_map=STRENGTH_COLORS),
            use_container_width=True,
        )

    # ── Top 10 players bar ───────────────────────────────────────────────
    df = cached_player_summary(key, spec, db_path)
    if not df.empty:
        top10 = df.nlargest(10, "post_shots")[["player_name", "post_shots", "crossbar", "left_post", "right_post"]].copy()
        fig = go.Figure()
        for col_name, label, color in [
            ("crossbar", "Crossbar", "#FFD700"),
            ("left_post", "Left Post", "#00BFFF"),
            ("right_post", "Right Post", "#00FF7F"),
        ]:
            fig.add_trace(go.Bar(
                name=label, x=top10["player_name"], y=top10[col_name],
                marker_color=color,
            ))
        fig.update_layout(
            barmode="stack", title="Top 10 Players — Post/Crossbar Shots",
            template="plotly_dark",
            xaxis_tickangle=-30,
            margin=dict(l=0, r=0, t=40, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)


def tab_players(spec, db_path: str) -> None:
    key = _spec_key(spec)
    df = cached_player_summary(key, spec, db_path)

    if df.empty:
        st.info("No player data for these filters.")
        return

    col1, col2, col3 = st.columns([2, 2, 1])
    sort_col = col1.selectbox("Sort by", options=[
        "post_shots", "post_per_game", "crossbar", "left_post", "right_post",
        "ev", "pp", "pk", "wrist", "slap", "snap",
    ], key="player_sort")
    chart_metric = col2.selectbox("Chart metric", options=[
        "post_shots", "post_per_game", "crossbar", "left_post", "right_post",
    ], key="player_chart_metric")
    top_n = col3.number_input("Show top N", min_value=5, max_value=100, value=25, key="player_top_n")

    df_sorted = df.sort_values(sort_col, ascending=False).head(int(top_n)).reset_index(drop=True)
    df_sorted.index += 1

    # ── Bar chart ────────────────────────────────────────────────────────
    fig = go.Figure()
    for col_name, label, color in [
        ("crossbar", "Crossbar", "#FFD700"),
        ("left_post", "Left Post", "#00BFFF"),
        ("right_post", "Right Post", "#00FF7F"),
    ]:
        if col_name in df_sorted.columns:
            fig.add_trace(go.Bar(name=label, x=df_sorted["player_name"],
                                  y=df_sorted[col_name], marker_color=color))
    fig.update_layout(
        barmode="stack",
        title=f"Top {top_n} Players — {chart_metric.replace('_', ' ').title()}",
        template="plotly_dark",
        xaxis_tickangle=-35,
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Table ────────────────────────────────────────────────────────────
    display_cols = ["player_name", "team", "position", "games_played", "games_with_post",
                    "post_shots", "post_per_game", "post_pct_of_shots",
                    "crossbar", "left_post", "right_post",
                    "crossbar_pct", "ev", "pp", "pk", "wrist", "slap", "snap"]
    display_cols = [c for c in display_cols if c in df_sorted.columns]

    col_config = {
        "player_name": st.column_config.TextColumn("Player", width="medium"),
        "team": st.column_config.TextColumn("Team", width="small"),
        "position": st.column_config.TextColumn("Pos", width="small"),
        "games_played": st.column_config.NumberColumn("GP", format="%d"),
        "games_with_post": st.column_config.NumberColumn("GP w/Post", format="%d"),
        "post_shots": st.column_config.NumberColumn("Posts", format="%d"),
        "post_per_game": st.column_config.NumberColumn("Post/GP", format="%.3f"),
        "post_pct_of_shots": st.column_config.NumberColumn("Post%", format="%.2f%%"),
        "crossbar": st.column_config.NumberColumn("CB", format="%d"),
        "left_post": st.column_config.NumberColumn("L", format="%d"),
        "right_post": st.column_config.NumberColumn("R", format="%d"),
        "crossbar_pct": st.column_config.NumberColumn("CB%", format="%.1f%%"),
        "ev": st.column_config.NumberColumn("EV", format="%d"),
        "pp": st.column_config.NumberColumn("PP", format="%d"),
        "pk": st.column_config.NumberColumn("PK", format="%d"),
        "wrist": st.column_config.NumberColumn("Wrist", format="%d"),
        "slap": st.column_config.NumberColumn("Slap", format="%d"),
        "snap": st.column_config.NumberColumn("Snap", format="%d"),
    }
    st.dataframe(
        df_sorted[display_cols],
        use_container_width=True,
        height=500,
        column_config=col_config,
    )

    # ── Download ─────────────────────────────────────────────────────────
    st.download_button(
        "⬇ Download CSV",
        data=df_sorted[display_cols].to_csv(index=False),
        file_name="nhl_post_shots_players.csv",
        mime="text/csv",
    )


def tab_teams(spec, db_path: str) -> None:
    key = _spec_key(spec)
    df = cached_team_summary(key, spec, db_path)

    if df.empty:
        st.info("No team data for these filters.")
        return

    col1, col2 = st.columns(2)

    # ── Stacked bar ──────────────────────────────────────────────────────
    fig = go.Figure()
    for col_name, label, color in [
        ("crossbar", "Crossbar", "#FFD700"),
        ("left_post", "Left Post", "#00BFFF"),
        ("right_post", "Right Post", "#00FF7F"),
    ]:
        fig.add_trace(go.Bar(name=label, x=df["team"], y=df[col_name], marker_color=color))
    fig.update_layout(
        barmode="stack", title="Post Shots by Team",
        template="plotly_dark",
        xaxis_tickangle=-45,
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=380,
    )
    col1.plotly_chart(fig, use_container_width=True)

    # ── Per-game scatter ─────────────────────────────────────────────────
    fig2 = px.scatter(
        df, x="games", y="post_shots", text="team",
        title="Post Shots vs Games Played",
        template="plotly_dark",
        labels={"games": "Games Played", "post_shots": "Post Shots"},
        color="post_per_game",
        color_continuous_scale="Blues",
    )
    fig2.update_traces(textposition="top center", marker_size=10)
    fig2.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=380)
    col2.plotly_chart(fig2, use_container_width=True)

    # ── Table ────────────────────────────────────────────────────────────
    display_cols = ["team", "games", "post_shots", "post_per_game",
                    "crossbar", "left_post", "right_post", "ev", "pp", "pk", "en"]
    display_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(
        df[display_cols].reset_index(drop=True),
        use_container_width=True,
        column_config={
            "post_shots": st.column_config.NumberColumn("Posts", format="%d"),
            "post_per_game": st.column_config.NumberColumn("Post/GP", format="%.3f"),
        },
    )

    st.download_button(
        "⬇ Download CSV",
        data=df[display_cols].to_csv(index=False),
        file_name="nhl_post_shots_teams.csv",
        mime="text/csv",
    )


def tab_shot_analysis(spec, db_path: str) -> None:
    key = _spec_key(spec)

    sub_tab1, sub_tab2, sub_tab3, sub_tab4 = st.tabs(
        ["Shot Type", "Game Situation", "Period", "Home / Away"]
    )

    with sub_tab1:
        df = cached_shot_type(key, spec, db_path)
        if df.empty:
            st.info("No data.")
        else:
            col1, col2 = st.columns(2)
            col1.plotly_chart(
                pie_chart(df, "shot_type", "post_shots", "Post Shots by Shot Type"),
                use_container_width=True,
            )
            fig = go.Figure()
            for col_name, label, color in [
                ("crossbar", "Crossbar", "#FFD700"),
                ("left_post", "Left Post", "#00BFFF"),
                ("right_post", "Right Post", "#00FF7F"),
            ]:
                fig.add_trace(go.Bar(name=label, x=df["shot_type"],
                                      y=df[col_name], marker_color=color))
            fig.update_layout(barmode="stack", template="plotly_dark",
                               title="Iron Type by Shot Type",
                               margin=dict(l=0, r=0, t=40, b=0))
            col2.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)

    with sub_tab2:
        df = cached_by_strength(key, spec, db_path)
        if df.empty:
            st.info("No data.")
        else:
            col1, col2 = st.columns(2)
            col1.plotly_chart(
                pie_chart(df, "strength_state", "post_shots", "Post Shots by Situation",
                          color_map=STRENGTH_COLORS),
                use_container_width=True,
            )
            fig = go.Figure()
            for col_name, label, color in [
                ("crossbar", "Crossbar", "#FFD700"),
                ("left_post", "Left Post", "#00BFFF"),
                ("right_post", "Right Post", "#00FF7F"),
            ]:
                fig.add_trace(go.Bar(name=label, x=df["strength_state"],
                                      y=df[col_name], marker_color=color))
            fig.update_layout(barmode="stack", template="plotly_dark",
                               title="Iron Type by Situation",
                               margin=dict(l=0, r=0, t=40, b=0))
            col2.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)

    with sub_tab3:
        df = cached_by_period(key, spec, db_path)
        if df.empty:
            st.info("No data.")
        else:
            df["period_label"] = df.apply(
                lambda r: "OT" if r["period_type"] == "OT" else f"P{int(r['period'])}", axis=1
            )
            col1, col2 = st.columns(2)
            col1.plotly_chart(
                bar_chart(df, "period_label", "post_shots", title="Post Shots by Period"),
                use_container_width=True,
            )
            fig = go.Figure()
            for col_name, label, color in [
                ("crossbar", "Crossbar", "#FFD700"),
                ("left_post", "Left Post", "#00BFFF"),
                ("right_post", "Right Post", "#00FF7F"),
            ]:
                fig.add_trace(go.Bar(name=label, x=df["period_label"],
                                      y=df[col_name], marker_color=color))
            fig.update_layout(barmode="stack", template="plotly_dark",
                               title="Iron Type by Period",
                               margin=dict(l=0, r=0, t=40, b=0))
            col2.plotly_chart(fig, use_container_width=True)

    with sub_tab4:
        df = cached_home_away(key, spec, db_path)
        if df.empty:
            st.info("No data.")
        else:
            top20 = df.head(20)
            fig = go.Figure()
            fig.add_trace(go.Bar(name="Home", x=top20["player_name"],
                                  y=top20["home"], marker_color="#4FC3F7"))
            fig.add_trace(go.Bar(name="Away", x=top20["player_name"],
                                  y=top20["away"], marker_color="#F06292"))
            fig.update_layout(
                barmode="stack", template="plotly_dark",
                title="Home/Away Post Shots (Top 20 Players)",
                xaxis_tickangle=-35,
                margin=dict(l=0, r=0, t=40, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)


def tab_player_spotlight(spec, db_path: str) -> None:
    players_df = get_all_players_df(db_path)

    if players_df.empty:
        st.info("No players in cache. Fetch a season first.")
        return

    col1, col2 = st.columns([3, 1])
    search = col1.text_input("Search player name", placeholder="e.g. Cole Perfetti", key="spotlight_search")

    selected_name = None
    if search:
        try:
            from rapidfuzz import process, fuzz
            results = process.extract(search, players_df["full_name"].tolist(),
                                       scorer=fuzz.WRatio, limit=8, score_cutoff=60)
            matches = [r[0] for r in results]
        except ImportError:
            matches = [n for n in players_df["full_name"].tolist() if search.lower() in n.lower()][:8]

        if matches:
            selected_name = col2.selectbox("Match", matches, key="spotlight_match")
        else:
            st.warning(f"No player found matching '{search}'.")
            return
    else:
        # Default to the first player from filter if set
        if spec.player_ids:
            row = players_df[players_df["player_id"].isin(spec.player_ids)]
            if not row.empty:
                selected_name = row.iloc[0]["full_name"]
        if not selected_name:
            st.info("Type a player name above to search.")
            return

    if not selected_name:
        return

    row = players_df[players_df["full_name"] == selected_name]
    if row.empty:
        return
    player_id = int(row.iloc[0]["player_id"])
    team = row.iloc[0]["team_abbrev"]
    pos = row.iloc[0]["position_code"]

    # Header
    st.markdown(f"### {selected_name} &nbsp; <span style='color:#9E9E9E; font-size:1rem'>{team} · {pos}</span>",
                unsafe_allow_html=True)

    from nhlposts.analysis.filters import FilterSpec
    player_spec = FilterSpec(
        seasons=spec.seasons,
        date_from=spec.date_from,
        date_to=spec.date_to,
        strength_states=spec.strength_states,
        reasons=spec.reasons,
        shot_types=spec.shot_types,
        periods=spec.periods,
        season_type=spec.season_type,
        player_ids=[player_id],
    )
    pkey = _spec_key(player_spec)

    stats = cached_summary_stats(pkey, player_spec, db_path)
    if not stats or stats.get("total_post_shots", 0) == 0:
        st.info(f"No post shots found for {selected_name} with these filters.")
        return

    # ── KPI row ──────────────────────────────────────────────────────────
    cols = st.columns(6)
    metric_card("Post Shots", str(int(stats.get("total_post_shots", 0))), cols[0])
    metric_card("Games w/ Post", str(int(stats.get("games_with_posts", 0))), cols[1])
    metric_card("Crossbars", str(int(stats.get("crossbar", 0))), cols[2])
    metric_card("Left Post", str(int(stats.get("left_post", 0))), cols[3])
    metric_card("Right Post", str(int(stats.get("right_post", 0))), cols[4])
    metric_card("EV / PP / PK",
                f"{int(stats.get('ev',0))} / {int(stats.get('pp',0))} / {int(stats.get('pk',0))}",
                cols[5])
    st.markdown("")

    col_left, col_right = st.columns(2)

    # ── Iron type pie ─────────────────────────────────────────────────────
    iron_df = pd.DataFrame([
        {"type": "Crossbar", "count": int(stats.get("crossbar", 0))},
        {"type": "Left Post", "count": int(stats.get("left_post", 0))},
        {"type": "Right Post", "count": int(stats.get("right_post", 0))},
    ])
    iron_df = iron_df[iron_df["count"] > 0]
    if not iron_df.empty:
        col_left.plotly_chart(
            pie_chart(iron_df, "type", "count", "Iron Type Split",
                      color_map={"Crossbar": "#FFD700", "Left Post": "#00BFFF", "Right Post": "#00FF7F"}),
            use_container_width=True,
        )

    # ── Shot type bar ─────────────────────────────────────────────────────
    type_df = cached_shot_type(pkey, player_spec, db_path)
    if not type_df.empty:
        col_right.plotly_chart(
            bar_chart(type_df, "shot_type", "post_shots", title="Post Shots by Shot Type"),
            use_container_width=True,
        )

    # ── Event log table ──────────────────────────────────────────────────
    st.markdown("#### Game Log")
    detail_df = cached_player_detail(player_id, pkey, player_spec, db_path)
    if not detail_df.empty:
        display_df = detail_df.copy()
        display_df["reason"] = display_df["reason"].map(REASON_LABELS).fillna(display_df["reason"])
        st.dataframe(
            display_df,
            use_container_width=True,
            column_config={
                "game_date": st.column_config.TextColumn("Date", width="small"),
                "matchup": st.column_config.TextColumn("Matchup"),
                "period": st.column_config.NumberColumn("Per", format="%d", width="small"),
                "time_in_period": st.column_config.TextColumn("Time", width="small"),
                "reason": st.column_config.TextColumn("Iron Hit"),
                "shot_type": st.column_config.TextColumn("Shot Type"),
                "strength_state": st.column_config.TextColumn("Sit", width="small"),
                "zone_code": st.column_config.TextColumn("Zone", width="small"),
            },
        )
        st.download_button(
            f"⬇ Download {selected_name}'s events",
            data=detail_df.to_csv(index=False),
            file_name=f"{selected_name.replace(' ', '_')}_post_shots.csv",
            mime="text/csv",
        )

    # ── Shot map ─────────────────────────────────────────────────────────
    loc_df = cached_by_location(pkey, player_spec, db_path)
    if not loc_df.empty:
        st.markdown("#### Shot Map")
        st.plotly_chart(
            rink_heatmap(loc_df, title=f"{selected_name} — Post Shot Locations"),
            use_container_width=True,
        )


def tab_shot_map(spec, db_path: str) -> None:
    key = _spec_key(spec)
    df = cached_by_location(key, spec, db_path)

    if df.empty:
        st.info("No location data for these filters.")
        return

    st.markdown(f"**{len(df)} shots plotted** — hover over points for details")
    st.plotly_chart(rink_heatmap(df, title="Post/Crossbar Shot Locations"), use_container_width=True)

    # ── Zone summary ─────────────────────────────────────────────────────
    if "zone_code" in df.columns:
        zone_map = {"O": "Offensive", "D": "Defensive", "N": "Neutral"}
        zone_df = df.groupby("zone_code").size().reset_index(name="count")
        zone_df["zone"] = zone_df["zone_code"].map(zone_map).fillna(zone_df["zone_code"])
        cols = st.columns(len(zone_df))
        for i, row in enumerate(zone_df.itertuples(index=False)):
            metric_card(row.zone, str(row.count), cols[i])


def tab_trend(spec, db_path: str) -> None:
    key = _spec_key(spec)
    gran = st.radio("Granularity", ["month", "week"], horizontal=True, key="trend_gran")
    df = cached_trend(key, spec, gran, db_path)

    if df.empty:
        st.info("No trend data for these filters.")
        return

    time_col = gran if gran in df.columns else df.columns[0]
    st.plotly_chart(
        line_chart(df, time_col, title=f"Post Shots Over Time ({gran.title()}ly)"),
        use_container_width=True,
    )
    st.dataframe(df, use_container_width=True)
    st.download_button(
        "⬇ Download Trend CSV",
        data=df.to_csv(index=False),
        file_name="nhl_post_shots_trend.csv",
        mime="text/csv",
    )


def tab_data(db_path: str) -> None:
    st.markdown("### Fetch & Cache Management")

    if Path(db_path) == _REPO_DB:
        st.info(
            "**Read-only mode** — this deployment reads data from the repository. "
            "To update data, run `nhlposts fetch season` locally, copy the DB to "
            "`data/cache.db`, and push to GitHub.",
            icon="ℹ️",
        )

    # ── Cache status ─────────────────────────────────────────────────────
    store = get_store(db_path)
    summary = store.cache_summary()

    if summary:
        st.markdown("#### Cached Data")
        summary_df = pd.DataFrame(summary)
        summary_df["pending"] = summary_df["total_games"] - summary_df["ingested_games"]
        st.dataframe(
            summary_df.rename(columns={
                "season": "Season",
                "total_games": "Total Games",
                "ingested_games": "Ingested",
                "pending": "Pending",
                "post_shots": "Post Shots",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No data cached yet.")

    st.markdown("---")
    st.markdown("#### Fetch New Data")

    col1, col2 = st.columns(2)

    # ── Fetch season ─────────────────────────────────────────────────────
    with col1:
        st.markdown("**By Season**")
        season_input = st.text_input(
            "Season code(s)", value="20242025",
            help="e.g. 20242025 — separate multiple with commas",
            key="fetch_season_input",
        )
        fetch_playoffs = st.checkbox("Fetch playoffs", key="fetch_playoffs")
        fetch_rosters = st.checkbox("Fetch rosters", value=True, key="fetch_rosters")
        workers = st.slider("Concurrent workers", 1, 8, 4, key="fetch_workers")

        if st.button("🏒 Fetch Season(s)", use_container_width=True, key="btn_fetch_season"):
            seasons_to_fetch = [s.strip() for s in season_input.split(",") if s.strip()]
            if not seasons_to_fetch:
                st.error("Enter at least one season code.")
            else:
                _run_fetch_season(db_path, seasons_to_fetch, fetch_playoffs, fetch_rosters, workers)

    # ── Fetch date range ──────────────────────────────────────────────────
    with col2:
        st.markdown("**By Date Range**")
        d_from = st.text_input("From date", placeholder="2025-01-01", key="fetch_date_from")
        d_to = st.text_input("To date", placeholder="2025-03-31", key="fetch_date_to")

        if st.button("📅 Fetch Date Range", use_container_width=True, key="btn_fetch_dates"):
            if not d_from or not d_to:
                st.error("Enter both a from and to date.")
            else:
                _run_fetch_dates(db_path, d_from, d_to, workers=4)

    st.markdown("---")
    st.markdown("#### Refresh Games-Played Data")
    st.caption(
        "Re-fetch player game logs to ensure posts/game uses each player's actual GP "
        "(not team games). Runs automatically after season fetch."
    )
    gp_season_input = st.text_input(
        "Season code(s)", value="20242025",
        help="e.g. 20242025 — separate multiple with commas",
        key="gp_season_input",
    )
    gp_playoffs = st.checkbox("Playoffs game logs", key="gp_playoffs")
    if st.button("🔄 Refresh GP Data", use_container_width=True, key="btn_refresh_gp"):
        gp_seasons = [s.strip() for s in gp_season_input.split(",") if s.strip()]
        if not gp_seasons:
            st.error("Enter at least one season code.")
        else:
            game_type = 3 if gp_playoffs else 2
            _run_fetch_gp(db_path, gp_seasons, game_type)
            st.cache_data.clear()
            st.rerun()

    st.markdown("---")
    st.markdown("#### Cache Management")

    col3, col4 = st.columns(2)
    with col3:
        clear_season = st.text_input("Clear season", placeholder="20242025", key="clear_season_input")
        if st.button("🗑 Clear Season", use_container_width=True, key="btn_clear_season"):
            if clear_season:
                store.clear_season(clear_season)
                st.cache_data.clear()
                st.success(f"Cleared season {clear_season}.")
                st.rerun()
    with col4:
        st.markdown("&nbsp;", unsafe_allow_html=True)
        st.markdown("&nbsp;", unsafe_allow_html=True)
        if st.button("⚠️ Clear ALL Data", use_container_width=True, key="btn_clear_all",
                     type="secondary"):
            if "clear_all_confirm" not in st.session_state:
                st.session_state["clear_all_confirm"] = True
                st.warning("Click again to confirm clearing ALL data.")
            else:
                del st.session_state["clear_all_confirm"]
                store.clear_all()
                st.cache_data.clear()
                st.success("All data cleared.")
                st.rerun()


def _run_fetch_season(db_path: str, seasons: list[str], playoffs: bool,
                       rosters: bool, workers: int) -> None:
    from nhlposts.api.client import NHLClient
    from nhlposts.cache.store import CacheStore
    from nhlposts.ingestion.games import GameIngester
    from nhlposts.ingestion.players import PlayerResolver
    from nhlposts.ingestion.season import SeasonFetcher

    store = get_store(db_path)
    client = get_client()
    game_type = 3 if playoffs else 2

    for season in seasons:
        with st.status(f"Fetching season {season}...", expanded=True) as status:
            if rosters:
                st.write("Loading rosters...")
                resolver = PlayerResolver(store, client)
                n = resolver.fetch_all_rosters(season)
                st.write(f"✓ Loaded {n} players")

            st.write("Discovering games...")
            fetcher = SeasonFetcher(store, client)
            games = fetcher.fetch_season(season, game_type=game_type)

            pending = store.get_pending_games(season=season)
            pending_ids = [r["game_id"] for r in pending]
            st.write(f"✓ {len(games)} games found, {len(pending_ids)} to ingest")

            if not pending_ids:
                status.update(label=f"Season {season} already fully ingested ✓", state="complete")
            else:
                ingester = GameIngester(store, client, workers=workers)
                progress_bar = st.progress(0, text="Ingesting games...")
                counter = {"done": 0, "shots": 0}

                def _cb(gid: int, shots: int) -> None:
                    counter["done"] += 1
                    counter["shots"] += shots

                # Run ingestion (blocking — shows progress after)
                result = ingester.ingest_batch(pending_ids, progress_callback=_cb)
                progress_bar.progress(1.0, text=f"Done! {result.post_shots_found} post shots found")
                status.update(
                    label=f"Season {season} ✓ — {result.games_processed} games, {result.post_shots_found} post shots",
                    state="complete",
                )

        # Auto-fetch player game logs for accurate GP
        _run_fetch_gp(db_path, [season], game_type)

    st.cache_data.clear()
    st.rerun()


def _run_fetch_gp(db_path: str, seasons: list[str], game_type: int = 2) -> None:
    """Fetch player game logs for accurate GP counts."""
    from nhlposts.ingestion.players import PlayerResolver

    store = get_store(db_path)
    client = get_client()
    resolver = PlayerResolver(store, client)

    for season in seasons:
        pairs = store.get_distinct_player_seasons(season=season)
        pairs = [(pid, s) for pid, s in pairs if s == season]
        if not pairs:
            continue
        with st.status(f"Fetching game logs for {len(pairs)} players ({season})...", expanded=False) as status:
            n = resolver.fetch_games_played_for_players(pairs, game_type=game_type)
            status.update(
                label=f"GP data ✓ — {n} new game logs fetched for {season}",
                state="complete",
            )


def _run_fetch_dates(db_path: str, date_from: str, date_to: str, workers: int = 4) -> None:
    from nhlposts.ingestion.games import GameIngester
    from nhlposts.ingestion.season import SeasonFetcher

    store = get_store(db_path)
    client = get_client()

    with st.status(f"Fetching {date_from} → {date_to}...", expanded=True) as status:
        st.write("Discovering games...")
        fetcher = SeasonFetcher(store, client)
        games = fetcher.fetch_date_range(date_from, date_to)

        pending = store.get_pending_games(date_from=date_from, date_to=date_to)
        pending_ids = [r["game_id"] for r in pending]
        st.write(f"✓ {len(games)} games found, {len(pending_ids)} to ingest")

        if not pending_ids:
            status.update(label="All games already ingested ✓", state="complete")
            st.cache_data.clear()
            st.rerun()
            return

        ingester = GameIngester(store, client, workers=workers)
        st.progress(0, text="Ingesting...")
        result = ingester.ingest_batch(pending_ids)
        status.update(
            label=f"Done ✓ — {result.games_processed} games, {result.post_shots_found} post shots",
            state="complete",
        )

    st.cache_data.clear()
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# Main app
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    spec, db_path = render_sidebar()

    # Active filter summary
    active = []
    if spec.seasons:
        active.append(f"Seasons: {', '.join(spec.seasons)}")
    if spec.date_from or spec.date_to:
        active.append(f"Dates: {spec.date_from or ''}–{spec.date_to or ''}")
    if spec.team_abbrevs:
        active.append(f"Teams: {', '.join(spec.team_abbrevs)}")
    if spec.strength_states:
        active.append(f"Sit: {', '.join(spec.strength_states)}")
    if spec.reasons:
        active.append(", ".join(REASON_LABELS[r] for r in spec.reasons))
    if spec.shot_types:
        active.append(f"Shot: {', '.join(spec.shot_types)}")

    st.title("🏒 NHL Post & Crossbar Shot Analyzer")
    if active:
        st.caption("  ·  ".join(active))

    tabs = st.tabs([
        "📊 Dashboard",
        "👤 Players",
        "🏙 Teams",
        "🎯 Shot Analysis",
        "🔍 Player Spotlight",
        "🗺 Shot Map",
        "📈 Trend",
        "💾 Data",
    ])

    with tabs[0]:
        tab_dashboard(spec, db_path)
    with tabs[1]:
        tab_players(spec, db_path)
    with tabs[2]:
        tab_teams(spec, db_path)
    with tabs[3]:
        tab_shot_analysis(spec, db_path)
    with tabs[4]:
        tab_player_spotlight(spec, db_path)
    with tabs[5]:
        tab_shot_map(spec, db_path)
    with tabs[6]:
        tab_trend(spec, db_path)
    with tabs[7]:
        tab_data(db_path)


if __name__ == "__main__":
    main()
