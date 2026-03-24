"""Optional matplotlib/seaborn charts for post shot analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _check_deps() -> bool:
    try:
        import matplotlib  # noqa: F401
        import seaborn  # noqa: F401
        return True
    except ImportError:
        return False


def shot_heatmap(
    df: pd.DataFrame,
    output: str | None = None,
    title: str = "Post/Crossbar Shot Locations",
) -> None:
    """
    Render a shot location heatmap on an NHL rink outline.

    Args:
        df: DataFrame with x_coord and y_coord columns (from aggregator.by_location).
        output: File path for PNG output. If None, opens interactive window.
        title: Chart title.
    """
    if not _check_deps():
        print("Install charts dependencies: pip install nhlposts[charts]")
        return

    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    import numpy as np

    fig, ax = plt.subplots(figsize=(12, 6))

    # Draw rink outline (half-rink: offensive zone x=25 to x=89)
    ax.set_xlim(24, 92)
    ax.set_ylim(-43, 43)
    ax.set_aspect("equal")
    ax.set_facecolor("#1a1a2e")
    fig.patch.set_facecolor("#1a1a2e")

    # Rink boundary (offensive zone half)
    rink = patches.FancyBboxPatch(
        (25, -42.5), 64, 85,
        boxstyle="round,pad=0,rounding_size=28",
        linewidth=2, edgecolor="white", facecolor="none"
    )
    ax.add_patch(rink)

    # Goal crease
    crease = patches.Arc(
        (89, 0), 8, 8, angle=0, theta1=90, theta2=270,
        linewidth=1.5, color="lightblue"
    )
    ax.add_patch(crease)

    # Goal line
    ax.plot([89, 89], [-3, 3], color="red", linewidth=2)

    # Blue line
    ax.axvline(x=25, color="blue", linewidth=2, alpha=0.7)

    # Faceoff circles
    for y in [-22, 22]:
        circle = plt.Circle((69, y), 15, color="red", fill=False, linewidth=1.5)
        ax.add_patch(circle)
        ax.plot(69, y, "ro", markersize=4)

    # Plot shots
    if not df.empty and "x_coord" in df.columns:
        colors = {
            "hit-crossbar": "#FFD700",
            "hit-left-post": "#00BFFF",
            "hit-right-post": "#00FF7F",
        }
        for reason, group in df.groupby("reason"):
            color = colors.get(reason, "white")
            # Normalize: shots should be plotted toward positive x
            x = group["x_coord"].abs()
            y = group["y_coord"]
            ax.scatter(x, y, c=color, alpha=0.6, s=50, label=reason, zorder=5)

    ax.set_title(title, color="white", fontsize=14, pad=12)
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_edgecolor("white")

    legend = ax.legend(
        facecolor="#2a2a4e", edgecolor="white", labelcolor="white", fontsize=9
    )

    plt.tight_layout()

    if output:
        plt.savefig(output, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        print(f"Saved heatmap to {output}")
    else:
        plt.show()

    plt.close(fig)


def trend_chart(
    df: pd.DataFrame,
    granularity: str = "month",
    output: str | None = None,
    title: str = "Post Shot Trend",
) -> None:
    """
    Line chart of post shots over time.

    Args:
        df: DataFrame from aggregator.season_trend().
        granularity: "week" or "month" (determines x-axis label).
        output: File path for PNG. If None, opens interactive window.
        title: Chart title.
    """
    if not _check_deps():
        print("Install charts dependencies: pip install nhlposts[charts]")
        return

    if df.empty:
        print("No trend data to chart.")
        return

    import matplotlib.pyplot as plt

    col = granularity if granularity in df.columns else df.columns[0]
    x = df[col].astype(str)
    y = df["post_shots"]

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    ax.plot(x, y, color="#00BFFF", linewidth=2, marker="o", markersize=5)
    ax.fill_between(range(len(x)), y, alpha=0.2, color="#00BFFF")

    # Also plot individual components
    if "crossbar" in df.columns:
        ax.plot(x, df["crossbar"], "--", color="#FFD700", linewidth=1, label="Crossbar")
    if "left_post" in df.columns:
        ax.plot(x, df["left_post"], "--", color="#00FF7F", linewidth=1, label="Left Post")
    if "right_post" in df.columns:
        ax.plot(x, df["right_post"], "--", color="#FF6B6B", linewidth=1, label="Right Post")

    ax.set_xticks(range(len(x)))
    ax.set_xticklabels(x, rotation=45, ha="right", color="white", fontsize=8)
    ax.tick_params(colors="white")
    ax.set_xlabel(granularity.capitalize(), color="white")
    ax.set_ylabel("Post Shots", color="white")
    ax.set_title(title, color="white", fontsize=14)
    for spine in ax.spines.values():
        spine.set_edgecolor("#444")

    if "crossbar" in df.columns:
        ax.legend(facecolor="#2a2a4e", edgecolor="white", labelcolor="white")

    plt.tight_layout()

    if output:
        plt.savefig(output, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        print(f"Saved trend chart to {output}")
    else:
        plt.show()

    plt.close(fig)
