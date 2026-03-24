"""Streamlit GUI for nhlposts."""

from __future__ import annotations


def launch() -> None:
    """Entry point for the nhlposts-gui command."""
    import subprocess
    import sys
    from pathlib import Path

    app = Path(__file__).parent / "app.py"
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(app), "--server.headless", "false"],
        check=True,
    )
