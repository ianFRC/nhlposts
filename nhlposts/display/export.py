"""CSV and JSON export helpers."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


def export_dataframe(
    df: pd.DataFrame,
    fmt: str,
    output: str | None = None,
) -> None:
    """
    Export a DataFrame to CSV or JSON.

    Args:
        df: The DataFrame to export.
        fmt: "csv" or "json".
        output: File path to write to. If None, writes to stdout.
    """
    if df.empty:
        return

    if fmt == "csv":
        text = df.to_csv(index=False)
    elif fmt == "json":
        text = df.to_json(orient="records", indent=2)
    else:
        raise ValueError(f"Unknown format: {fmt!r}")

    if output:
        path = Path(output)
        path.write_text(text, encoding="utf-8")
        print(f"Exported {len(df)} rows to {path}")
    else:
        sys.stdout.write(text)
