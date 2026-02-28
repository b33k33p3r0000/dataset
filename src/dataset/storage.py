"""Parquet I/O — save, load, append OHLCV DataFrames."""

from pathlib import Path
from typing import Optional

import pandas as pd


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    """Save DataFrame to Parquet. Creates parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow")


def load_parquet(path: Path) -> Optional[pd.DataFrame]:
    """Load Parquet file. Returns None if file doesn't exist."""
    if not path.exists():
        return None
    return pd.read_parquet(path, engine="pyarrow")


def append_parquet(new_df: pd.DataFrame, path: Path) -> None:
    """Append new rows to existing Parquet. Deduplicates by index."""
    existing = load_parquet(path)
    if existing is None:
        save_parquet(new_df, path)
        return
    combined = pd.concat([existing, new_df])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)
    save_parquet(combined, path)
