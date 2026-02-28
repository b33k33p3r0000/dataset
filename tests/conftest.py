"""Shared test fixtures for dataset tests."""

import pandas as pd
import numpy as np


def make_ohlcv_df(
    n_bars: int = 100,
    tf_minutes: int = 60,
    start: str = "2025-01-01",
    seed: int = 42,
) -> pd.DataFrame:
    """Generate synthetic OHLCV DataFrame for testing.

    Returns DataFrame with DatetimeIndex (UTC) and columns:
    open, high, low, close, volume.
    """
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n_bars, freq=f"{tf_minutes}min", tz="UTC")
    close = 50000 + np.cumsum(rng.normal(0, 100, n_bars))
    spread = rng.uniform(50, 200, n_bars)

    return pd.DataFrame(
        {
            "open": close + rng.normal(0, 30, n_bars),
            "high": close + spread,
            "low": close - spread,
            "close": close,
            "volume": rng.uniform(100, 10000, n_bars),
        },
        index=idx,
    )
