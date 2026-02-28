"""Incremental daily update — fetch new bars and append."""

import logging
import time
from pathlib import Path
from typing import Optional

from dataset.config import (
    SYMBOLS,
    TIMEFRAMES,
    TF_MS,
    SLEEP_BETWEEN_REQUESTS,
    SLEEP_BETWEEN_PAIRS,
    DATA_DIR,
    symbol_to_dirname,
)
from dataset.fetch import fetch_ohlcv_paginated, utcnow_ms
from dataset.storage import load_parquet, append_parquet

logger = logging.getLogger(__name__)


def update_symbol_tf(
    exchange,
    symbol: str,
    tf: str,
    data_dir: Path,
) -> int:
    """Update one symbol/TF pair. Returns number of new bars appended."""
    dirname = symbol_to_dirname(symbol)
    path = data_dir / dirname / f"{tf}.parquet"

    existing = load_parquet(path)
    if existing is None:
        logger.warning("  %s %s: no existing file, skipping (run fetch_all first)", symbol, tf)
        return 0

    last_ts = existing.index[-1]
    since_ms = int(last_ts.timestamp() * 1000) + TF_MS[tf]
    until_ms = utcnow_ms()

    if since_ms >= until_ms:
        logger.info("  %s %s: already up to date", symbol, tf)
        return 0

    new_df = fetch_ohlcv_paginated(exchange, symbol, tf, since_ms, until_ms)
    if new_df.empty:
        logger.info("  %s %s: no new bars", symbol, tf)
        return 0

    append_parquet(new_df, path)
    logger.info("  %s %s: +%d bars", symbol, tf, len(new_df))
    return len(new_df)


def update_all(
    exchange,
    symbols: Optional[list] = None,
    data_dir: Optional[Path] = None,
) -> int:
    """Update all pairs × all timeframes. Returns total new bars."""
    if symbols is None:
        symbols = SYMBOLS
    if data_dir is None:
        data_dir = DATA_DIR

    total_new = 0
    total_symbols = len(symbols)
    for i, symbol in enumerate(symbols, 1):
        logger.info("[%d/%d] %s", i, total_symbols, symbol)
        for tf in TIMEFRAMES:
            total_new += update_symbol_tf(exchange, symbol, tf, data_dir)
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        if i < total_symbols:
            time.sleep(SLEEP_BETWEEN_PAIRS)

    logger.info("Update done — %d new bars total", total_new)
    return total_new
