"""Binance OHLCV fetching via ccxt — paginated with rate limiting."""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from dataset.config import (
    OHLCV_LIMIT_PER_CALL,
    MAX_API_RETRIES,
    SLEEP_BETWEEN_REQUESTS,
    SLEEP_BETWEEN_PAIRS,
    SAFETY_MAX_ROWS,
    TF_MS,
    TIMEFRAMES,
    SYMBOLS,
    MAX_HISTORY_YEARS,
    symbol_to_dirname,
)
from dataset.storage import save_parquet

logger = logging.getLogger(__name__)


def utcnow_ms() -> int:
    """Current UTC time in milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def fetch_ohlcv_paginated(
    exchange,
    symbol: str,
    tf: str,
    since_ms: int,
    until_ms: int,
) -> pd.DataFrame:
    """Fetch OHLCV data in paginated batches.

    Returns DataFrame with DatetimeIndex (UTC) and columns:
    open, high, low, close, volume.
    """
    tf_ms = TF_MS[tf]
    all_rows: list = []
    cursor = since_ms
    retries = 0

    while cursor < until_ms:
        try:
            batch = exchange.fetch_ohlcv(
                symbol, timeframe=tf, since=cursor, limit=OHLCV_LIMIT_PER_CALL
            )
            if not batch:
                break

            all_rows.extend(batch)
            last_ts = batch[-1][0]

            if last_ts <= cursor:
                cursor += tf_ms
            else:
                cursor = last_ts + tf_ms

            retries = 0
            time.sleep(SLEEP_BETWEEN_REQUESTS)

            if len(all_rows) >= SAFETY_MAX_ROWS:
                logger.warning(
                    "%s %s: hit safety limit %d rows", symbol, tf, SAFETY_MAX_ROWS
                )
                break

        except Exception as e:
            retries += 1
            if retries > MAX_API_RETRIES:
                logger.error(
                    "%s %s: exhausted %d retries, last error: %s",
                    symbol, tf, MAX_API_RETRIES, e,
                )
                break
            wait = 2 ** (retries - 1)
            logger.warning(
                "%s %s: retry %d/%d after %.1fs — %s",
                symbol, tf, retries, MAX_API_RETRIES, wait, e,
            )
            time.sleep(wait)

    if not all_rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    df = df[~df.index.duplicated(keep="first")]
    df.sort_index(inplace=True)
    return df


def detect_listing_date(exchange, symbol: str) -> Optional[int]:
    """Detect the listing date of a perpetual contract.

    Fetches the earliest available 1d bar. Returns timestamp in ms,
    capped at MAX_HISTORY_YEARS ago. Returns None if no data.
    """
    now_ms = utcnow_ms()
    cap_ms = now_ms - MAX_HISTORY_YEARS * 365 * 24 * 3600 * 1000

    try:
        bars = exchange.fetch_ohlcv(symbol, timeframe="1d", since=0, limit=1)
    except Exception as e:
        logger.error("%s: failed to detect listing date — %s", symbol, e)
        return None

    if not bars:
        logger.warning("%s: no data available", symbol)
        return None

    listing_ms = bars[0][0]
    return max(listing_ms, cap_ms)


def fetch_symbol_all_tfs(
    exchange,
    symbol: str,
    data_dir: Path,
) -> None:
    """Fetch all timeframes for one symbol and save as Parquet."""
    since_ms = detect_listing_date(exchange, symbol)
    if since_ms is None:
        logger.warning("Skipping %s — no listing date", symbol)
        return

    until_ms = utcnow_ms()
    dirname = symbol_to_dirname(symbol)
    symbol_dir = data_dir / dirname

    for tf in TIMEFRAMES:
        logger.info("  %s %s: fetching...", symbol, tf)
        df = fetch_ohlcv_paginated(exchange, symbol, tf, since_ms, until_ms)
        if df.empty:
            logger.warning("  %s %s: no data returned", symbol, tf)
            continue
        path = symbol_dir / f"{tf}.parquet"
        save_parquet(df, path)
        logger.info("  %s %s: %d bars saved", symbol, tf, len(df))


def fetch_all_pairs(
    exchange,
    symbols: Optional[list] = None,
    data_dir: Optional[Path] = None,
) -> None:
    """Fetch all pairs x all timeframes. Full initial download."""
    if symbols is None:
        symbols = SYMBOLS
    if data_dir is None:
        from dataset.config import DATA_DIR
        data_dir = DATA_DIR

    total = len(symbols)
    for i, symbol in enumerate(symbols, 1):
        logger.info("[%d/%d] %s", i, total, symbol)
        fetch_symbol_all_tfs(exchange, symbol, data_dir)
        if i < total:
            time.sleep(SLEEP_BETWEEN_PAIRS)

    logger.info("Done — %d pairs fetched", total)
