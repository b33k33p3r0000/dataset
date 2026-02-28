"""Binance OHLCV fetching via ccxt — paginated with rate limiting."""

import logging
import time
from datetime import datetime, timezone

import pandas as pd

from dataset.config import (
    OHLCV_LIMIT_PER_CALL,
    MAX_API_RETRIES,
    SLEEP_BETWEEN_REQUESTS,
    SAFETY_MAX_ROWS,
    TF_MS,
)

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
