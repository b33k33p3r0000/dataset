#!/usr/bin/env python3
"""Initial full download — fetch all pairs × all timeframes."""

import logging
import sys

import ccxt

from dataset.config import SYMBOLS, DATA_DIR
from dataset.fetch import fetch_all_pairs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    symbols = SYMBOLS
    if len(sys.argv) > 1:
        symbols = [s.upper() for s in sys.argv[1:]]
        # Normalize: BTCUSDT → BTC/USDT
        symbols = [f"{s[:-4]}/{s[-4:]}" if "/" not in s else s for s in symbols]
        logger.info("Custom symbols: %s", symbols)

    logger.info("Fetching %d pairs × 7 timeframes", len(symbols))
    exchange = ccxt.binance({"enableRateLimit": True})
    fetch_all_pairs(exchange, symbols=symbols, data_dir=DATA_DIR)
    logger.info("All done.")


if __name__ == "__main__":
    main()
