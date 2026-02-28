#!/usr/bin/env python3
"""Daily incremental update — fetch new bars + validate."""

import logging

import ccxt

from dataset.config import SYMBOLS, DATA_DIR, REPORTS_DIR, TIMEFRAMES, symbol_to_dirname
from dataset.update import update_all
from dataset.validate import validate_file, Severity
from dataset.report import generate_report, save_report
from dataset.storage import load_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

TF_MINUTES = {
    "15m": 15, "1h": 60, "4h": 240, "8h": 480,
    "12h": 720, "1d": 1440, "1w": 10080,
}


def main():
    exchange = ccxt.binance({"enableRateLimit": True})

    logger.info("Starting daily update...")
    total_new = update_all(exchange)
    logger.info("Fetched %d new bars", total_new)

    logger.info("Running validation...")
    file_issues = {}
    total_bars = 0

    for symbol in SYMBOLS:
        dirname = symbol_to_dirname(symbol)
        for tf in TIMEFRAMES:
            path = DATA_DIR / dirname / f"{tf}.parquet"
            df = load_parquet(path)
            if df is None:
                continue
            total_bars += len(df)
            key = f"{dirname}/{tf}"
            file_issues[key] = validate_file(df, tf_minutes=TF_MINUTES[tf])

    report = generate_report(file_issues, total_bars=total_bars)
    path = save_report(report, REPORTS_DIR)
    logger.info("Validation report saved: %s", path)

    all_errors = sum(
        1 for issues in file_issues.values()
        for i in issues
        if i.severity == Severity.ERROR
    )
    if all_errors:
        logger.warning("Found %d ERROR(s) — check report", all_errors)
    else:
        logger.info("All clean — no errors found")


if __name__ == "__main__":
    main()
