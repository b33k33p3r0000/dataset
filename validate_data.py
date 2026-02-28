#!/usr/bin/env python3
"""Standalone validation — run all checks and generate report."""

import logging
import sys

from dataset.config import SYMBOLS, DATA_DIR, REPORTS_DIR, TIMEFRAMES, symbol_to_dirname
from dataset.storage import load_parquet
from dataset.validate import validate_file, Severity
from dataset.report import generate_report, save_report

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
    logger.info("Validating dataset...")
    file_issues = {}
    total_bars = 0
    files_found = 0

    for symbol in SYMBOLS:
        dirname = symbol_to_dirname(symbol)
        for tf in TIMEFRAMES:
            path = DATA_DIR / dirname / f"{tf}.parquet"
            df = load_parquet(path)
            if df is None:
                logger.warning("Missing: %s/%s", dirname, tf)
                continue
            files_found += 1
            total_bars += len(df)
            key = f"{dirname}/{tf}"
            issues = validate_file(df, tf_minutes=TF_MINUTES[tf])
            file_issues[key] = issues

            for issue in issues:
                if issue.severity == Severity.ERROR:
                    logger.error("  %s: %s", key, issue.message)
                elif issue.severity == Severity.WARN:
                    logger.warning("  %s: %s", key, issue.message)

    report = generate_report(file_issues, total_bars=total_bars)
    path = save_report(report, REPORTS_DIR)

    all_errors = sum(
        1 for issues in file_issues.values()
        for i in issues
        if i.severity == Severity.ERROR
    )

    logger.info("Files validated: %d, Total bars: %d", files_found, total_bars)
    logger.info("Report saved: %s", path)

    if all_errors:
        logger.error("FAILED — %d error(s) found", all_errors)
        sys.exit(1)
    else:
        logger.info("PASSED — no errors")


if __name__ == "__main__":
    main()
