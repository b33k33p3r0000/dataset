#!/usr/bin/env python3
"""Daily incremental update — fetch new bars + validate."""

import logging
import traceback
from datetime import datetime, timezone
from urllib.request import Request, urlopen
import json
import os

from dotenv import load_dotenv

load_dotenv()

import ccxt

from dataset.config import SYMBOLS, DATA_DIR, REPORTS_DIR, TIMEFRAMES, TF_MINUTES, symbol_to_dirname
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

def send_discord(message: str) -> None:
    """Send message to Discord webhook (best-effort, never raises)."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK", "")
    if not webhook_url:
        logger.debug("DISCORD_WEBHOOK not set, skipping notification")
        return
    try:
        payload = json.dumps({"content": message}).encode("utf-8")
        req = Request(webhook_url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("User-Agent", "dataset-bot")
        urlopen(req, timeout=10)
    except Exception as e:
        logger.warning("Discord notification failed: %s", e)


def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    try:
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
        all_warns = sum(
            1 for issues in file_issues.values()
            for i in issues
            if i.severity == Severity.WARN
        )

        if all_errors:
            logger.warning("Found %d ERROR(s) — check report", all_errors)
            send_discord(
                f"```\nDATASET UPDATE — {now}\n"
                f"{'=' * 30}\n"
                f"New bars:  {total_new:,}\n"
                f"Total:     {total_bars:,}\n"
                f"Errors:    {all_errors}\n"
                f"Warnings:  {all_warns}\n"
                f"Status:    ERRORS FOUND\n```"
            )
        else:
            logger.info("All clean — no errors found")
            send_discord(
                f"```\nDATASET UPDATE — {now}\n"
                f"{'=' * 30}\n"
                f"New bars:  {total_new:,}\n"
                f"Total:     {total_bars:,}\n"
                f"Warnings:  {all_warns}\n"
                f"Status:    OK\n```"
            )

    except Exception:
        tb = traceback.format_exc()
        logger.error("Daily update failed:\n%s", tb)
        send_discord(
            f"```\nDATASET UPDATE — {now}\n"
            f"{'=' * 30}\n"
            f"Status:    FAILED\n"
            f"{'─' * 30}\n"
            f"{tb[-500:]}\n```"
        )
        raise


if __name__ == "__main__":
    main()
