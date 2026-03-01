"""Data validation — gap detection, sanity checks, integrity verification."""

from dataclasses import dataclass
from enum import Enum
from typing import List

import pandas as pd

from datetime import datetime, timezone

from dataset.config import SHORT_GAP_THRESHOLD, EARLY_LISTING_DAYS, STALE_THRESHOLD_DAYS, KNOWN_GAPS_BEFORE


class Severity(Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


@dataclass
class Issue:
    severity: Severity
    message: str


def check_gaps(df: pd.DataFrame, tf_minutes: int) -> List[Issue]:
    """Detect missing bars based on expected frequency.

    Gaps before KNOWN_GAPS_BEFORE are downgraded to INFO (historical
    Binance maintenance windows that will never be filled).
    """
    if len(df) < 2:
        return []
    issues: List[Issue] = []
    expected_delta = pd.Timedelta(minutes=tf_minutes)
    known_cutoff = pd.Timestamp(KNOWN_GAPS_BEFORE, tz="UTC")
    deltas = df.index.to_series().diff().dropna()
    gap_mask = deltas > expected_delta

    if not gap_mask.any():
        return []

    gap_starts = deltas[gap_mask]
    for ts, delta in gap_starts.items():
        n_missing = int(delta / expected_delta) - 1
        if ts < known_cutoff:
            sev = Severity.INFO
        elif n_missing <= SHORT_GAP_THRESHOLD:
            sev = Severity.WARN
        else:
            sev = Severity.ERROR
        issues.append(Issue(
            severity=sev,
            message=f"Gap: {n_missing} missing bar(s) before {ts}",
        ))
    return issues


def check_duplicates(df: pd.DataFrame) -> List[Issue]:
    """Detect duplicate timestamps."""
    n_dupes = df.index.duplicated().sum()
    if n_dupes == 0:
        return []
    return [Issue(Severity.ERROR, f"Duplicates: {n_dupes} duplicate timestamp(s)")]


def check_nan(df: pd.DataFrame) -> List[Issue]:
    """Detect NaN/null values in OHLCV columns."""
    n_nan = df[["open", "high", "low", "close", "volume"]].isna().sum().sum()
    if n_nan == 0:
        return []
    return [Issue(Severity.ERROR, f"NaN: {n_nan} missing value(s)")]


def check_ohlcv_sanity(df: pd.DataFrame) -> List[Issue]:
    """Detect impossible OHLCV values."""
    issues: List[Issue] = []
    bad_hl = (df["high"] < df["low"]).sum()
    if bad_hl:
        issues.append(Issue(Severity.ERROR, f"OHLCV: {bad_hl} bar(s) with high < low"))
    neg_vol = (df["volume"] < 0).sum()
    if neg_vol:
        issues.append(Issue(Severity.ERROR, f"OHLCV: {neg_vol} bar(s) with negative volume"))
    return issues


def check_zero_volume(df: pd.DataFrame) -> List[Issue]:
    """Detect bars with zero volume."""
    n_zero = (df["volume"] == 0).sum()
    if n_zero == 0:
        return []
    return [Issue(Severity.INFO, f"Zero volume: {n_zero} bar(s)")]


def check_early_listing(df: pd.DataFrame, early_days: int = EARLY_LISTING_DAYS) -> List[Issue]:
    """Flag bars in the first N days after listing."""
    if df.empty:
        return []
    first_ts = df.index[0]
    cutoff = first_ts + pd.Timedelta(days=early_days)
    n_early = (df.index < cutoff).sum()
    if n_early == 0:
        return []
    return [Issue(
        Severity.INFO,
        f"Early listing: {n_early} bar(s) in first {early_days} days (from {first_ts.date()})",
    )]


def check_stale(df: pd.DataFrame, stale_days: int = STALE_THRESHOLD_DAYS) -> List[Issue]:
    """Warn if last bar is older than N days."""
    if df.empty:
        return []
    last_ts = df.index[-1]
    now = pd.Timestamp.now(tz="UTC")
    age = now - last_ts
    if age.days >= stale_days:
        return [Issue(
            Severity.WARN,
            f"Stale data: last bar {last_ts.date()} is {age.days} days old",
        )]
    return []


def validate_file(
    df: pd.DataFrame,
    tf_minutes: int,
    early_days: int = EARLY_LISTING_DAYS,
) -> List[Issue]:
    """Run all validation checks on a DataFrame."""
    issues: List[Issue] = []
    issues.extend(check_gaps(df, tf_minutes))
    issues.extend(check_duplicates(df))
    issues.extend(check_nan(df))
    issues.extend(check_ohlcv_sanity(df))
    issues.extend(check_zero_volume(df))
    issues.extend(check_early_listing(df, early_days))
    issues.extend(check_stale(df))
    return issues
