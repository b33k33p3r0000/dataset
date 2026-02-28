"""Tests for data validation — gaps, duplicates, sanity checks."""

import pandas as pd
import numpy as np
import pytest

from conftest import make_ohlcv_df
from dataset.validate import (
    check_gaps,
    check_duplicates,
    check_nan,
    check_ohlcv_sanity,
    check_zero_volume,
    check_early_listing,
    validate_file,
    Issue,
    Severity,
)


class TestCheckGaps:
    def test_no_gaps_returns_empty(self):
        df = make_ohlcv_df(n_bars=100, tf_minutes=60)
        issues = check_gaps(df, tf_minutes=60)
        assert issues == []

    def test_detects_short_gap(self):
        df = make_ohlcv_df(n_bars=100, tf_minutes=60)
        df = df.drop(df.index[50:52])
        issues = check_gaps(df, tf_minutes=60)
        assert len(issues) == 1
        assert issues[0].severity == Severity.WARN

    def test_detects_long_gap(self):
        df = make_ohlcv_df(n_bars=100, tf_minutes=60)
        df = df.drop(df.index[50:55])
        issues = check_gaps(df, tf_minutes=60)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR

    def test_multiple_gaps(self):
        df = make_ohlcv_df(n_bars=100, tf_minutes=60)
        df = df.drop(df.index[20:22])
        df = df.drop(df.index[60:62])
        issues = check_gaps(df, tf_minutes=60)
        assert len(issues) == 2


class TestCheckDuplicates:
    def test_no_duplicates_returns_empty(self):
        df = make_ohlcv_df(n_bars=50)
        issues = check_duplicates(df)
        assert issues == []

    def test_detects_duplicates(self):
        df = make_ohlcv_df(n_bars=50)
        df = pd.concat([df, df.iloc[:3]])
        issues = check_duplicates(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "3" in issues[0].message


class TestCheckNan:
    def test_clean_data_returns_empty(self):
        df = make_ohlcv_df(n_bars=50)
        issues = check_nan(df)
        assert issues == []

    def test_detects_nan(self):
        df = make_ohlcv_df(n_bars=50)
        df.iloc[10, 0] = np.nan
        df.iloc[20, 3] = np.nan
        issues = check_nan(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.ERROR
        assert "2" in issues[0].message


class TestCheckOhlcvSanity:
    def test_clean_data_returns_empty(self):
        df = make_ohlcv_df(n_bars=50)
        issues = check_ohlcv_sanity(df)
        assert issues == []

    def test_detects_high_below_low(self):
        df = make_ohlcv_df(n_bars=50)
        df.iloc[10, 1] = df.iloc[10, 2] - 10
        issues = check_ohlcv_sanity(df)
        assert len(issues) >= 1
        assert any("high < low" in i.message.lower() for i in issues)

    def test_detects_negative_volume(self):
        df = make_ohlcv_df(n_bars=50)
        df.iloc[5, 4] = -100
        issues = check_ohlcv_sanity(df)
        assert any("volume" in i.message.lower() for i in issues)


class TestCheckZeroVolume:
    def test_no_zeros_returns_empty(self):
        df = make_ohlcv_df(n_bars=50)
        issues = check_zero_volume(df)
        assert issues == []

    def test_detects_zero_volume(self):
        df = make_ohlcv_df(n_bars=50)
        df.iloc[10, 4] = 0
        df.iloc[20, 4] = 0
        issues = check_zero_volume(df)
        assert len(issues) == 1
        assert issues[0].severity == Severity.INFO


class TestCheckEarlyListing:
    def test_flags_early_bars(self):
        df = make_ohlcv_df(n_bars=200, tf_minutes=60)
        issues = check_early_listing(df, early_days=7)
        assert len(issues) == 1
        assert issues[0].severity == Severity.INFO
        assert "168" in issues[0].message or "7" in issues[0].message

    def test_short_data_no_flag(self):
        df = make_ohlcv_df(n_bars=100, tf_minutes=60)
        issues = check_early_listing(df, early_days=7)
        assert len(issues) == 1


class TestValidateFile:
    def test_clean_file_no_errors(self):
        df = make_ohlcv_df(n_bars=200, tf_minutes=60)
        issues = validate_file(df, tf_minutes=60)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert errors == []

    def test_bad_file_returns_issues(self):
        df = make_ohlcv_df(n_bars=100, tf_minutes=60)
        df.iloc[10, 0] = np.nan
        df = df.drop(df.index[50:55])
        issues = validate_file(df, tf_minutes=60)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) >= 2
