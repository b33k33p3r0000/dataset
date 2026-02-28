"""Tests for fetch orchestration — listing detection, multi-TF, multi-pair."""

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import pandas as pd
import pytest

from dataset.fetch import detect_listing_date, fetch_symbol_all_tfs, fetch_all_pairs
from dataset.config import TIMEFRAMES, TF_MS, SYMBOLS, MAX_HISTORY_YEARS


class TestDetectListingDate:
    def test_returns_first_bar_timestamp(self):
        exchange = MagicMock()
        first_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        exchange.fetch_ohlcv.return_value = [
            [first_ts, 100, 105, 95, 102, 1000]
        ]
        result = detect_listing_date(exchange, "BTC/USDT")
        assert result == first_ts

    def test_caps_at_max_history(self):
        exchange = MagicMock()
        old_ts = int(datetime(2015, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        exchange.fetch_ohlcv.return_value = [
            [old_ts, 100, 105, 95, 102, 1000]
        ]
        result = detect_listing_date(exchange, "BTC/USDT")
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        ten_years_ms = MAX_HISTORY_YEARS * 365 * 24 * 3600 * 1000
        assert result >= now_ms - ten_years_ms

    def test_returns_none_on_no_data(self):
        exchange = MagicMock()
        exchange.fetch_ohlcv.return_value = []
        result = detect_listing_date(exchange, "FAKE/USDT")
        assert result is None


class TestFetchSymbolAllTfs:
    def test_fetches_all_timeframes(self, tmp_path):
        with patch("dataset.fetch.detect_listing_date") as mock_detect:
            mock_detect.return_value = 0
            with patch("dataset.fetch.fetch_ohlcv_paginated") as mock_fetch:
                mock_fetch.return_value = pd.DataFrame(
                    {"open": [1], "high": [2], "low": [0.5], "close": [1.5], "volume": [100]},
                    index=pd.DatetimeIndex(["2025-01-01"], tz="UTC"),
                )
                exchange = MagicMock()
                exchange.rateLimit = 100
                fetch_symbol_all_tfs(exchange, "BTC/USDT", tmp_path)

        assert mock_fetch.call_count == len(TIMEFRAMES)

    def test_saves_parquet_files(self, tmp_path):
        with patch("dataset.fetch.detect_listing_date") as mock_detect:
            mock_detect.return_value = 0
            with patch("dataset.fetch.fetch_ohlcv_paginated") as mock_fetch:
                mock_fetch.return_value = pd.DataFrame(
                    {"open": [1], "high": [2], "low": [0.5], "close": [1.5], "volume": [100]},
                    index=pd.DatetimeIndex(["2025-01-01"], tz="UTC"),
                )
                exchange = MagicMock()
                exchange.rateLimit = 100
                fetch_symbol_all_tfs(exchange, "BTC/USDT", tmp_path)

        parquet_files = list((tmp_path / "BTCUSDT").glob("*.parquet"))
        assert len(parquet_files) == len(TIMEFRAMES)

    def test_skips_symbol_if_no_listing_date(self, tmp_path):
        with patch("dataset.fetch.detect_listing_date") as mock_detect:
            mock_detect.return_value = None
            with patch("dataset.fetch.fetch_ohlcv_paginated") as mock_fetch:
                exchange = MagicMock()
                fetch_symbol_all_tfs(exchange, "FAKE/USDT", tmp_path)
                mock_fetch.assert_not_called()
