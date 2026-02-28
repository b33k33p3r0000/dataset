"""Tests for Binance OHLCV fetching."""

import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dataset.fetch import fetch_ohlcv_paginated, utcnow_ms
from dataset.config import TF_MS, OHLCV_LIMIT_PER_CALL


class TestUtcnowMs:
    def test_returns_int(self):
        result = utcnow_ms()
        assert isinstance(result, int)

    def test_returns_reasonable_value(self):
        assert utcnow_ms() > 1_735_689_600_000


class TestFetchOhlcvPaginated:
    def _make_exchange(self, pages):
        exchange = MagicMock()
        exchange.rateLimit = 100
        exchange.fetch_ohlcv = MagicMock(side_effect=pages)
        return exchange

    def _make_bars(self, start_ms, tf_ms, count):
        return [
            [start_ms + i * tf_ms, 100.0, 105.0, 95.0, 102.0, 1000.0]
            for i in range(count)
        ]

    def test_single_page_fetch(self):
        tf_ms = TF_MS["1h"]
        bars = self._make_bars(0, tf_ms, 100)
        exchange = self._make_exchange([bars, []])
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", 0, 100 * tf_ms)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100
        assert list(df.columns) == ["open", "high", "low", "close", "volume"]

    def test_multi_page_fetch(self):
        tf_ms = TF_MS["1h"]
        page1 = self._make_bars(0, tf_ms, 1500)
        page2 = self._make_bars(1500 * tf_ms, tf_ms, 500)
        exchange = self._make_exchange([page1, page2, []])
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", 0, 2000 * tf_ms)
        assert len(df) == 2000

    def test_returns_utc_datetimeindex(self):
        tf_ms = TF_MS["1h"]
        bars = self._make_bars(0, tf_ms, 10)
        exchange = self._make_exchange([bars, []])
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", 0, 10 * tf_ms)
        assert df.index.tz is not None
        assert str(df.index.tz) == "UTC"

    def test_empty_response_returns_empty_df(self):
        exchange = self._make_exchange([[]])
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", 0, 1000)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_retries_on_exception(self):
        tf_ms = TF_MS["1h"]
        bars = self._make_bars(0, tf_ms, 10)
        exchange = MagicMock()
        exchange.rateLimit = 100
        exchange.fetch_ohlcv = MagicMock(
            side_effect=[Exception("timeout"), bars, []]
        )
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", 0, 10 * tf_ms)
        assert len(df) == 10

    def test_respects_sleep_between_pages(self):
        tf_ms = TF_MS["1h"]
        page1 = self._make_bars(0, tf_ms, 1500)
        page2 = self._make_bars(1500 * tf_ms, tf_ms, 100)
        exchange = self._make_exchange([page1, page2, []])
        with patch("dataset.fetch.time.sleep") as mock_sleep:
            fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", 0, 1600 * tf_ms)
            assert mock_sleep.called

    def test_deduplicates_rows(self):
        tf_ms = TF_MS["1h"]
        page1 = self._make_bars(0, tf_ms, 10)
        page2 = self._make_bars(9 * tf_ms, tf_ms, 5)
        exchange = self._make_exchange([page1, page2, []])
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", 0, 14 * tf_ms)
        assert df.index.is_unique
