"""Tests for incremental daily update."""

from unittest.mock import MagicMock, patch
from pathlib import Path

import pandas as pd
import pytest

from conftest import make_ohlcv_df
from dataset.update import update_symbol_tf, update_all
from dataset.storage import save_parquet, load_parquet
from dataset.config import TF_MS


class TestUpdateSymbolTf:
    def test_appends_new_bars(self, tmp_path):
        existing = make_ohlcv_df(n_bars=100, tf_minutes=60, start="2025-01-01")
        path = tmp_path / "BTCUSDT" / "1h.parquet"
        save_parquet(existing, path)

        new_bars = make_ohlcv_df(n_bars=10, tf_minutes=60, start="2025-01-05T04:00:00", seed=99)

        with patch("dataset.update.fetch_ohlcv_paginated") as mock_fetch:
            mock_fetch.return_value = new_bars
            exchange = MagicMock()
            exchange.rateLimit = 100
            update_symbol_tf(exchange, "BTC/USDT", "1h", tmp_path)

        loaded = load_parquet(path)
        assert len(loaded) > 100

    def test_skips_if_no_existing_file(self, tmp_path):
        with patch("dataset.update.fetch_ohlcv_paginated") as mock_fetch:
            exchange = MagicMock()
            update_symbol_tf(exchange, "BTC/USDT", "1h", tmp_path)
            mock_fetch.assert_not_called()

    def test_no_new_bars_leaves_file_unchanged(self, tmp_path):
        existing = make_ohlcv_df(n_bars=50, tf_minutes=60)
        path = tmp_path / "BTCUSDT" / "1h.parquet"
        save_parquet(existing, path)

        with patch("dataset.update.fetch_ohlcv_paginated") as mock_fetch:
            mock_fetch.return_value = pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"]
            )
            exchange = MagicMock()
            exchange.rateLimit = 100
            update_symbol_tf(exchange, "BTC/USDT", "1h", tmp_path)

        loaded = load_parquet(path)
        assert len(loaded) == 50
