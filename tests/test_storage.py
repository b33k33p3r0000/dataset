"""Tests for Parquet storage (save/load/append)."""

import pandas as pd
import pytest
from pathlib import Path

from conftest import make_ohlcv_df
from dataset.storage import save_parquet, load_parquet, append_parquet


class TestSaveAndLoad:
    def test_save_creates_file(self, tmp_path):
        df = make_ohlcv_df(n_bars=50)
        path = tmp_path / "BTCUSDT" / "1h.parquet"
        save_parquet(df, path)
        assert path.exists()

    def test_roundtrip_preserves_data(self, tmp_path):
        df = make_ohlcv_df(n_bars=50)
        path = tmp_path / "BTCUSDT" / "1h.parquet"
        save_parquet(df, path)
        loaded = load_parquet(path)
        # Parquet does not preserve DatetimeIndex freq — compare without it
        pd.testing.assert_frame_equal(df, loaded, check_freq=False)

    def test_roundtrip_preserves_utc_index(self, tmp_path):
        df = make_ohlcv_df(n_bars=10)
        path = tmp_path / "test.parquet"
        save_parquet(df, path)
        loaded = load_parquet(path)
        assert loaded.index.tz is not None
        assert str(loaded.index.tz) == "UTC"

    def test_load_nonexistent_returns_none(self, tmp_path):
        result = load_parquet(tmp_path / "nonexistent.parquet")
        assert result is None


class TestAppend:
    def test_append_adds_rows(self, tmp_path):
        df1 = make_ohlcv_df(n_bars=50, start="2025-01-01")
        df2 = make_ohlcv_df(n_bars=50, start="2025-01-03T02:00:00")
        path = tmp_path / "test.parquet"
        save_parquet(df1, path)
        append_parquet(df2, path)
        loaded = load_parquet(path)
        assert len(loaded) == 100

    def test_append_deduplicates(self, tmp_path):
        df1 = make_ohlcv_df(n_bars=50, start="2025-01-01")
        df2 = make_ohlcv_df(n_bars=30, start="2025-01-02T16:00:00")
        path = tmp_path / "test.parquet"
        save_parquet(df1, path)
        append_parquet(df2, path)
        loaded = load_parquet(path)
        assert loaded.index.is_unique

    def test_append_to_nonexistent_creates_file(self, tmp_path):
        df = make_ohlcv_df(n_bars=20)
        path = tmp_path / "new.parquet"
        append_parquet(df, path)
        assert path.exists()
        loaded = load_parquet(path)
        assert len(loaded) == 20
