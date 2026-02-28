"""Integration test — real Binance API call (small window).

Run with: pytest tests/test_integration.py -v -m integration
"""

import pytest
import ccxt

from dataset.fetch import fetch_ohlcv_paginated, detect_listing_date, utcnow_ms
from dataset.storage import save_parquet, load_parquet
from dataset.validate import validate_file, Severity
from dataset.config import TF_MS


pytestmark = pytest.mark.integration


class TestRealFetch:
    @pytest.fixture
    def exchange(self):
        return ccxt.binance({"enableRateLimit": True})

    def test_fetch_btc_1h_small_window(self, exchange):
        """Fetch 48h of BTC/USDT 1h data from Binance."""
        now = utcnow_ms()
        since = now - 48 * TF_MS["1h"]
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", since, now)
        assert len(df) >= 40
        assert len(df) <= 50
        assert list(df.columns) == ["open", "high", "low", "close", "volume"]
        assert df.index.tz is not None

    def test_detect_btc_listing_date(self, exchange):
        """BTC/USDT perp listing should be around 2019-2020."""
        listing = detect_listing_date(exchange, "BTC/USDT")
        assert listing is not None
        assert listing < utcnow_ms()

    def test_full_pipeline_one_pair_one_tf(self, exchange, tmp_path):
        """Full pipeline: fetch -> save -> load -> validate."""
        now = utcnow_ms()
        since = now - 24 * TF_MS["1h"]
        df = fetch_ohlcv_paginated(exchange, "BTC/USDT", "1h", since, now)

        path = tmp_path / "BTCUSDT" / "1h.parquet"
        save_parquet(df, path)

        loaded = load_parquet(path)
        assert len(loaded) == len(df)

        issues = validate_file(loaded, tf_minutes=60)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert errors == [], f"Unexpected errors: {errors}"
