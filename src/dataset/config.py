"""Dataset configuration — pairs, timeframes, constants."""

from typing import Dict, List

# ── Symbols ──────────────────────────────────────────────
# 15 USDT perpetual pairs (MQE pair list, tier-ordered)
SYMBOLS: List[str] = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT",
    "XRP/USDT", "BNB/USDT", "LINK/USDT",
    "SUI/USDT", "DOT/USDT", "ADA/USDT",
    "NEAR/USDT", "LTC/USDT", "APT/USDT",
    "ARB/USDT", "OP/USDT", "INJ/USDT",
]

# ── Timeframes ───────────────────────────────────────────
TIMEFRAMES: List[str] = ["15m", "1h", "4h", "8h", "12h", "1d", "1w"]

TF_MINUTES: Dict[str, int] = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "1w": 10080,
}

TF_MS: Dict[str, int] = {tf: m * 60_000 for tf, m in TF_MINUTES.items()}

# ── Fetch constants ──────────────────────────────────────
OHLCV_LIMIT_PER_CALL = 1500        # Max bars per API request
MAX_API_RETRIES = 3                 # Retry attempts per request
SLEEP_BETWEEN_REQUESTS = 0.3       # Seconds between API calls
SLEEP_BETWEEN_PAIRS = 2.0          # Seconds between pairs
SAFETY_MAX_ROWS = 500_000          # Abort if single TF exceeds this
MAX_HISTORY_YEARS = 10             # Cap history at 10 years

# ── Validation constants ─────────────────────────────────
EARLY_LISTING_DAYS = 7             # Flag first N days after listing
SHORT_GAP_THRESHOLD = 3            # Gaps <= N bars = WARN, > N = ERROR
STALE_THRESHOLD_DAYS = 5           # WARN if last bar older than N days
KNOWN_GAPS_BEFORE = "2024-01-01"   # Gaps before this date = INFO (historical Binance maintenance)

# ── Paths ────────────────────────────────────────────────
import pathlib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
LOGS_DIR = PROJECT_ROOT / "logs"

# ── Helpers ──────────────────────────────────────────────

def symbol_to_dirname(symbol: str) -> str:
    """Convert 'BTC/USDT' → 'BTCUSDT' for filesystem."""
    return symbol.replace("/", "")
