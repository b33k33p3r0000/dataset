"""Dataset configuration — pairs, timeframes, constants."""

from typing import Dict, List

# ── Symbols ──────────────────────────────────────────────
# 15 USDT perpetual pairs (MQE pair list, tier-ordered)
SYMBOLS: List[str] = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT",
    "XRP/USDT", "BNB/USDT", "LINK/USDT",
    "SUI/USDT", "AVAX/USDT", "ADA/USDT",
    "NEAR/USDT", "LTC/USDT", "APT/USDT",
    "ARB/USDT", "OP/USDT", "INJ/USDT",
]

# ── Timeframes ───────────────────────────────────────────
TIMEFRAMES: List[str] = ["15m", "1h", "4h", "8h", "12h", "1d", "1w"]

TF_MS: Dict[str, int] = {
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}

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
