# Dataset — Centralized OHLCV Store

Centralized local OHLCV dataset for crypto backtesting. Fetches historical and daily data from Binance, stores as Parquet files, validates data integrity.

## Pairs (15)

BTC, ETH, SOL, XRP, BNB, LINK, SUI, DOT, ADA, NEAR, LTC, APT, ARB, OP, INJ (USDT perpetuals)

## Timeframes

15m, 1h, 4h, 8h, 12h, 1d, 1w

## Data Format

One Parquet file per pair/timeframe:

```
data/
  BTCUSDT/
    1h.parquet
    4h.parquet
    ...
  ETHUSDT/
    ...
```

## Stack

Python, ccxt, pandas, pyarrow

## Setup

```bash
uv sync
cp .env.example .env
# Fill in DISCORD_WEBHOOK in .env (optional, for update notifications)
```

## Usage

```bash
# Initial fetch (all pairs, all timeframes, full history)
python fetch_all.py

# Daily incremental update + validation
python update_daily.py

# Standalone validation
python -m dataset.validate
```

## Daily Auto-Update

Uses macOS launchd. Copy and customize the plist template:

```bash
cp com.dataset.daily-update.plist.example com.dataset.daily-update.plist
# Edit paths in .plist to match your local setup
launchctl load com.dataset.daily-update.plist
```

## Validation

8 checks per file: gaps, duplicates, NaN, OHLCV sanity, zero volume, early listing detection, stale data, continuity. Reports saved to `reports/`.
