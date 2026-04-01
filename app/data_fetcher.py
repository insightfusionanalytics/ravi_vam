"""
Auto-download market data via yfinance when DataBento CSV files are not available.

DataBento CSVs are NOT split-adjusted; yfinance returns split-adjusted data.
We save in a format compatible with both vam_step1/step2 and backtest_ravi_v5:
  - index column named "datetime" (for vam_step1/step2 compatibility)
  - columns: open, high, low, close, volume
  - yfinance data IS already split-adjusted, so no split-adjustment needed

The backtest_ravi_v5 uses index_col=0 and accesses df["close"] — compatible.
The vam_step1/step2 use parse_dates=["datetime"] and set_index("datetime") — compatible.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Symbols needed by each engine
EQUITY_SYMBOLS = ["SPY", "QQQ", "UPRO", "TQQQ", "SHY", "GLD", "TLT"]
BACKFILL_START = "2019-01-01"  # 1 year warmup before 2020 data


def _download_symbol(symbol: str, start: str = BACKFILL_START) -> pd.DataFrame | None:
    """Download daily OHLCV from yfinance. Returns None on failure."""
    try:
        import yfinance as yf

        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start, interval="1d", auto_adjust=True)
        if df.empty:
            logger.warning("yfinance returned empty data for %s", symbol)
            return None

        # Normalize column names to lowercase
        df.columns = [c.lower() for c in df.columns]
        df.index.name = "datetime"
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df = df[["open", "high", "low", "close", "volume"]].copy()
        df = df[~df.index.duplicated(keep="first")]
        df.sort_index(inplace=True)
        return df

    except Exception as exc:
        logger.warning("Failed to download %s via yfinance: %s", symbol, exc)
        return None


def ensure_equity_data(data_dir: Path) -> bool:
    """
    Ensure all required equity CSVs exist in data_dir/databento/equities/.
    Downloads any missing files from yfinance.
    Returns True if all required symbols are available after this call.
    """
    equities_dir = data_dir / "databento" / "equities"
    equities_dir.mkdir(parents=True, exist_ok=True)

    all_ok = True
    for sym in EQUITY_SYMBOLS:
        csv_path = equities_dir / f"{sym}_daily.csv"

        # Check if file exists AND has real data (not zero-filled OneDrive placeholder)
        if csv_path.exists():
            try:
                with open(csv_path, "rb") as f:
                    sample = f.read(200)
                if any(b != 0 for b in sample):
                    continue  # file is valid, skip download
                else:
                    logger.info("%s is a zero-filled placeholder, re-downloading.", csv_path)
            except OSError:
                pass

        logger.info("Downloading %s from yfinance...", sym)
        df = _download_symbol(sym)
        if df is None:
            logger.error("Could not obtain data for %s", sym)
            all_ok = False
            continue

        df.to_csv(csv_path, index_label="datetime")
        logger.info("Saved %s (%d rows) → %s", sym, len(df), csv_path)

    return all_ok


def ensure_vix_data(data_dir: Path) -> bool:
    """
    Ensure VIX daily CSV exists in data_dir/cboe/VIX_daily.csv.
    Downloads ^VIX from yfinance as fallback.
    """
    cboe_dir = data_dir / "cboe"
    cboe_dir.mkdir(parents=True, exist_ok=True)
    vix_path = cboe_dir / "VIX_daily.csv"

    if vix_path.exists():
        try:
            with open(vix_path, "rb") as f:
                sample = f.read(200)
            if any(b != 0 for b in sample):
                return True
        except OSError:
            pass

    logger.info("Downloading VIX (^VIX) from yfinance...")
    df = _download_symbol("^VIX")
    if df is None:
        return False

    df.to_csv(vix_path, index_label="datetime")
    logger.info("Saved VIX (%d rows) → %s", len(df), vix_path)
    return True
