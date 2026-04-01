"""
Download DataBento data for Ravi's project.

Downloads:
1. US Equities daily bars (2020-2025): SPY, QQQ, TQQQ, SOXL, TLT, GLD
   - Dataset: XNAS.ITCH, Schema: ohlcv-1d
2. NQ Futures 1-minute bars (2020-2025):
   - Dataset: GLBX.MDP3, Symbol: NQ.c.0, Schema: ohlcv-1m

Cost estimate: ~$7.67 from $125 free credits.
Permission: GRANTED by Anmol on 2026-03-20.
"""

from pathlib import Path

import databento as db
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_KEY = "db-iMWEu94gMvi9PKm67kURKM6DaWJJ8"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EQUITIES_DIR = PROJECT_ROOT / "data" / "databento" / "equities"
FUTURES_DIR = PROJECT_ROOT / "data" / "databento" / "futures"

# NOTE: databento SDK v0.73+ to_df() already converts fixed-point to float.
# No manual price scaling needed.

EQUITIES_SYMBOLS = ["SPY", "QQQ", "TQQQ", "SOXL", "TLT", "GLD"]
START = "2020-01-01"
END = "2025-12-31"


def standardize_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataBento raw DataFrame to standard OHLCV format.

    - Converts fixed-point prices (int64 * 1e-9) to float
    - Uses ts_event as datetime index if present, otherwise uses the existing index
    - Strips timezone, keeps UTC-naive
    - Returns columns: open, high, low, close, volume
    """
    if raw_df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = raw_df.copy()

    # Use ts_event as index if available, otherwise keep existing index
    if "ts_event" in df.columns:
        df["datetime"] = pd.to_datetime(df["ts_event"])
        df.set_index("datetime", inplace=True)
    else:
        df.index = pd.to_datetime(df.index)
        df.index.name = "datetime"

    # Strip timezone
    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)

    # SDK v0.73+ to_df() already returns decimal float prices — no scaling needed
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df["volume"] = df["volume"].astype(float)

    # Keep only standard columns
    df = df[["open", "high", "low", "close", "volume"]]
    df.sort_index(inplace=True)

    return df


def validate_data(df: pd.DataFrame, symbol: str, expected_freq: str = "daily"):
    """Validate data quality: no gaps > 5 trading days, prices > 0."""
    issues = []

    # Check prices > 0
    for col in ["open", "high", "low", "close"]:
        bad = (df[col] <= 0).sum()
        if bad > 0:
            issues.append(f"  {col}: {bad} rows with price <= 0")

    # Check for large gaps (> 5 calendar days for daily, > 2 days for minute)
    if len(df) > 1:
        diffs = df.index.to_series().diff().dropna()
        if expected_freq == "daily":
            max_gap_days = 5
            large_gaps = diffs[diffs > pd.Timedelta(days=max_gap_days)]
        else:
            # For minute data, gaps > 2 calendar days are suspicious
            max_gap_days = 3
            large_gaps = diffs[diffs > pd.Timedelta(days=max_gap_days)]

        if len(large_gaps) > 0:
            issues.append(f"  {len(large_gaps)} gap(s) > {max_gap_days} days:")
            for ts, gap in large_gaps.head(5).items():
                issues.append(f"    {ts}: gap of {gap}")

    if issues:
        print(f"  [!] Validation issues for {symbol}:")
        for issue in issues:
            print(issue)
    else:
        print(f"  [OK] {symbol} passed validation")


def download_equities():
    """Download US equities daily bars."""
    print("=" * 60)
    print("STEP 1: US Equities Daily Bars (XNAS.ITCH)")
    print(f"Symbols: {EQUITIES_SYMBOLS}")
    print(f"Range: {START} to {END}")
    print("=" * 60)

    EQUITIES_DIR.mkdir(parents=True, exist_ok=True)

    client = db.Historical(key=API_KEY)

    # Cost estimate first
    try:
        cost = client.metadata.get_cost(
            dataset="XNAS.ITCH",
            symbols=EQUITIES_SYMBOLS,
            schema="ohlcv-1d",
            start=START,
            end=END,
        )
        print(f"Estimated cost: ${float(cost):.2f}")
    except Exception as e:
        print(f"Cost estimate failed (proceeding anyway): {e}")

    print("\nFetching data...")
    data = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        symbols=EQUITIES_SYMBOLS,
        schema="ohlcv-1d",
        start=START,
        end=END,
    )

    raw_df = data.to_df()
    print(f"Raw rows received: {len(raw_df)}")
    print(f"Raw columns: {list(raw_df.columns)}")

    # Check if there's a symbol column to split by
    symbol_col = None
    for candidate in ["symbol", "instrument_id", "raw_symbol"]:
        if candidate in raw_df.columns:
            symbol_col = candidate
            break

    if symbol_col:
        print(f"Splitting by column: {symbol_col}")
        unique_symbols = raw_df[symbol_col].unique()
        print(f"Unique values: {unique_symbols}")
    else:
        print("No symbol column found. Checking if data has a 'symbol' attribute...")
        # DataBento DBNStore may need to_df with pretty_ts and pretty_px
        try:
            raw_df2 = data.to_df(pretty_ts=True, pretty_px=True)
            for candidate in ["symbol", "instrument_id", "raw_symbol"]:
                if candidate in raw_df2.columns:
                    symbol_col = candidate
                    raw_df = raw_df2
                    break
        except Exception:
            pass

    if symbol_col is None:
        # If still no symbol column, try iterating with replay
        print("WARNING: No symbol column found. Saving as combined file.")
        df = standardize_df(raw_df)
        out_path = EQUITIES_DIR / "all_equities_daily.csv"
        df.to_csv(out_path)
        print(
            f"Saved combined: {out_path} ({len(df)} rows, {out_path.stat().st_size / 1024:.1f} KB)"
        )
        validate_data(df, "ALL_EQUITIES", "daily")
        return

    # Split and save per symbol
    print(f"\nSaving per-symbol CSVs to {EQUITIES_DIR}/")
    summary = []

    for sym_val in sorted(raw_df[symbol_col].unique()):
        sym_df = raw_df[raw_df[symbol_col] == sym_val].copy()
        df = standardize_df(sym_df)

        # Try to get a clean ticker name
        ticker = str(sym_val).strip()
        # If it's an instrument_id (numeric), we need a mapping
        # For XNAS.ITCH the symbol column should give us the ticker directly

        out_path = EQUITIES_DIR / f"{ticker}_daily.csv"
        df.to_csv(out_path)

        file_size = out_path.stat().st_size / 1024
        date_range = f"{df.index[0]} to {df.index[-1]}" if len(df) > 0 else "EMPTY"
        summary.append(
            {
                "symbol": ticker,
                "rows": len(df),
                "date_range": date_range,
                "file_size_kb": f"{file_size:.1f}",
            }
        )

        print(f"  {ticker}: {len(df)} rows, {date_range}, {file_size:.1f} KB")
        validate_data(df, ticker, "daily")

    print(f"\nEquities download complete: {len(summary)} symbols saved")
    return summary


def download_futures():
    """Download NQ futures 1-minute bars."""
    print("\n" + "=" * 60)
    print("STEP 2: NQ Futures 1-Minute Bars (GLBX.MDP3)")
    print("Symbol: NQ.c.0 (continuous front-month)")
    print(f"Range: {START} to {END}")
    print("=" * 60)

    FUTURES_DIR.mkdir(parents=True, exist_ok=True)

    client = db.Historical(key=API_KEY)

    # Cost estimate first
    try:
        cost = client.metadata.get_cost(
            dataset="GLBX.MDP3",
            symbols=["NQ.c.0"],
            schema="ohlcv-1m",
            start=START,
            end=END,
            stype_in="continuous",
        )
        print(f"Estimated cost: ${float(cost):.2f}")
    except Exception as e:
        print(f"Cost estimate failed (proceeding anyway): {e}")

    # For 5 years of 1-minute data, download in yearly chunks to avoid timeouts
    all_dfs = []
    for year in range(2020, 2026):
        year_start = f"{year}-01-01"
        year_end = f"{year}-12-31"
        print(f"\n  Fetching {year}...")

        try:
            data = client.timeseries.get_range(
                dataset="GLBX.MDP3",
                symbols=["NQ.c.0"],
                schema="ohlcv-1m",
                stype_in="continuous",
                start=year_start,
                end=year_end,
            )
            raw_df = data.to_df()
            df = standardize_df(raw_df)
            print(f"    {year}: {len(df)} bars")
            all_dfs.append(df)
        except Exception as e:
            print(f"    {year}: FAILED — {e}")

    if not all_dfs:
        print("ERROR: No NQ data downloaded!")
        return None

    # Combine all years
    combined = pd.concat(all_dfs)
    combined.sort_index(inplace=True)
    combined = combined[~combined.index.duplicated(keep="first")]

    out_path = FUTURES_DIR / "NQ_1m.csv"
    combined.to_csv(out_path)

    file_size_mb = out_path.stat().st_size / (1024 * 1024)
    date_range = f"{combined.index[0]} to {combined.index[-1]}"

    print(f"\n  NQ combined: {len(combined)} bars")
    print(f"  Date range: {date_range}")
    print(f"  File size: {file_size_mb:.1f} MB")
    print(f"  Saved to: {out_path}")

    validate_data(combined, "NQ", "minute")

    return {
        "symbol": "NQ",
        "rows": len(combined),
        "date_range": date_range,
        "file_size_mb": f"{file_size_mb:.1f}",
    }


def main():
    print("DataBento Download Script — Ravi's Project")
    print(f"API Key: {API_KEY[:10]}...{API_KEY[-4:]}")
    print()

    # Download equities
    download_equities()

    # Download NQ futures
    download_futures()

    # Final summary
    print("\n" + "=" * 60)
    print("DOWNLOAD COMPLETE — SUMMARY")
    print("=" * 60)

    print(f"\nEquities dir: {EQUITIES_DIR}")
    if EQUITIES_DIR.exists():
        for f in sorted(EQUITIES_DIR.glob("*.csv")):
            print(f"  {f.name}: {f.stat().st_size / 1024:.1f} KB")

    print(f"\nFutures dir: {FUTURES_DIR}")
    if FUTURES_DIR.exists():
        for f in sorted(FUTURES_DIR.glob("*.csv")):
            size = f.stat().st_size / (1024 * 1024)
            print(f"  {f.name}: {size:.1f} MB")


if __name__ == "__main__":
    main()
