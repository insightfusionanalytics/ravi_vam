"""
Download UPRO and SHY daily bars from DataBento.

These are missing from the original download and needed for Ravi VAM v3 strategy.
Dataset: XNAS.ITCH, Schema: ohlcv-1d, Range: 2020-01-01 to 2025-12-31

Cost estimate: ~$0.02 (two symbols, daily bars only)
Permission: GRANTED by Anmol on 2026-03-20.
"""

import sys
from pathlib import Path

import databento as db
import pandas as pd

API_KEY = "db-iMWEu94gMvi9PKm67kURKM6DaWJJ8"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EQUITIES_DIR = PROJECT_ROOT / "data" / "databento" / "equities"

SYMBOLS = ["UPRO", "SHY"]
START = "2020-01-01"
END = "2025-12-31"


def standardize_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataBento raw DataFrame to standard OHLCV format."""
    if raw_df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = raw_df.copy()

    if "ts_event" in df.columns:
        df["datetime"] = pd.to_datetime(df["ts_event"])
        df.set_index("datetime", inplace=True)
    else:
        df.index = pd.to_datetime(df.index)
        df.index.name = "datetime"

    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df["volume"] = df["volume"].astype(float)
    df = df[["open", "high", "low", "close", "volume"]]
    df.sort_index(inplace=True)

    return df


def main():
    print("DataBento Download — UPRO & SHY for Ravi VAM v3")
    print(f"Symbols: {SYMBOLS}")
    print(f"Range: {START} to {END}")
    print()

    EQUITIES_DIR.mkdir(parents=True, exist_ok=True)

    client = db.Historical(key=API_KEY)

    # Cost estimate
    try:
        cost = client.metadata.get_cost(
            dataset="XNAS.ITCH",
            symbols=SYMBOLS,
            schema="ohlcv-1d",
            start=START,
            end=END,
        )
        print(f"Estimated cost: ${float(cost):.4f}")
    except Exception as e:
        print(f"Cost estimate failed (proceeding): {e}")

    print("\nFetching data...")
    data = client.timeseries.get_range(
        dataset="XNAS.ITCH",
        symbols=SYMBOLS,
        schema="ohlcv-1d",
        start=START,
        end=END,
    )

    raw_df = data.to_df()
    print(f"Raw rows received: {len(raw_df)}")
    print(f"Raw columns: {list(raw_df.columns)}")

    # Find symbol column
    symbol_col = None
    for candidate in ["symbol", "instrument_id", "raw_symbol"]:
        if candidate in raw_df.columns:
            symbol_col = candidate
            break

    if symbol_col is None:
        try:
            raw_df = data.to_df(pretty_ts=True, pretty_px=True)
            for candidate in ["symbol", "instrument_id", "raw_symbol"]:
                if candidate in raw_df.columns:
                    symbol_col = candidate
                    break
        except Exception:
            pass

    if symbol_col is None:
        print("ERROR: No symbol column found. Cannot split by symbol.")
        sys.exit(1)

    print(f"Symbol column: {symbol_col}")
    print(f"Unique symbols: {sorted(raw_df[symbol_col].unique())}")

    for sym_val in sorted(raw_df[symbol_col].unique()):
        sym_df = raw_df[raw_df[symbol_col] == sym_val].copy()
        df = standardize_df(sym_df)

        ticker = str(sym_val).strip()
        out_path = EQUITIES_DIR / f"{ticker}_daily.csv"
        df.to_csv(out_path)

        file_size = out_path.stat().st_size / 1024
        date_range = f"{df.index[0]} to {df.index[-1]}" if len(df) > 0 else "EMPTY"

        print(f"  {ticker}: {len(df)} rows, {date_range}, {file_size:.1f} KB")

        # Quick validation
        for col in ["open", "high", "low", "close"]:
            bad = (df[col] <= 0).sum()
            if bad > 0:
                print(f"    WARNING: {col} has {bad} rows <= 0")

    print("\nDownload complete.")


if __name__ == "__main__":
    main()
