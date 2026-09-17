"""
Data integrity checks on the raw price files the engines load.

These exist because of a real, previously-undiscovered bug (found while
writing this test suite on 2026-09-18): the live app runs on Yahoo Finance
data, which comes pre-adjusted for stock splits, but the loader code was
also manually re-applying split adjustments meant for raw, unadjusted
DataBento data. Doing both corrupted the price on every split date with a
fake ~2x jump, which -- because the strategy happened to be fully invested
at the time -- permanently inflated every CAGR computed from that point on
(Step 1 was overstated at ~30% vs a real ~18%; Step 2 at ~39% vs ~20%).

A price-continuity check like this would have caught it immediately instead
of it silently sitting in every reported number for an entire audit cycle.
"""

import pandas as pd
import pytest

from app.config import ensure_data_available


def _load_close_series(name: str) -> pd.Series:
    data_dir = ensure_data_available()
    df = pd.read_csv(data_dir / "databento" / "equities" / f"{name}_daily.csv", parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df["close"]


# Known split dates for the leveraged ETFs this app trades. A real split
# should already be reflected continuously in the raw price series if the
# data source is pre-adjusted (Yahoo) -- there should be no separate jump.
KNOWN_SPLIT_DATES = [
    ("UPRO", "2022-01-13"),
    ("TQQQ", "2021-01-21"),
    ("TQQQ", "2022-01-13"),
]


@pytest.mark.parametrize("symbol,split_date", KNOWN_SPLIT_DATES)
def test_no_artificial_price_jump_at_known_split_dates(symbol, split_date):
    """The engines' *final loaded* price series must be continuous across
    every known split date -- not just the raw CSV. This is what actually
    caught the double-adjustment bug: the raw CSV was fine, but the engine's
    own load_stepN_data() function corrupted it after loading.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import vam_step1_databento as step1_mod
    import vam_step2_databento as step2_mod

    step1_mod.DATA_DIR = ensure_data_available()
    step2_mod.DATA_DIR = ensure_data_available()

    if symbol == "UPRO":
        df = step1_mod.load_step1_data()
        price_col = "UPRO_Close"
    else:
        df = step2_mod.load_step2_data()
        price_col = "TQQQ_Close"

    split_ts = pd.Timestamp(split_date)
    before = df[df.index < split_ts][price_col]
    after = df[df.index >= split_ts][price_col]
    if before.empty or after.empty:
        pytest.skip(f"{symbol} split date {split_date} outside loaded data range")

    day_before_price = before.iloc[-1]
    day_of_price = after.iloc[0]
    pct_change = abs(day_of_price / day_before_price - 1) * 100

    # A normal single-day move for these leveraged ETFs is well under 30%
    # even on the most volatile days on record (2020 COVID crash). Anything
    # near 50%/66%/100% (the 2:1/3:1 split ratios) is the double-adjustment
    # bug, not a real market move.
    assert pct_change < 30, (
        f"{symbol} shows a {pct_change:.1f}% jump across its {split_date} split date "
        f"({day_before_price:.4f} -> {day_of_price:.4f}) in the engine's loaded data. "
        f"This is the signature of double split-adjustment, not a real market move."
    )


def test_spxu_split_ratio_within_bounds():
    """SPXU had a 1:4 reverse split on 2023-01-13, already baked into the
    source CSV per audit_import's own verification. Confirm it's still true
    for whatever SPXU file the live app is actually using.
    """
    close = _load_close_series("SPXU")
    split_ts = pd.Timestamp("2023-01-12")
    next_ts = pd.Timestamp("2023-01-13")
    if split_ts not in close.index or next_ts not in close.index:
        pytest.skip("SPXU split-date rows not present in this data range")
    ratio = close.loc[next_ts] / close.loc[split_ts]
    assert 0.85 < ratio < 1.15, f"SPXU split not properly adjusted: ratio={ratio:.4f}"
