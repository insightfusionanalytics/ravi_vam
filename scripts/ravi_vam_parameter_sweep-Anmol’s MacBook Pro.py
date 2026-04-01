#!/usr/bin/env python3
"""
Ravi VAM Parameter Sweep — 384 Variations on DataBento Data
============================================================
Sweeps the 7-state regime rotation strategy across parameter combinations:
  - SMA periods: (50,200), (100,200)
  - RSI entry threshold: 35, 40, 45, 50
  - UPRO/TQQQ split: (75,25), (100,0)
  - Defensive instrument: SHY, TLT, CASH
  - SMA confirmation days: 0, 2
  - BULL_CAUTION enabled: True, False
  - VIX kill switch: False, 35

Total combinations: 2 * 4 * 2 * 3 * 2 * 2 * 2 = 384

Signal source: SPY (SMA, RSI)
Instruments: UPRO, TQQQ, SHY, TLT (daily close-to-close returns)
Commission: 0.10% + Slippage: 0.05% per transition (applied to turnover)
Initial capital: $100,000

Output:
  results/vam_parameter_sweep.csv          — all 192 rows
  results/vam_parameter_sweep_top20.json   — top 20 by Sharpe
"""

import itertools
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent  # clients/ravi_vam/
ENGINE_DIR = PROJECT_DIR.parent.parent  # _engine/code/
DATA_DIR = ENGINE_DIR / "data" / "databento" / "equities"
RESULTS_DIR = PROJECT_DIR / "results"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
INITIAL_CAPITAL = 100_000.0
COMMISSION_PCT = 0.0010  # 0.10%
SLIPPAGE_PCT = 0.0005  # 0.05%


# ============================================================
# Data Loading
# ============================================================
def load_all_data() -> dict[str, pd.DataFrame]:
    """Load SPY, UPRO, TQQQ, SHY, TLT, VIX daily CSVs from DataBento."""
    dfs = {}
    required = ["SPY", "UPRO", "TQQQ", "SHY", "TLT"]
    optional = ["VIX"]
    for sym in required:
        path = DATA_DIR / f"{sym}_daily.csv"
        if not path.exists():
            print(f"ERROR: Missing data file: {path}")
            sys.exit(1)
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.sort_index(inplace=True)
        dfs[sym] = df
    for sym in optional:
        path = DATA_DIR / f"{sym}_daily.csv"
        if path.exists():
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            df.sort_index(inplace=True)
            dfs[sym] = df
            print(f"  VIX data loaded: {len(df)} bars")
        else:
            print("  VIX data not found — vix_kill variations will be skipped")
    return dfs


def align_dates(dfs: dict[str, pd.DataFrame]) -> pd.DatetimeIndex:
    """Find common trading dates across all loaded instruments."""
    common = dfs["SPY"].index
    for sym in ["UPRO", "TQQQ", "SHY", "TLT"]:
        common = common.intersection(dfs[sym].index)
    return common.sort_values()


# ============================================================
# Indicator Computation (vectorized, computed once per SMA pair)
# ============================================================
def compute_spy_indicators(spy_close: pd.Series, sma_fast: int, sma_slow: int) -> pd.DataFrame:
    """Compute SMA fast, SMA slow, RSI(14) on SPY close prices.

    Returns DataFrame with columns: sma_fast, sma_slow, rsi_14
    """
    df = pd.DataFrame({"close": spy_close})

    df["sma_fast"] = spy_close.rolling(sma_fast).mean()
    df["sma_slow"] = spy_close.rolling(sma_slow).mean()

    # RSI — Wilder smoothing (alpha = 1/14)
    delta = spy_close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - (100 / (1 + rs))

    return df


# ============================================================
# Regime Classification
# ============================================================
def classify_regimes(
    indicators: pd.DataFrame,
    rsi_entry: float,
    rsi_caution_lo: float,
    bull_caution_enabled: bool,
    confirm_days: int,
) -> pd.Series:
    """Vectorized regime classification for the full series.

    States (priority order):
      CRASH        — RSI < 20
      NEUTRAL      — |SMA_fast - SMA_slow| / SMA_slow < 0.5%
      MOMENTUM     — SMA_fast > SMA_slow AND RSI > 65
      BULL_CAUTION — SMA_fast > SMA_slow AND rsi_caution_lo <= RSI <= rsi_entry (if enabled)
      BULL_FULL    — SMA_fast > SMA_slow AND RSI > rsi_entry
      RECOVERY     — SMA_fast < SMA_slow AND RSI > 50 AND RSI was < 30 within last 5 bars
      BEAR_HEDGE   — SMA_fast < SMA_slow (default bear)

    confirm_days: if > 0, require SMA crossover to persist for N consecutive days
                  before switching from bear to bull (or bull to bear).
    """
    n = len(indicators)
    sma_f = indicators["sma_fast"].values
    sma_s = indicators["sma_slow"].values
    rsi = indicators["rsi_14"].values

    regimes = np.full(n, "WARMUP", dtype=object)

    # Pre-compute SMA confirmation: rolling count of consecutive days sma_f > sma_s
    bull_raw = sma_f > sma_s  # True if bull signal on that day

    if confirm_days > 0:
        # Build confirmed bull signal: bull_raw must be True for confirm_days consecutive days
        bull_confirmed = np.zeros(n, dtype=bool)
        consec = 0
        for i in range(n):
            if np.isnan(sma_f[i]) or np.isnan(sma_s[i]):
                consec = 0
                continue
            if bull_raw[i]:
                consec += 1
            else:
                consec = 0
            if consec >= confirm_days:
                bull_confirmed[i] = True

        # Similarly for bear confirmation
        bear_raw = sma_f < sma_s
        bear_confirmed = np.zeros(n, dtype=bool)
        consec = 0
        for i in range(n):
            if np.isnan(sma_f[i]) or np.isnan(sma_s[i]):
                consec = 0
                continue
            if bear_raw[i]:
                consec += 1
            else:
                consec = 0
            if consec >= confirm_days:
                bear_confirmed[i] = True
    else:
        bull_confirmed = bull_raw
        bear_confirmed = ~bull_raw  # simple inverse when no confirmation

    # Classify each bar
    for i in range(n):
        if np.isnan(sma_f[i]) or np.isnan(sma_s[i]) or np.isnan(rsi[i]):
            regimes[i] = "WARMUP"
            continue

        # 1. CRASH — highest priority
        if rsi[i] < 20:
            regimes[i] = "CRASH"
            continue

        # 2. NEUTRAL — SMA convergence
        if abs(sma_f[i] - sma_s[i]) / sma_s[i] < 0.005:
            regimes[i] = "NEUTRAL"
            continue

        # 3. Bull regimes
        if bull_confirmed[i]:
            if rsi[i] > 65:
                regimes[i] = "MOMENTUM"
            elif bull_caution_enabled and rsi_caution_lo <= rsi[i] <= rsi_entry:
                regimes[i] = "BULL_CAUTION"
            elif rsi[i] > rsi_entry:
                regimes[i] = "BULL_FULL"
            else:
                # RSI below caution zone in bull — treat as caution if enabled, else bear hedge
                if bull_caution_enabled:
                    regimes[i] = "BULL_CAUTION"
                else:
                    regimes[i] = "BEAR_HEDGE"
            continue

        # 4. Bear regimes
        if bear_confirmed[i]:
            # RECOVERY: RSI > 50 and RSI was < 30 within last 5 bars
            if rsi[i] > 50:
                lookback_start = max(0, i - 5)
                if np.any(rsi[lookback_start:i] < 30):
                    regimes[i] = "RECOVERY"
                    continue
            regimes[i] = "BEAR_HEDGE"
            continue

        # 5. In the gap between confirmed signals (during confirmation delay),
        #    default to previous regime or BEAR_HEDGE
        regimes[i] = "BEAR_HEDGE"

    return pd.Series(regimes, index=indicators.index)


# ============================================================
# Backtest Engine
# ============================================================
def run_backtest(
    regimes: pd.Series,
    returns: dict[str, pd.Series],
    upro_pct: float,
    tqqq_pct: float,
    defensive: str,
    bull_caution_enabled: bool,
    vix_kill: float | bool = False,
    vix_close: pd.Series | None = None,
) -> dict:
    """Run portfolio rotation backtest given pre-computed regimes.

    Args:
        regimes: Series of regime labels aligned to common dates.
        returns: Dict of daily returns Series for UPRO, TQQQ, SHY, TLT.
        upro_pct: UPRO weight in BULL_FULL (0-100).
        tqqq_pct: TQQQ weight in BULL_FULL (0-100).
        defensive: 'SHY', 'TLT', or 'CASH'.
        bull_caution_enabled: whether BULL_CAUTION state is used.
        vix_kill: False (disabled) or a threshold number (e.g. 35).
        vix_close: VIX close price Series aligned to common dates.

    Returns:
        Dict with performance metrics.
    """
    upro_w = upro_pct / 100.0
    tqqq_w = tqqq_pct / 100.0

    # Build allocation map for this parameter set
    allocations = {
        "WARMUP": {"CASH": 1.0},
        "CRASH": {"CASH": 1.0},
        "NEUTRAL": {defensive: 1.0} if defensive != "CASH" else {"CASH": 1.0},
        "BULL_FULL": {},
        "MOMENTUM": {"UPRO": 1.0},
        "RECOVERY": {"UPRO": 0.25},
        "BEAR_HEDGE": {defensive: 1.0} if defensive != "CASH" else {"CASH": 1.0},
    }

    # BULL_FULL allocation
    if tqqq_w > 0:
        allocations["BULL_FULL"]["UPRO"] = upro_w
        allocations["BULL_FULL"]["TQQQ"] = tqqq_w
    else:
        allocations["BULL_FULL"]["UPRO"] = 1.0  # 100% UPRO when tqqq_pct=0

    # RECOVERY: 25% UPRO + 75% defensive
    if defensive != "CASH":
        allocations["RECOVERY"][defensive] = 0.75
    else:
        allocations["RECOVERY"]["CASH"] = 0.75

    # BULL_CAUTION: 50% UPRO + 50% defensive
    if bull_caution_enabled:
        if defensive != "CASH":
            allocations["BULL_CAUTION"] = {"UPRO": 0.50, defensive: 0.50}
        else:
            allocations["BULL_CAUTION"] = {"UPRO": 0.50, "CASH": 0.50}
    else:
        # If not enabled, BULL_CAUTION should never appear in regimes,
        # but add a fallback just in case
        allocations["BULL_CAUTION"] = allocations["BULL_FULL"]

    # Run through bars
    n = len(regimes)
    equity = INITIAL_CAPITAL
    equity_values = np.empty(n)
    prev_alloc = {}
    prev_regime = None
    transitions = 0
    tc_rate = COMMISSION_PCT + SLIPPAGE_PCT

    regime_arr = regimes.values

    # VIX kill switch data
    vix_arr = vix_close.values if vix_close is not None else None
    vix_threshold = vix_kill if isinstance(vix_kill, (int, float)) and vix_kill else None

    for i in range(n):
        regime = regime_arr[i]

        # VIX kill switch: override to CASH if VIX > threshold
        if vix_threshold and vix_arr is not None and not np.isnan(vix_arr[i]):
            if vix_arr[i] > vix_threshold:
                regime = "CRASH"  # Force to cash

        alloc = allocations.get(regime, {"CASH": 1.0})

        # Daily return from PREVIOUS allocation (applied to today's bar)
        daily_ret = 0.0
        for sym, weight in prev_alloc.items():
            if sym == "CASH":
                continue
            ret_val = returns[sym].iloc[i]
            if not np.isnan(ret_val):
                daily_ret += weight * ret_val

        # Transaction costs on allocation change
        tc = 0.0
        if prev_regime is not None and alloc != prev_alloc:
            all_syms = set(list(alloc.keys()) + list(prev_alloc.keys()))
            turnover = sum(abs(alloc.get(s, 0.0) - prev_alloc.get(s, 0.0)) for s in all_syms)
            tc = turnover * tc_rate
            transitions += 1

        equity *= 1 + daily_ret - tc
        equity_values[i] = equity

        prev_alloc = alloc
        prev_regime = regime

    # Compute metrics
    dates = regimes.index
    total_return = (equity / INITIAL_CAPITAL - 1) * 100
    n_years = (dates[-1] - dates[0]).days / 365.25
    if n_years <= 0:
        n_years = 1.0

    cagr = ((equity / INITIAL_CAPITAL) ** (1 / n_years) - 1) * 100

    # Daily returns for Sharpe/Sortino
    daily_rets = np.diff(equity_values) / equity_values[:-1]
    daily_rets = daily_rets[~np.isnan(daily_rets)]

    if len(daily_rets) > 0 and np.std(daily_rets) > 0:
        sharpe = (np.mean(daily_rets) / np.std(daily_rets)) * np.sqrt(252)
    else:
        sharpe = 0.0

    downside_rets = daily_rets[daily_rets < 0]
    if len(downside_rets) > 0 and np.std(downside_rets) > 0:
        sortino = (np.mean(daily_rets) / np.std(downside_rets)) * np.sqrt(252)
    else:
        sortino = 0.0

    # Max drawdown
    cummax = np.maximum.accumulate(equity_values)
    drawdowns = (equity_values - cummax) / cummax
    max_dd = np.min(drawdowns) * 100

    # Calmar
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0.0

    # Yearly returns
    equity_series = pd.Series(equity_values, index=dates)
    yearly_groups = equity_series.groupby(equity_series.index.year)
    yearly_returns = {}
    for year, group in yearly_groups:
        if len(group) > 1:
            yr_ret = (group.iloc[-1] / group.iloc[0] - 1) * 100
            yearly_returns[year] = round(yr_ret, 2)

    sorted(yearly_returns.keys())
    best_year = max(yearly_returns.values()) if yearly_returns else 0.0
    worst_year = min(yearly_returns.values()) if yearly_returns else 0.0
    year_2022 = yearly_returns.get(2022)

    # Best/worst year labels
    best_year_label = ""
    worst_year_label = ""
    for y, r in yearly_returns.items():
        if r == best_year:
            best_year_label = f"{y}: {r:+.2f}%"
        if r == worst_year:
            worst_year_label = f"{y}: {r:+.2f}%"

    return {
        "total_return_pct": round(total_return, 2),
        "cagr_pct": round(cagr, 2),
        "sharpe": round(sharpe, 4),
        "sortino": round(sortino, 4),
        "max_dd_pct": round(max_dd, 2),
        "calmar": round(calmar, 4),
        "transitions": transitions,
        "best_year": best_year_label,
        "worst_year": worst_year_label,
        "year_2022_return": round(year_2022, 2) if year_2022 is not None else "N/A",
        "yearly_returns": yearly_returns,
        "final_equity": round(equity, 2),
    }


# ============================================================
# Parameter Space
# ============================================================
def build_parameter_grid(has_vix: bool = False) -> list[dict]:
    """Build the full parameter grid (384 with VIX, 192 without)."""
    sma_periods = [(50, 200), (100, 200)]
    rsi_entries = [35, 40, 45, 50]
    upro_tqqq_splits = [(75, 25), (100, 0)]
    defensives = ["SHY", "TLT", "CASH"]
    confirm_days_list = [0, 2]
    bull_caution_list = [True, False]
    vix_kills = [False, 35] if has_vix else [False]

    grid = []
    for sma, rsi_e, split, defn, conf, bc, vk in itertools.product(
        sma_periods,
        rsi_entries,
        upro_tqqq_splits,
        defensives,
        confirm_days_list,
        bull_caution_list,
        vix_kills,
    ):
        grid.append(
            {
                "sma_fast": sma[0],
                "sma_slow": sma[1],
                "rsi_entry": rsi_e,
                "upro_pct": split[0],
                "tqqq_pct": split[1],
                "defensive": defn,
                "confirm_days": conf,
                "bull_caution": bc,
                "vix_kill": vk,
            }
        )

    return grid


def variant_name(p: dict) -> str:
    """Generate a human-readable variant name."""
    bc = "BC" if p["bull_caution"] else "noBC"
    vk = f"_VIX{p['vix_kill']}" if p["vix_kill"] else ""
    return (
        f"SMA{p['sma_fast']}_{p['sma_slow']}_RSI{p['rsi_entry']}_"
        f"U{p['upro_pct']}T{p['tqqq_pct']}_{p['defensive']}_"
        f"C{p['confirm_days']}_{bc}{vk}"
    )


# ============================================================
# Main
# ============================================================
def main():
    t_start = time.time()

    print("=" * 70)
    print("RAVI VAM PARAMETER SWEEP — DataBento Data")
    print("=" * 70)
    print()

    # Load data
    print("Loading data...")
    dfs = load_all_data()
    common_dates = align_dates(dfs)
    print(
        f"  Common dates: {common_dates[0].date()} to {common_dates[-1].date()} ({len(common_dates)} bars)"
    )

    # Pre-compute daily returns for all instruments on common dates
    print("Computing daily returns...")
    returns = {}
    for sym in ["UPRO", "TQQQ", "SHY", "TLT"]:
        returns[sym] = dfs[sym].loc[common_dates, "close"].pct_change()
    print()

    # Pre-compute SPY indicators for each unique SMA pair (2 pairs)
    spy_close = dfs["SPY"].loc[common_dates, "close"]
    sma_pairs = [(50, 200), (100, 200)]
    spy_indicators = {}
    for fast, slow in sma_pairs:
        print(f"  Computing SPY indicators for SMA({fast},{slow})...")
        spy_indicators[(fast, slow)] = compute_spy_indicators(spy_close, fast, slow)

    # Load VIX if available
    has_vix = "VIX" in dfs
    vix_close = None
    if has_vix:
        vix_close = dfs["VIX"].loc[common_dates, "close"]
        print("  VIX data available — running full 384 variations")
    else:
        print("  VIX data not found — running 192 variations (no VIX kill)")

    # Build parameter grid
    grid = build_parameter_grid(has_vix=has_vix)
    total = len(grid)
    print(f"\nTotal parameter combinations: {total}")
    print("Starting sweep...\n")

    # Run sweep
    results = []
    for idx, params in enumerate(grid):
        # Get pre-computed indicators
        ind = spy_indicators[(params["sma_fast"], params["sma_slow"])]

        # RSI caution zone: from (rsi_entry - 10) to rsi_entry
        rsi_caution_lo = params["rsi_entry"] - 10

        # Classify regimes
        regimes = classify_regimes(
            indicators=ind.loc[common_dates],
            rsi_entry=params["rsi_entry"],
            rsi_caution_lo=rsi_caution_lo,
            bull_caution_enabled=params["bull_caution"],
            confirm_days=params["confirm_days"],
        )

        # Run backtest
        metrics = run_backtest(
            regimes=regimes,
            returns=returns,
            upro_pct=params["upro_pct"],
            tqqq_pct=params["tqqq_pct"],
            defensive=params["defensive"],
            bull_caution_enabled=params["bull_caution"],
            vix_kill=params["vix_kill"],
            vix_close=vix_close,
        )

        # Build result row
        row = {
            "variant_name": variant_name(params),
            "sma_fast": params["sma_fast"],
            "sma_slow": params["sma_slow"],
            "rsi_entry": params["rsi_entry"],
            "upro_pct": params["upro_pct"],
            "tqqq_pct": params["tqqq_pct"],
            "defensive": params["defensive"],
            "confirm_days": params["confirm_days"],
            "bull_caution": params["bull_caution"],
            "vix_kill": params["vix_kill"],
            "total_return_pct": metrics["total_return_pct"],
            "cagr_pct": metrics["cagr_pct"],
            "sharpe": metrics["sharpe"],
            "sortino": metrics["sortino"],
            "max_dd_pct": metrics["max_dd_pct"],
            "calmar": metrics["calmar"],
            "transitions": metrics["transitions"],
            "best_year": metrics["best_year"],
            "worst_year": metrics["worst_year"],
            "year_2022_return": metrics["year_2022_return"],
        }
        results.append(row)

        # Progress
        if (idx + 1) % 25 == 0 or (idx + 1) == total:
            elapsed = time.time() - t_start
            print(
                f"  [{idx + 1:3d}/{total}] {elapsed:.1f}s — last: {row['variant_name']}  "
                f"Sharpe={row['sharpe']:.3f}  Return={row['total_return_pct']:.1f}%  "
                f"MaxDD={row['max_dd_pct']:.1f}%"
            )

    # Convert to DataFrame
    results_df = pd.DataFrame(results)

    # Sort by Sharpe descending
    results_df.sort_values("sharpe", ascending=False, inplace=True)
    results_df.reset_index(drop=True, inplace=True)

    # Save full CSV
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RESULTS_DIR / "vam_parameter_sweep.csv"
    results_df.to_csv(csv_path, index=False)
    print(f"\nFull results saved: {csv_path}")

    # Save top 20 JSON
    top20 = results_df.head(20).to_dict(orient="records")
    json_path = RESULTS_DIR / "vam_parameter_sweep_top20.json"
    with open(json_path, "w") as f:
        json.dump(
            {
                "metadata": {
                    "strategy": "Ravi VAM 7-State Regime Rotation",
                    "data_source": "DataBento XNAS.ITCH",
                    "data_range": f"{common_dates[0].date()} to {common_dates[-1].date()}",
                    "total_bars": len(common_dates),
                    "total_variants": total,
                    "initial_capital": INITIAL_CAPITAL,
                    "commission_pct": COMMISSION_PCT * 100,
                    "slippage_pct": SLIPPAGE_PCT * 100,
                    "generated": datetime.now().isoformat(),
                },
                "top_20_by_sharpe": top20,
            },
            f,
            indent=2,
            default=str,
        )
    print(f"Top 20 saved: {json_path}")

    # Print leaderboard
    print("\n" + "=" * 70)
    print("TOP 20 BY SHARPE RATIO")
    print("=" * 70)
    print(
        f"{'#':>3}  {'Variant':<50}  {'Sharpe':>7}  {'CAGR':>7}  {'Return':>9}  "
        f"{'MaxDD':>7}  {'Calmar':>7}  {'Trades':>6}  {'2022':>8}"
    )
    print("-" * 120)

    for i, row in results_df.head(20).iterrows():
        yr22 = (
            f"{row['year_2022_return']:+.1f}%"
            if isinstance(row["year_2022_return"], (int, float))
            else row["year_2022_return"]
        )
        print(
            f"{i + 1:3d}  {row['variant_name']:<50}  {row['sharpe']:>7.3f}  "
            f"{row['cagr_pct']:>6.1f}%  {row['total_return_pct']:>8.1f}%  "
            f"{row['max_dd_pct']:>6.1f}%  {row['calmar']:>7.3f}  "
            f"{row['transitions']:>6d}  {yr22:>8}"
        )

    # Print 2022 filter for top 20
    print("\n" + "=" * 70)
    print("2022 STRESS TEST — Top 20 by Sharpe")
    print("=" * 70)
    print("Strategies that lost >50% in 2022 are GARBAGE for leveraged ETFs.\n")

    for i, row in results_df.head(20).iterrows():
        yr22 = row["year_2022_return"]
        if isinstance(yr22, (int, float)):
            flag = " *** GARBAGE ***" if yr22 < -50 else (" !! CAUTION !!" if yr22 < -30 else " OK")
            print(f"  {i + 1:3d}. {row['variant_name']:<50}  2022: {yr22:+.1f}%{flag}")
        else:
            print(f"  {i + 1:3d}. {row['variant_name']:<50}  2022: N/A")

    # Summary stats
    print("\n" + "=" * 70)
    print("SWEEP SUMMARY")
    print("=" * 70)
    print(f"  Total variants tested: {total}")
    print(f"  Sharpe > 0.5:   {(results_df['sharpe'] > 0.5).sum()}")
    print(f"  Sharpe > 0.7:   {(results_df['sharpe'] > 0.7).sum()}")
    print(f"  Sharpe > 1.0:   {(results_df['sharpe'] > 1.0).sum()}")
    print(f"  CAGR > 20%:     {(results_df['cagr_pct'] > 20).sum()}")
    print(f"  Max DD > -40%:  {(results_df['max_dd_pct'] > -40).sum()}")

    # Filter: 2022 return > -50%
    yr22_col = results_df["year_2022_return"]
    numeric_2022 = pd.to_numeric(yr22_col, errors="coerce")
    surviving_2022 = (numeric_2022 > -50).sum()
    print(f"  2022 return > -50%: {surviving_2022}")

    elapsed = time.time() - t_start
    print(f"\n  Elapsed time: {elapsed:.1f}s")
    print("\nDone.")


if __name__ == "__main__":
    main()
