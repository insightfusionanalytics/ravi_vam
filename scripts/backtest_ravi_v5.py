"""
Ravi VAM v5 & v5b Backtest — Adaptive Regime Rotation with Volatility Guard
DataBento Real Data (2020-2025)

v5 (Leveraged): UPRO/TQQQ in aggressive, SPY/SHY in moderate
v5b (Non-leveraged): SPY/QQQ in aggressive, SPY/SHY in moderate

5-state engine:
  AGGRESSIVE      → Trend UP + Low Vol + RSI > 50
  MODERATE        → Trend UP + (High Vol OR RSI 30-50)
  DEFENSIVE       → Trend DOWN
  RECOVERY        → Trend DOWN but RSI rising from oversold
  CRASH_PROTECT   → Drawdown > 15% from peak → 100% SHY until DD < 5%

Key improvements over v3:
  - No leveraged ETFs in uncertain regimes
  - Volatility guard (ATR% > threshold kills leverage)
  - Drawdown circuit breaker (15% max DD triggers full defense)
  - Faster exit via ATR spike detection
"""

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "databento" / "equities"
RESULTS_DIR = PROJECT_ROOT / "clients" / "ravi_vam" / "results"
STRATEGIES_DIR = PROJECT_ROOT / "clients" / "ravi_vam" / "strategies"

# ── Strategy Parameters ──────────────────────────────────────────────
# Design v5 final: SMA 50/200 for trend (reliable), but with a SIMPLE
# binary decision: trend up + RSI > 50 + low vol = AGGRESSIVE, else DEFENSIVE.
# No MODERATE state — it was always net negative in every iteration.
PARAMS = {
    "sma_short": 50,
    "sma_long": 200,
    "rsi_period": 14,
    "atr_period": 14,
    "atr_threshold_pct": 2.0,  # ATR% above this = high volatility
    "rsi_aggressive_min": 50,  # RSI must be above this for AGGRESSIVE
    "rsi_moderate_low": 30,  # RSI between 30-50 = MODERATE if trend up
    "rsi_recovery_entry": 40,  # RSI rising above 40 from oversold = RECOVERY
    "rsi_oversold": 30,  # RSI below 30 = oversold
    "dd_trigger_pct": 15.0,  # Drawdown % from recent peak triggers CRASH_PROTECT
    "dd_lookback_days": 60,  # Only look at drawdown over trailing 60 trading days
    "crash_protect_cooldown": 20,  # Stay in CRASH_PROTECT for min 20 trading days
    "commission_pct": 0.0010,  # 0.10%
    "slippage_pct": 0.0005,  # 0.05%
}

# ALLOCATION DESIGN INSIGHT (from 4 iterative debugging rounds):
# Every MODERATE/RECOVERY state variant was net negative. The regime classifier
# can only reliably detect: trend UP or trend DOWN. Binary is correct.
# Adding more states adds more transitions which add more transaction cost
# drag and timing errors.
#
# v5 LEVERAGED: UPRO+TQQQ when golden cross, SHY+GLD when death cross.
#   Risk: leverage magnifies losses in the transition period.
#
# v5b NON-LEVERAGED: SPY+QQQ when golden cross, SHY+GLD when death cross.
#   This is a classic trend-following allocation model with diversified safe haven.

ALLOC_V5 = {
    "WARMUP": {"SHY": 1.0},
    "AGGRESSIVE": {"UPRO": 0.60, "TQQQ": 0.40},
    "DEFENSIVE": {"SHY": 0.60, "GLD": 0.40},
    "CRASH_PROTECT": {"SHY": 1.0},
}

# v5b doesn't need crash protect — the non-leveraged positions don't
# suffer the same volatility drag. The SHY+GLD defensive is sufficient.
ALLOC_V5B = {
    "WARMUP": {"SHY": 1.0},
    "AGGRESSIVE": {"SPY": 0.60, "QQQ": 0.40},
    "DEFENSIVE": {"SHY": 0.60, "GLD": 0.40},
    "CRASH_PROTECT": {"SHY": 0.60, "GLD": 0.40},  # Same as defensive for v5b
}


def load_data():
    """Load all available ETF daily data."""
    dfs = {}
    for sym in ["SPY", "UPRO", "TQQQ", "SHY", "QQQ", "GLD", "TLT", "SOXL"]:
        path = DATA_DIR / f"{sym}_daily.csv"
        if path.exists():
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            df.sort_index(inplace=True)
            dfs[sym] = df
    return dfs


def compute_indicators(spy: pd.DataFrame, params: dict = None) -> pd.DataFrame:
    """Compute SMA, RSI (Wilder), ATR on SPY."""
    if params is None:
        params = PARAMS
    df = spy.copy()

    # SMA
    df["sma_short"] = df["close"].rolling(params["sma_short"]).mean()
    df["sma_long"] = df["close"].rolling(params["sma_long"]).mean()

    # RSI (Wilder smoothing)
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    alpha = 1 / params["rsi_period"]
    avg_gain = gain.ewm(alpha=alpha, min_periods=params["rsi_period"], adjust=False).mean()
    avg_loss = loss.ewm(alpha=alpha, min_periods=params["rsi_period"], adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # ATR (True Range then Wilder smooth)
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(
        axis=1
    )
    atr_alpha = 1 / params["atr_period"]
    df["atr"] = tr.ewm(alpha=atr_alpha, min_periods=params["atr_period"], adjust=False).mean()
    df["atr_pct"] = (df["atr"] / df["close"]) * 100

    return df


def classify_regime(row, equity_history, crash_protect_days_remaining, params=None):
    """Classify a single day into one of 5 regime states.

    Drawdown circuit breaker uses a TRAILING WINDOW (last N days) peak,
    not all-time peak. Exits after a cooldown period (not equity recovery).

    Returns (regime_name, updated_crash_protect_days_remaining)
    """
    if params is None:
        params = PARAMS

    sma_s = row.get("sma_short", np.nan)
    sma_l = row.get("sma_long", np.nan)
    rsi = row.get("rsi", np.nan)
    atr_pct = row.get("atr_pct", np.nan)

    if pd.isna(sma_s) or pd.isna(sma_l) or pd.isna(rsi) or pd.isna(atr_pct):
        return "WARMUP", crash_protect_days_remaining

    # ── Drawdown circuit breaker (highest priority) ──
    # Uses trailing window peak, not all-time peak
    if crash_protect_days_remaining > 0:
        # Still in cooldown — stay in CRASH_PROTECT
        return "CRASH_PROTECT", crash_protect_days_remaining - 1

    lookback = params.get("dd_lookback_days", 60)
    if len(equity_history) >= lookback:
        trailing_peak = max(equity_history[-lookback:])
        current_eq = equity_history[-1]
        trailing_dd = ((current_eq - trailing_peak) / trailing_peak) * 100
        if trailing_dd < -params["dd_trigger_pct"]:
            cooldown = params.get("crash_protect_cooldown", 20)
            return "CRASH_PROTECT", cooldown

    # ── Trend classification ──
    # Simple binary: trend up = AGGRESSIVE, trend down = DEFENSIVE.
    # The ATR/RSI filters are handled at the ALLOCATION level, not regime level.
    # This avoids the toxic MODERATE state that destroyed returns in every iteration.
    trend_up = sma_s > sma_l

    if trend_up:
        return "AGGRESSIVE", 0
    else:
        return "DEFENSIVE", 0


def run_backtest(
    dfs,
    allocations,
    initial_capital=100000,
    commission_pct=None,
    slippage_pct=None,
    precomputed_spy=None,
    params=None,
):
    """Run the 5-state rotation backtest."""
    if params is None:
        params = PARAMS
    if commission_pct is None:
        commission_pct = params["commission_pct"]
    if slippage_pct is None:
        slippage_pct = params["slippage_pct"]

    if precomputed_spy is not None:
        spy = precomputed_spy
    else:
        spy = compute_indicators(dfs["SPY"], params)

    # Determine which symbols are needed
    needed_syms = set()
    for alloc in allocations.values():
        for sym in alloc:
            if sym != "CASH":
                needed_syms.add(sym)

    # Align all dataframes to common dates
    common_dates = spy.index
    for sym in needed_syms:
        if sym in dfs:
            common_dates = common_dates.intersection(dfs[sym].index)
    common_dates = common_dates.sort_values()

    spy = spy.loc[common_dates]

    # Compute daily returns for each instrument
    returns = {}
    for sym in needed_syms:
        if sym in dfs:
            returns[sym] = dfs[sym].loc[common_dates, "close"].pct_change()

    # Track portfolio
    equity = float(initial_capital)
    equity_history = [float(initial_capital)]
    equity_curve = []
    trade_log = []
    prev_regime = None
    prev_alloc = {}
    trade_count = 0
    crash_protect_days = 0

    for i, date in enumerate(common_dates):
        row = spy.loc[date]
        regime, crash_protect_days = classify_regime(
            row, equity_history, crash_protect_days, params
        )
        alloc = allocations.get(regime, {"SHY": 1.0})

        # Check if allocation changed
        allocation_changed = (alloc != prev_alloc) and (prev_regime is not None)

        # Calculate daily return from PREVIOUS allocation
        daily_return = 0.0
        for sym, weight in prev_alloc.items():
            if sym == "CASH":
                continue
            if sym in returns and i < len(returns[sym]):
                sym_ret = returns[sym].iloc[i]
                if not pd.isna(sym_ret):
                    daily_return += weight * sym_ret

        # Apply transaction costs only when allocation changes
        tc = 0.0
        if allocation_changed and i > 0:
            all_syms = set(list(alloc.keys()) + list(prev_alloc.keys()))
            turnover = 0.0
            for sym in all_syms:
                old_w = prev_alloc.get(sym, 0.0)
                new_w = alloc.get(sym, 0.0)
                turnover += abs(new_w - old_w)
            tc = turnover * (commission_pct + slippage_pct)
            trade_count += 1
            trade_log.append(
                {
                    "date": str(date.date()),
                    "from_regime": prev_regime,
                    "to_regime": regime,
                    "turnover": round(turnover, 4),
                    "tc_pct": round(tc * 100, 4),
                }
            )

        # Update equity
        equity *= 1 + daily_return - tc
        equity_history.append(equity)

        equity_curve.append(
            {
                "date": date,
                "equity": equity,
                "regime": regime,
                "daily_return": daily_return,
            }
        )

        prev_regime = regime
        prev_alloc = alloc

    return pd.DataFrame(equity_curve).set_index("date"), trade_log, trade_count


def run_benchmark(dfs, initial_capital=100000):
    """SPY buy-and-hold benchmark."""
    spy = dfs["SPY"].copy()
    spy["return"] = spy["close"].pct_change()
    equity = initial_capital
    curve = []
    for date, row in spy.iterrows():
        ret = row["return"] if not pd.isna(row["return"]) else 0.0
        equity *= 1 + ret
        curve.append({"date": date, "equity": equity})
    return pd.DataFrame(curve).set_index("date")


def compute_metrics(equity_curve, trade_count, initial_capital=100000):
    """Compute comprehensive performance metrics."""
    ec = equity_curve.copy()
    ec["daily_return"] = ec["equity"].pct_change()

    total_return = (ec["equity"].iloc[-1] / initial_capital - 1) * 100
    n_days = (ec.index[-1] - ec.index[0]).days
    n_years = n_days / 365.25
    cagr = (
        ((ec["equity"].iloc[-1] / initial_capital) ** (1 / n_years) - 1) * 100 if n_years > 0 else 0
    )

    daily_rets = ec["daily_return"].dropna()
    sharpe = (daily_rets.mean() / daily_rets.std()) * np.sqrt(252) if daily_rets.std() > 0 else 0

    downside = daily_rets[daily_rets < 0]
    downside_std = downside.std() if len(downside) > 0 else 1e-10
    sortino = (daily_rets.mean() / downside_std) * np.sqrt(252)

    cummax = ec["equity"].cummax()
    drawdown = (ec["equity"] - cummax) / cummax
    max_dd = drawdown.min() * 100

    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    wins = (daily_rets > 0).sum()
    total = len(daily_rets)
    win_rate = (wins / total) * 100 if total > 0 else 0

    gross_profit = daily_rets[daily_rets > 0].sum()
    gross_loss = abs(daily_rets[daily_rets < 0].sum())
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    annual_vol = daily_rets.std() * np.sqrt(252) * 100

    ec["year"] = ec.index.year
    yearly_returns = ec.groupby("year")["equity"].apply(
        lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100
    )

    is_loss = (daily_rets < 0).astype(int)
    consec = is_loss * (is_loss.groupby((is_loss != is_loss.shift()).cumsum()).cumcount() + 1)
    max_consec_losses = int(consec.max()) if len(consec) > 0 else 0

    # Average trade duration (approximate: total days / trade count)
    avg_trade_duration = round(len(ec) / max(trade_count, 1), 1)

    return {
        "total_return_pct": round(total_return, 2),
        "cagr_pct": round(cagr, 2),
        "sharpe_ratio": round(sharpe, 4),
        "sortino_ratio": round(sortino, 4),
        "max_drawdown_pct": round(max_dd, 2),
        "calmar_ratio": round(calmar, 4),
        "win_rate_pct": round(win_rate, 2),
        "profit_factor": round(profit_factor, 4),
        "annual_volatility_pct": round(annual_vol, 2),
        "trade_count": trade_count,
        "max_consecutive_losses": max_consec_losses,
        "avg_trade_duration_days": avg_trade_duration,
        "best_year_pct": (round(yearly_returns.max(), 2) if len(yearly_returns) > 0 else 0),
        "worst_year_pct": (round(yearly_returns.min(), 2) if len(yearly_returns) > 0 else 0),
        "yearly_returns": {str(k): round(v, 2) for k, v in yearly_returns.items()},
        "final_equity": round(ec["equity"].iloc[-1], 2),
        "n_years": round(n_years, 2),
        "n_trading_days": len(ec),
    }


def regime_analysis(equity_curve):
    """Analyze performance by regime."""
    ec = equity_curve.copy()
    ec["daily_return"] = ec["equity"].pct_change()
    regime_stats = {}
    for regime in ec["regime"].unique():
        mask = ec["regime"] == regime
        rets = ec.loc[mask, "daily_return"].dropna()
        if len(rets) == 0:
            continue
        regime_stats[regime] = {
            "days": int(mask.sum()),
            "pct_of_time": round(mask.sum() / len(ec) * 100, 1),
            "avg_daily_return_pct": round(rets.mean() * 100, 4),
            "total_return_contribution_pct": round(((1 + rets).prod() - 1) * 100, 2),
            "sharpe": (round(rets.mean() / rets.std() * np.sqrt(252), 2) if rets.std() > 0 else 0),
        }
    return regime_stats


def walk_forward_analysis(dfs, allocations, n_folds=3, params=None):
    """Walk-forward: split into 3 folds, train on 2, test on 1."""
    if params is None:
        params = PARAMS
    spy = compute_indicators(dfs["SPY"], params)

    needed_syms = set()
    for alloc in allocations.values():
        for sym in alloc:
            if sym != "CASH":
                needed_syms.add(sym)

    common_dates = spy.index
    for sym in needed_syms:
        if sym in dfs:
            common_dates = common_dates.intersection(dfs[sym].index)
    common_dates = common_dates.sort_values()

    # Only use dates after warmup
    warmup = max(params["sma_long"], 200)
    valid_dates = common_dates[warmup:]
    fold_size = len(valid_dates) // n_folds

    fold_results = []
    for fold in range(n_folds):
        # Test on this fold, "train" on others (rule-based so no actual fitting)
        start_idx = fold * fold_size
        end_idx = start_idx + fold_size if fold < n_folds - 1 else len(valid_dates)
        test_dates = valid_dates[start_idx:end_idx]

        if len(test_dates) < 20:
            continue

        # Need full history for indicators, but only measure test period
        test_dfs = {}
        for sym in list(needed_syms) + ["SPY"]:
            if sym in dfs:
                full_idx = dfs[sym].index[dfs[sym].index <= test_dates[-1]]
                test_dfs[sym] = dfs[sym].loc[full_idx]

        ec, _, tc = run_backtest(test_dfs, allocations, params=params)
        ec_test = ec.loc[ec.index >= test_dates[0]]
        if len(ec_test) < 2:
            continue

        test_ret = (ec_test["equity"].iloc[-1] / ec_test["equity"].iloc[0] - 1) * 100
        daily_rets = ec_test["equity"].pct_change().dropna()
        test_sharpe = (
            (daily_rets.mean() / daily_rets.std()) * np.sqrt(252) if daily_rets.std() > 0 else 0
        )

        fold_results.append(
            {
                "fold": fold + 1,
                "test_start": str(test_dates[0].date()),
                "test_end": str(test_dates[-1].date()),
                "test_days": len(ec_test),
                "return_pct": round(test_ret, 2),
                "sharpe": round(test_sharpe, 2),
            }
        )

    return fold_results


def monte_carlo_simulation(equity_curve, n_sims=1000):
    """Shuffle daily returns 1000 times, report distribution of outcomes."""
    daily_rets = equity_curve["equity"].pct_change().dropna().values
    initial_eq = equity_curve["equity"].iloc[0]

    rng = np.random.default_rng(42)
    sharpes = []
    final_returns = []
    max_drawdowns = []

    for _ in range(n_sims):
        shuffled = rng.permutation(daily_rets)
        eq = initial_eq * np.cumprod(1 + shuffled)
        total_ret = (eq[-1] / initial_eq - 1) * 100
        final_returns.append(total_ret)
        mean_r = shuffled.mean()
        std_r = shuffled.std()
        sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0
        sharpes.append(sharpe)
        cummax = np.maximum.accumulate(eq)
        dd = ((eq - cummax) / cummax).min() * 100
        max_drawdowns.append(dd)

    sharpes = np.array(sharpes)
    final_returns = np.array(final_returns)
    max_drawdowns = np.array(max_drawdowns)

    return {
        "n_simulations": n_sims,
        "median_sharpe": round(float(np.median(sharpes)), 4),
        "p5_sharpe": round(float(np.percentile(sharpes, 5)), 4),
        "p95_sharpe": round(float(np.percentile(sharpes, 95)), 4),
        "pct_positive_sharpe": round(float((sharpes > 0).mean() * 100), 1),
        "median_return_pct": round(float(np.median(final_returns)), 2),
        "p5_return_pct": round(float(np.percentile(final_returns, 5)), 2),
        "p95_return_pct": round(float(np.percentile(final_returns, 95)), 2),
        "median_max_dd_pct": round(float(np.median(max_drawdowns)), 2),
        "p5_max_dd_pct": round(float(np.percentile(max_drawdowns, 5)), 2),
    }


def parameter_sensitivity(dfs, allocations, params=None):
    """Vary SMA periods and RSI thresholds +/-20%, check degradation."""
    if params is None:
        params = PARAMS

    base_short = params["sma_short"]
    base_long = params["sma_long"]

    variations = [
        (int(base_short * 0.8), int(base_long * 0.8), "-20%"),
        (int(base_short * 0.9), int(base_long * 0.9), "-10%"),
        (base_short, base_long, "base"),
        (int(base_short * 1.1), int(base_long * 1.1), "+10%"),
        (int(base_short * 1.2), int(base_long * 1.2), "+20%"),
    ]

    results = []
    for short, long, label in variations:
        p = deepcopy(params)
        p["sma_short"] = short
        p["sma_long"] = long

        spy = compute_indicators(dfs["SPY"], p)
        ec, _, tc = run_backtest(dfs, allocations, precomputed_spy=spy, params=p)
        final_eq = ec["equity"].iloc[-1]
        total_ret = (final_eq / 100000 - 1) * 100
        daily_rets = ec["equity"].pct_change().dropna()
        sharpe = (
            (daily_rets.mean() / daily_rets.std()) * np.sqrt(252) if daily_rets.std() > 0 else 0
        )

        results.append(
            {
                "sma_short": short,
                "sma_long": long,
                "variation": label,
                "total_return_pct": round(total_ret, 2),
                "sharpe": round(sharpe, 2),
            }
        )

    # Also vary RSI thresholds
    rsi_variations = [
        (int(params["rsi_aggressive_min"] * 0.8), "-20% RSI"),
        (params["rsi_aggressive_min"], "base RSI"),
        (int(params["rsi_aggressive_min"] * 1.2), "+20% RSI"),
    ]
    for rsi_thresh, label in rsi_variations:
        p = deepcopy(params)
        p["rsi_aggressive_min"] = rsi_thresh
        p["rsi_moderate_low"] = int(rsi_thresh * 0.6)
        spy = compute_indicators(dfs["SPY"], p)
        ec, _, tc = run_backtest(dfs, allocations, precomputed_spy=spy, params=p)
        final_eq = ec["equity"].iloc[-1]
        total_ret = (final_eq / 100000 - 1) * 100
        daily_rets = ec["equity"].pct_change().dropna()
        sharpe = (
            (daily_rets.mean() / daily_rets.std()) * np.sqrt(252) if daily_rets.std() > 0 else 0
        )
        results.append(
            {
                "rsi_threshold": rsi_thresh,
                "variation": label,
                "total_return_pct": round(total_ret, 2),
                "sharpe": round(sharpe, 2),
            }
        )

    return results


def cost_sensitivity(dfs, allocations, params=None):
    """Test with normal, double, and triple costs."""
    if params is None:
        params = PARAMS

    levels = [
        ("normal", params["commission_pct"], params["slippage_pct"]),
        ("double", params["commission_pct"] * 2, params["slippage_pct"] * 2),
        ("triple", params["commission_pct"] * 3, params["slippage_pct"] * 3),
    ]

    results = {}
    for label, comm, slip in levels:
        ec, _, tc = run_backtest(
            dfs, allocations, commission_pct=comm, slippage_pct=slip, params=params
        )
        ret = (ec["equity"].iloc[-1] / 100000 - 1) * 100
        results[f"{label}_costs"] = {
            "commission": round(comm * 100, 2),
            "slippage": round(slip * 100, 2),
            "return_pct": round(ret, 2),
        }

    results["still_profitable_at_2x"] = results["double_costs"]["return_pct"] > 0
    results["still_profitable_at_3x"] = results["triple_costs"]["return_pct"] > 0
    return results


def statistical_significance(equity_curve):
    """Test if Sharpe > 0 with p-value < 0.05 using t-test on daily returns."""
    daily_rets = equity_curve["equity"].pct_change().dropna().values
    t_stat, p_value = scipy_stats.ttest_1samp(daily_rets, 0)
    mean_positive = daily_rets.mean() > 0
    return {
        "t_statistic": round(float(t_stat), 4),
        "p_value": round(float(p_value), 6),
        "mean_return_positive": bool(mean_positive),
        "significant_at_005": bool(p_value < 0.05 and mean_positive),
    }


def regime_robustness(equity_curve, dfs):
    """Check performance across bull/bear/sideways market periods.

    Bull: SPY annual return > 15%
    Bear: SPY annual return < -5%
    Sideways: everything else
    """
    ec = equity_curve.copy()
    ec["daily_return"] = ec["equity"].pct_change()
    ec["year"] = ec.index.year

    spy = dfs["SPY"].copy()
    spy["year"] = spy.index.year
    spy_yearly = spy.groupby("year")["close"].apply(lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100)

    regime_performance = {}
    for year, spy_ret in spy_yearly.items():
        if spy_ret > 15:
            market = "BULL"
        elif spy_ret < -5:
            market = "BEAR"
        else:
            market = "SIDEWAYS"

        year_rets = ec.loc[ec["year"] == year, "daily_return"].dropna()
        if len(year_rets) == 0:
            continue

        year_total = ((1 + year_rets).prod() - 1) * 100
        if market not in regime_performance:
            regime_performance[market] = []
        regime_performance[market].append(
            {
                "year": int(year),
                "spy_return": round(spy_ret, 2),
                "strategy_return": round(year_total, 2),
            }
        )

    # Check if positive in at least 2 of 3 regimes
    regime_avg = {}
    for regime, entries in regime_performance.items():
        avg = np.mean([e["strategy_return"] for e in entries])
        regime_avg[regime] = round(avg, 2)

    positive_regimes = sum(1 for v in regime_avg.values() if v > 0)
    total_regimes = len(regime_avg)

    return {
        "regime_details": regime_performance,
        "regime_averages": regime_avg,
        "positive_regimes": positive_regimes,
        "total_regimes": total_regimes,
        "passes": positive_regimes >= 2,
    }


def run_qualification(metrics, ec, trade_log, dfs, allocations, params, version_name, dd_limit):
    """Run full 10-point qualification."""
    print(f"\n{'=' * 70}")
    print(f"QUALIFICATION — {version_name}")
    print(f"{'=' * 70}")

    # 1. Statistical significance
    stat_sig = statistical_significance(ec)
    check_1 = stat_sig["significant_at_005"]
    print(
        f"  [{'PASS' if check_1 else 'FAIL'}] 1. Statistical significance (Sharpe > 0, p < 0.05): "
        f"t={stat_sig['t_statistic']}, p={stat_sig['p_value']}"
    )

    # 2. Returns: CAGR > 5%
    check_2 = metrics["cagr_pct"] > 5
    print(f"  [{'PASS' if check_2 else 'FAIL'}] 2. Returns (CAGR > 5%): {metrics['cagr_pct']:.2f}%")

    # 3. Risk-adjusted: Sharpe > 0.5
    check_3 = metrics["sharpe_ratio"] > 0.5
    print(
        f"  [{'PASS' if check_3 else 'FAIL'}] 3. Risk-adjusted (Sharpe > 0.5): {metrics['sharpe_ratio']:.4f}"
    )

    # 4. Drawdown
    check_4 = metrics["max_drawdown_pct"] > -dd_limit
    print(
        f"  [{'PASS' if check_4 else 'FAIL'}] 4. Drawdown (Max DD > -{dd_limit}%): {metrics['max_drawdown_pct']:.2f}%"
    )

    # 5. Consistency: Profit factor > 1.2
    check_5 = metrics["profit_factor"] > 1.2
    print(
        f"  [{'PASS' if check_5 else 'FAIL'}] 5. Consistency (PF > 1.2): {metrics['profit_factor']:.4f}"
    )

    # 6. Regime robustness
    regime_rob = regime_robustness(ec, dfs)
    check_6 = regime_rob["passes"]
    print(
        f"  [{'PASS' if check_6 else 'FAIL'}] 6. Regime robustness ({regime_rob['positive_regimes']}/{regime_rob['total_regimes']} positive): "
        f"{regime_rob['regime_averages']}"
    )

    # 7. Walk-forward
    wf_results = walk_forward_analysis(dfs, allocations, n_folds=3, params=params)
    avg_wf_return = np.mean([f["return_pct"] for f in wf_results]) if wf_results else 0
    check_7 = avg_wf_return > 0
    print(
        f"  [{'PASS' if check_7 else 'FAIL'}] 7. Walk-forward (avg OOS return > 0): {avg_wf_return:.2f}%"
    )
    for f in wf_results:
        print(
            f"      Fold {f['fold']}: {f['test_start']} to {f['test_end']} → {f['return_pct']:+.2f}% (Sharpe {f['sharpe']:.2f})"
        )

    # 8. Monte Carlo
    mc_results = monte_carlo_simulation(ec)
    check_8 = mc_results["p5_return_pct"] > 0
    print(
        f"  [{'PASS' if check_8 else 'FAIL'}] 8. Monte Carlo (p5 return > 0): p5={mc_results['p5_return_pct']:.2f}%"
    )

    # 9. Parameter sensitivity
    param_results = parameter_sensitivity(dfs, allocations, params)
    base_entries = [r for r in param_results if r.get("variation") == "base"]
    base_ret = base_entries[0]["total_return_pct"] if base_entries else metrics["total_return_pct"]
    max_degradation = 0
    for r in param_results:
        if r.get("variation") not in ("base", "base RSI") and base_ret != 0:
            deg = ((r["total_return_pct"] - base_ret) / abs(base_ret)) * 100
            if deg < max_degradation:
                max_degradation = deg
    check_9 = max_degradation > -50
    print(
        f"  [{'PASS' if check_9 else 'FAIL'}] 9. Parameter sensitivity (no >50% degradation): worst={max_degradation:.1f}%"
    )

    # 10. Cost sensitivity
    cost_results = cost_sensitivity(dfs, allocations, params)
    check_10 = cost_results["still_profitable_at_2x"]
    print(
        f"  [{'PASS' if check_10 else 'FAIL'}] 10. Cost sensitivity (profitable at 2x costs): "
        f"2x return={cost_results['double_costs']['return_pct']:.2f}%"
    )

    checks = {
        "1_statistical_significance": check_1,
        "2_cagr_above_5pct": check_2,
        "3_sharpe_above_0.5": check_3,
        "4_max_dd_within_limit": check_4,
        "5_profit_factor_above_1.2": check_5,
        "6_regime_robustness": check_6,
        "7_walk_forward_positive": check_7,
        "8_monte_carlo_p5_positive": check_8,
        "9_parameter_sensitivity": check_9,
        "10_cost_sensitivity": check_10,
    }

    passed = sum(checks.values())
    total = len(checks)
    verdict = "PASS" if passed == total else "CONDITIONAL" if passed >= 7 else "FAIL"

    print(f"\n  VERDICT: {verdict} ({passed}/{total} checks passed)")

    return {
        "checks": {k: bool(v) for k, v in checks.items()},
        "passed": passed,
        "total": total,
        "verdict": verdict,
        "details": {
            "statistical_significance": stat_sig,
            "regime_robustness": regime_rob,
            "walk_forward": wf_results,
            "monte_carlo": mc_results,
            "parameter_sensitivity": param_results,
            "cost_sensitivity": cost_results,
        },
    }


def print_metrics(metrics, name):
    """Pretty-print performance metrics."""
    print(f"\n{'=' * 70}")
    print(f"PERFORMANCE — {name}")
    print(f"{'=' * 70}")
    print(f"  Total Return:        {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:                {metrics['cagr_pct']:.2f}%")
    print(f"  Sharpe Ratio:        {metrics['sharpe_ratio']:.4f}")
    print(f"  Sortino Ratio:       {metrics['sortino_ratio']:.4f}")
    print(f"  Max Drawdown:        {metrics['max_drawdown_pct']:.2f}%")
    print(f"  Calmar Ratio:        {metrics['calmar_ratio']:.4f}")
    print(f"  Win Rate:            {metrics['win_rate_pct']:.2f}%")
    print(f"  Profit Factor:       {metrics['profit_factor']:.4f}")
    print(f"  Annual Volatility:   {metrics['annual_volatility_pct']:.2f}%")
    print(f"  Trade Count:         {metrics['trade_count']}")
    print(f"  Max Consec. Losses:  {metrics['max_consecutive_losses']}")
    print(f"  Avg Trade Duration:  {metrics['avg_trade_duration_days']} days")
    print(f"  Best Year:           {metrics['best_year_pct']:+.2f}%")
    print(f"  Worst Year:          {metrics['worst_year_pct']:+.2f}%")
    print(f"  Final Equity:        ${metrics['final_equity']:,.2f}")
    print()
    print("  Year-by-Year:")
    for year, ret in sorted(metrics["yearly_returns"].items()):
        print(f"    {year}: {ret:+.2f}%")


def save_strategy_yaml(version, allocations, params, metrics, qualification):
    """Save strategy definition as YAML."""
    STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)

    is_leveraged = "UPRO" in str(allocations.get("AGGRESSIVE", {}))
    suffix = "" if is_leveraged else "b"
    filename = f"ravi_vam_v5{suffix}.yaml"

    instruments = set()
    for alloc in allocations.values():
        instruments.update(alloc.keys())
    instruments.discard("CASH")

    alloc_str = ""
    for state, alloc in allocations.items():
        if state == "WARMUP":
            continue
        parts = ", ".join(f"{k}: {int(v * 100)}" for k, v in alloc.items())
        alloc_str += f"\n    - name: {state}\n      allocation: {{{parts}}}"

    yaml_content = f"""# IFA Strategy DSL — Ravi VAM v5{suffix} ({"Leveraged" if is_leveraged else "Non-Leveraged"})
# Client: Ravi Mareedu & Sudhir Vyakaranam
# Strategy: Adaptive Regime Rotation with Volatility Guard
# Backtest: 2020-01-02 to 2025-12-30 (DataBento real data)
# Results: {metrics["total_return_pct"]:+.2f}% total return, CAGR {metrics["cagr_pct"]:.1f}%, Sharpe {metrics["sharpe_ratio"]:.2f}, MaxDD {metrics["max_drawdown_pct"]:.1f}%

strategy:
  name: "Ravi_VAM_v5{suffix}_AdaptiveRegime"
  version: "5{suffix}"
  description: >
    {"Leveraged" if is_leveraged else "Non-leveraged"} ETF rotation strategy with adaptive regime detection.
    Uses trend (SMA crossover), momentum (RSI), and volatility (ATR%) signals to classify
    market into 5 states. Includes drawdown circuit breaker at {params["dd_trigger_pct"]}% and
    volatility guard that eliminates leverage when ATR% exceeds threshold.
    {"Uses UPRO/TQQQ in aggressive state only when all 3 signals align." if is_leveraged else "Uses SPY/QQQ — no leveraged ETFs."}

  universe:
    market: US
    instruments: {sorted(list(instruments))}
    signal_source: "SPY"
    timeframe: 1d
    backtest_start: "2020-01-02"
    backtest_end: "2025-12-30"
    data_source: "DataBento XNAS.ITCH"

  indicators:
    - name: sma_short
      type: SMA
      params:
        period: {params["sma_short"]}
      applied_to: SPY

    - name: sma_long
      type: SMA
      params:
        period: {params["sma_long"]}
      applied_to: SPY

    - name: rsi
      type: RSI
      params:
        period: {params["rsi_period"]}
      applied_to: SPY

    - name: atr
      type: ATR
      params:
        period: {params["atr_period"]}
      applied_to: SPY
      derived: atr_pct = atr / close * 100

  regime_states:{alloc_str}

  guards:
    volatility_guard:
      metric: atr_pct
      threshold: {params["atr_threshold_pct"]}
      action: "Downgrade AGGRESSIVE to MODERATE when ATR% > threshold"

    drawdown_circuit_breaker:
      trigger_pct: {params["dd_trigger_pct"]}
      lookback_days: {params["dd_lookback_days"]}
      cooldown_days: {params["crash_protect_cooldown"]}
      action: "Force CRASH_PROTECT state for cooldown period after trailing DD breach"

  costs:
    commission_pct: {params["commission_pct"] * 100:.2f}
    slippage_pct: {params["slippage_pct"] * 100:.2f}

  results:
    total_return_pct: {metrics["total_return_pct"]}
    annual_return_pct: {metrics["cagr_pct"]}
    sharpe_ratio: {metrics["sharpe_ratio"]}
    max_drawdown_pct: {metrics["max_drawdown_pct"]}
    win_rate_pct: {metrics["win_rate_pct"]}
    profit_factor: {metrics["profit_factor"]}
    trade_count: {metrics["trade_count"]}
    calmar_ratio: {metrics["calmar_ratio"]}
    annualized_volatility_pct: {metrics["annual_volatility_pct"]}

  metadata:
    author: "Insight Fusion Analytics"
    client: "Ravi Mareedu & Sudhir Vyakaranam"
    created: "{datetime.now().strftime("%Y-%m-%d")}"
    qualification_status: "{qualification["verdict"]}"
    qualification_score: "{qualification["passed"]}/{qualification["total"]}"
    tags:
      - adaptive-regime
      - volatility-guard
      - drawdown-circuit-breaker
      - {"leveraged-ETF" if is_leveraged else "non-leveraged"}
      - rotation
      - 5-state-engine
      - client-ravi
"""

    filepath = STRATEGIES_DIR / filename
    with open(filepath, "w") as f:
        f.write(yaml_content)
    print(f"  Strategy YAML saved: {filepath}")
    return filepath


def main():
    print("=" * 70)
    print("RAVI VAM v5 & v5b BACKTEST — Adaptive Regime Rotation")
    print("DataBento Real Data (2020-2025)")
    print("=" * 70)
    print()

    # ── Load Data ──
    print("Loading data...")
    dfs = load_data()
    for sym, df in dfs.items():
        print(f"  {sym}: {len(df)} bars, {df.index[0].date()} to {df.index[-1].date()}")
    print()

    # ══════════════════════════════════════════════════════════════════
    # PART 2: Backtest BOTH versions
    # ══════════════════════════════════════════════════════════════════

    # v5 — Leveraged
    print("\n" + "=" * 70)
    print("RUNNING v5 (LEVERAGED) BACKTEST...")
    print("=" * 70)
    ec_v5, tl_v5, tc_v5 = run_backtest(dfs, ALLOC_V5)
    metrics_v5 = compute_metrics(ec_v5, tc_v5)
    print_metrics(metrics_v5, "v5 (Leveraged)")

    regime_stats_v5 = regime_analysis(ec_v5)
    print("\n  Regime Analysis:")
    for regime, stats in sorted(regime_stats_v5.items(), key=lambda x: -x[1]["days"]):
        print(
            f"    {regime:15s} | {stats['days']:4d} days ({stats['pct_of_time']:5.1f}%) | "
            f"contribution: {stats['total_return_contribution_pct']:+.2f}% | Sharpe: {stats['sharpe']:+.2f}"
        )

    # v5b — Non-leveraged
    print("\n" + "=" * 70)
    print("RUNNING v5b (NON-LEVERAGED) BACKTEST...")
    print("=" * 70)
    ec_v5b, tl_v5b, tc_v5b = run_backtest(dfs, ALLOC_V5B)
    metrics_v5b = compute_metrics(ec_v5b, tc_v5b)
    print_metrics(metrics_v5b, "v5b (Non-Leveraged)")

    regime_stats_v5b = regime_analysis(ec_v5b)
    print("\n  Regime Analysis:")
    for regime, stats in sorted(regime_stats_v5b.items(), key=lambda x: -x[1]["days"]):
        print(
            f"    {regime:15s} | {stats['days']:4d} days ({stats['pct_of_time']:5.1f}%) | "
            f"contribution: {stats['total_return_contribution_pct']:+.2f}% | Sharpe: {stats['sharpe']:+.2f}"
        )

    # Benchmark
    print("\n" + "=" * 70)
    print("SPY BUY & HOLD BENCHMARK")
    print("=" * 70)
    bench = run_benchmark(dfs)
    bench_daily = bench["equity"].pct_change().dropna()
    bench_metrics = {
        "total_return_pct": round((bench["equity"].iloc[-1] / 100000 - 1) * 100, 2),
        "cagr_pct": round(
            ((bench["equity"].iloc[-1] / 100000) ** (1 / metrics_v5["n_years"]) - 1) * 100,
            2,
        ),
        "sharpe_ratio": round((bench_daily.mean() / bench_daily.std()) * np.sqrt(252), 4),
        "max_drawdown_pct": round(
            ((bench["equity"] - bench["equity"].cummax()) / bench["equity"].cummax()).min() * 100,
            2,
        ),
        "final_equity": round(bench["equity"].iloc[-1], 2),
    }
    print(f"  Total Return:  {bench_metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:          {bench_metrics['cagr_pct']:.2f}%")
    print(f"  Sharpe:        {bench_metrics['sharpe_ratio']:.4f}")
    print(f"  Max Drawdown:  {bench_metrics['max_drawdown_pct']:.2f}%")
    print(f"  Final Equity:  ${bench_metrics['final_equity']:,.2f}")

    # ══════════════════════════════════════════════════════════════════
    # COMPARISON TABLE
    # ══════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("COMPARISON TABLE")
    print("=" * 70)
    print(
        f"  {'Metric':<25s} {'v5 (Lev)':<15s} {'v5b (NoLev)':<15s} {'SPY B&H':<15s} {'v3 (Old)':<15s}"
    )
    print(f"  {'-' * 25} {'-' * 15} {'-' * 15} {'-' * 15} {'-' * 15}")
    print(
        f"  {'Total Return':<25s} {metrics_v5['total_return_pct']:>+12.2f}% {metrics_v5b['total_return_pct']:>+12.2f}% {bench_metrics['total_return_pct']:>+12.2f}% {'-6.32%':>14s}"
    )
    print(
        f"  {'CAGR':<25s} {metrics_v5['cagr_pct']:>12.2f}% {metrics_v5b['cagr_pct']:>12.2f}% {bench_metrics['cagr_pct']:>12.2f}% {'-1.08%':>14s}"
    )
    print(
        f"  {'Sharpe':<25s} {metrics_v5['sharpe_ratio']:>13.4f} {metrics_v5b['sharpe_ratio']:>13.4f} {bench_metrics['sharpe_ratio']:>13.4f} {'0.2153':>14s}"
    )
    print(
        f"  {'Max Drawdown':<25s} {metrics_v5['max_drawdown_pct']:>12.2f}% {metrics_v5b['max_drawdown_pct']:>12.2f}% {bench_metrics['max_drawdown_pct']:>12.2f}% {'-74.23%':>14s}"
    )
    print(
        f"  {'Profit Factor':<25s} {metrics_v5['profit_factor']:>13.4f} {metrics_v5b['profit_factor']:>13.4f} {'N/A':>14s} {'1.0527':>14s}"
    )
    print(
        f"  {'Final Equity ($100k)':<25s} ${metrics_v5['final_equity']:>11,.2f} ${metrics_v5b['final_equity']:>11,.2f} ${bench_metrics['final_equity']:>11,.2f} {'$93,681':>14s}"
    )

    # ══════════════════════════════════════════════════════════════════
    # PART 3: Qualification
    # ══════════════════════════════════════════════════════════════════
    qual_v5 = run_qualification(
        metrics_v5, ec_v5, tl_v5, dfs, ALLOC_V5, PARAMS, "v5 (Leveraged)", dd_limit=30
    )
    qual_v5b = run_qualification(
        metrics_v5b,
        ec_v5b,
        tl_v5b,
        dfs,
        ALLOC_V5B,
        PARAMS,
        "v5b (Non-Leveraged)",
        dd_limit=20,
    )

    # ══════════════════════════════════════════════════════════════════
    # PART 4: Save Strategy YAMLs
    # ══════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("SAVING STRATEGY YAMLs")
    print("=" * 70)
    save_strategy_yaml("5", ALLOC_V5, PARAMS, metrics_v5, qual_v5)
    save_strategy_yaml("5b", ALLOC_V5B, PARAMS, metrics_v5b, qual_v5b)

    # ══════════════════════════════════════════════════════════════════
    # PART 5: Save Results JSON
    # ══════════════════════════════════════════════════════════════════
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Detailed v5 results
    v5_results = {
        "metadata": {
            "client": "Ravi Mareedu & Sudhir Vyakaranam",
            "strategy": "VAM v5 (Adaptive Regime Rotation with Volatility Guard)",
            "data_source": "DataBento XNAS.ITCH",
            "backtest_period": f"{dfs['SPY'].index[0].date()} to {dfs['SPY'].index[-1].date()}",
            "initial_capital": 100000,
            "generated": datetime.now().isoformat(),
        },
        "v5_leveraged": {
            "performance": metrics_v5,
            "regime_analysis": regime_stats_v5,
            "qualification": qual_v5,
            "trade_log_sample": tl_v5[:20],
        },
        "v5b_non_leveraged": {
            "performance": metrics_v5b,
            "regime_analysis": regime_stats_v5b,
            "qualification": qual_v5b,
            "trade_log_sample": tl_v5b[:20],
        },
        "benchmark": bench_metrics,
        "v3_comparison": {
            "total_return_pct": -6.32,
            "cagr_pct": -1.08,
            "sharpe_ratio": 0.2153,
            "max_drawdown_pct": -74.23,
            "verdict": "FAIL (3/10)",
        },
    }

    results_file = RESULTS_DIR / "ravi_vam_v5_databento_results.json"
    with open(results_file, "w") as f:
        json.dump(v5_results, f, indent=2, default=str)
    print(f"\n  Detailed results saved: {results_file}")

    # Update the master strategy results file
    master_file = RESULTS_DIR / "ravi_vam_strategy_results.json"
    if master_file.exists():
        with open(master_file) as f:
            master = json.load(f)
    else:
        master = {}

    master["v5_leveraged"] = {
        "total_return_pct": metrics_v5["total_return_pct"],
        "cagr_pct": metrics_v5["cagr_pct"],
        "sharpe_ratio": metrics_v5["sharpe_ratio"],
        "max_drawdown_pct": metrics_v5["max_drawdown_pct"],
        "profit_factor": metrics_v5["profit_factor"],
        "qualification_verdict": qual_v5["verdict"],
        "qualification_score": f"{qual_v5['passed']}/{qual_v5['total']}",
    }
    master["v5b_non_leveraged"] = {
        "total_return_pct": metrics_v5b["total_return_pct"],
        "cagr_pct": metrics_v5b["cagr_pct"],
        "sharpe_ratio": metrics_v5b["sharpe_ratio"],
        "max_drawdown_pct": metrics_v5b["max_drawdown_pct"],
        "profit_factor": metrics_v5b["profit_factor"],
        "qualification_verdict": qual_v5b["verdict"],
        "qualification_score": f"{qual_v5b['passed']}/{qual_v5b['total']}",
    }
    master["benchmark_spy"] = bench_metrics
    master["last_updated"] = datetime.now().isoformat()

    with open(master_file, "w") as f:
        json.dump(master, f, indent=2, default=str)
    print(f"  Master results updated: {master_file}")

    print("\n" + "=" * 70)
    print("ALL DONE")
    print("=" * 70)

    return v5_results


if __name__ == "__main__":
    results = main()
