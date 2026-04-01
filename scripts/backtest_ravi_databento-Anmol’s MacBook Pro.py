"""
Ravi VAM v3 Backtest on DataBento Real Data (2020-2025)

7-state rotation engine:
  CRASH       → RSI < 20 → 100% cash (highest priority)
  NEUTRAL     → |SMA50 - SMA200| / SMA200 < 0.5% → 100% SHY
  RECOVERY    → SMA50 < SMA200 AND RSI > 50 AND RSI[-5] < 30 → 25% UPRO + 75% SHY
  MOMENTUM    → SMA50 > SMA200 AND RSI > 65 → 100% UPRO
  BULL_CAUTION → SMA50 > SMA200 AND 30 <= RSI <= 40 → 50% UPRO + 50% SHY
  BULL_FULL   → SMA50 > SMA200 AND RSI > 40 → 75% UPRO + 25% TQQQ
  BEAR_HEDGE  → SMA50 < SMA200 → 100% SHY (default bear)

Signal source: SPY
Instruments: UPRO, TQQQ, SHY
Commission: 0.10% per trade, Slippage: 0.05% per trade
"""

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "databento" / "equities"
RESULTS_DIR = PROJECT_ROOT / "clients" / "ravi_vam" / "results"


def load_data():
    """Load SPY, UPRO, TQQQ, SHY daily data."""
    dfs = {}
    for sym in ["SPY", "UPRO", "TQQQ", "SHY"]:
        path = DATA_DIR / f"{sym}_daily.csv"
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.sort_index(inplace=True)
        dfs[sym] = df
    return dfs


def compute_indicators(spy: pd.DataFrame) -> pd.DataFrame:
    """Compute SMA(50), SMA(200), RSI(14) on SPY."""
    df = spy.copy()

    # SMA
    df["sma_50"] = df["close"].rolling(50).mean()
    df["sma_200"] = df["close"].rolling(200).mean()

    # RSI (Wilder smoothing)
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - (100 / (1 + rs))

    return df


def classify_regime(row, rsi_5_ago):
    """Classify a single day into one of 7 regime states.

    Priority order matters — CRASH checked first, BEAR_HEDGE is default fallback.
    """
    sma50 = row["sma_50"]
    sma200 = row["sma_200"]
    rsi = row["rsi_14"]

    if pd.isna(sma50) or pd.isna(sma200) or pd.isna(rsi):
        return "WARMUP"

    # 1. CRASH — highest priority circuit breaker
    if rsi < 20:
        return "CRASH"

    # 2. NEUTRAL — SMA convergence zone
    if abs(sma50 - sma200) / sma200 < 0.005:
        return "NEUTRAL"

    # 3. Bull regimes (SMA50 > SMA200)
    if sma50 > sma200:
        if rsi > 65:
            return "MOMENTUM"
        elif 30 <= rsi <= 40:
            return "BULL_CAUTION"
        elif rsi > 40:
            return "BULL_FULL"
        else:
            # RSI < 30 in bull — unusual, treat as caution
            return "BULL_CAUTION"

    # 4. Bear regimes (SMA50 < SMA200)
    if sma50 < sma200:
        if rsi > 50 and rsi_5_ago is not None and rsi_5_ago < 30:
            return "RECOVERY"
        return "BEAR_HEDGE"

    return "BEAR_HEDGE"


ALLOCATIONS = {
    "WARMUP": {"CASH": 1.0},
    "CRASH": {"CASH": 1.0},
    "NEUTRAL": {"SHY": 1.0},
    "BULL_FULL": {"UPRO": 0.75, "TQQQ": 0.25},
    "BULL_CAUTION": {"UPRO": 0.50, "SHY": 0.50},
    "MOMENTUM": {"UPRO": 1.0},
    "RECOVERY": {"UPRO": 0.25, "SHY": 0.75},
    "BEAR_HEDGE": {"SHY": 1.0},
}


def run_backtest(
    dfs,
    initial_capital=100000,
    commission_pct=0.0010,
    slippage_pct=0.0005,
    precomputed_spy=None,
):
    """Run the 7-state rotation backtest."""
    if precomputed_spy is not None:
        spy = precomputed_spy
    else:
        spy = compute_indicators(dfs["SPY"])

    # Align all dataframes to common dates
    common_dates = spy.index
    for sym in ["UPRO", "TQQQ", "SHY"]:
        common_dates = common_dates.intersection(dfs[sym].index)
    common_dates = common_dates.sort_values()

    spy = spy.loc[common_dates]

    # Compute daily returns for each instrument
    returns = {}
    for sym in ["UPRO", "TQQQ", "SHY"]:
        returns[sym] = dfs[sym].loc[common_dates, "close"].pct_change()

    # Track portfolio
    equity = initial_capital
    equity_curve = []
    trade_log = []
    regimes = []
    prev_regime = None
    prev_alloc = {}
    trade_count = 0

    rsi_series = spy["rsi_14"]

    for i, date in enumerate(common_dates):
        # Get RSI from 5 days ago for RECOVERY check
        rsi_5_ago = rsi_series.iloc[i - 5] if i >= 5 else None

        regime = classify_regime(spy.loc[date], rsi_5_ago)
        alloc = ALLOCATIONS[regime]

        # Check if allocation changed (trade occurred)
        allocation_changed = (alloc != prev_alloc) and (prev_regime is not None)

        # Calculate daily return
        daily_return = 0.0
        for sym, weight in prev_alloc.items():
            if sym == "CASH":
                continue
            sym_ret = returns[sym].iloc[i] if not pd.isna(returns[sym].iloc[i]) else 0.0
            daily_return += weight * sym_ret

        # Apply transaction costs only when allocation changes
        tc = 0.0
        if allocation_changed and i > 0:
            # Estimate turnover: sum of absolute weight changes
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

        equity_curve.append(
            {
                "date": date,
                "equity": equity,
                "regime": regime,
                "daily_return": daily_return,
            }
        )

        regimes.append(regime)
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
    n_years = (ec.index[-1] - ec.index[0]).days / 365.25
    cagr = ((ec["equity"].iloc[-1] / initial_capital) ** (1 / n_years) - 1) * 100

    # Sharpe (annualized, 252 trading days, risk-free = 0 for simplicity)
    daily_rets = ec["daily_return"].dropna()
    sharpe = (daily_rets.mean() / daily_rets.std()) * np.sqrt(252) if daily_rets.std() > 0 else 0

    # Sortino (downside deviation)
    downside = daily_rets[daily_rets < 0]
    downside_std = downside.std() if len(downside) > 0 else 1e-10
    sortino = (daily_rets.mean() / downside_std) * np.sqrt(252)

    # Max drawdown
    cummax = ec["equity"].cummax()
    drawdown = (ec["equity"] - cummax) / cummax
    max_dd = drawdown.min() * 100

    # Calmar
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    # Win rate (daily)
    wins = (daily_rets > 0).sum()
    total = len(daily_rets)
    win_rate = (wins / total) * 100 if total > 0 else 0

    # Profit factor
    gross_profit = daily_rets[daily_rets > 0].sum()
    gross_loss = abs(daily_rets[daily_rets < 0].sum())
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Annual volatility
    annual_vol = daily_rets.std() * np.sqrt(252) * 100

    # Best/worst year
    ec["year"] = ec.index.year
    yearly_returns = ec.groupby("year")["equity"].apply(
        lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100
    )

    # Max consecutive losses
    is_loss = (daily_rets < 0).astype(int)
    consec = is_loss * (is_loss.groupby((is_loss != is_loss.shift()).cumsum()).cumcount() + 1)
    max_consec_losses = int(consec.max())

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
        "best_year_pct": round(yearly_returns.max(), 2),
        "worst_year_pct": round(yearly_returns.min(), 2),
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
            "total_return_pct": round((1 + rets).prod() - 1, 4) * 100,
            "sharpe": (round(rets.mean() / rets.std() * np.sqrt(252), 2) if rets.std() > 0 else 0),
        }
    return regime_stats


def walk_forward_analysis(dfs, n_folds=5, train_pct=0.6):
    """Walk-forward analysis: train on 60%, test on 40% of each fold."""
    spy = compute_indicators(dfs["SPY"])
    common_dates = spy.index
    for sym in ["UPRO", "TQQQ", "SHY"]:
        common_dates = common_dates.intersection(dfs[sym].index)
    common_dates = common_dates.sort_values()

    # Only use dates after warmup (200 bars for SMA200)
    valid_dates = common_dates[200:]
    fold_size = len(valid_dates) // n_folds

    fold_results = []
    for fold in range(n_folds):
        start_idx = fold * fold_size
        end_idx = start_idx + fold_size if fold < n_folds - 1 else len(valid_dates)
        fold_dates = valid_dates[start_idx:end_idx]

        train_end = int(len(fold_dates) * train_pct)
        test_dates = fold_dates[train_end:]

        if len(test_dates) < 20:
            continue

        # Run backtest on test period only (strategy is rule-based, no fitting)
        # We create sub-dataframes for the test period
        test_dfs = {}
        for sym in ["SPY", "UPRO", "TQQQ", "SHY"]:
            # Need full history up to test period for indicators
            full_idx = dfs[sym].index[dfs[sym].index <= test_dates[-1]]
            test_dfs[sym] = dfs[sym].loc[full_idx]

        ec, _, tc = run_backtest(test_dfs)
        # Filter to only test period
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


def parameter_sensitivity(dfs, base_sma_short=50, base_sma_long=200):
    """Vary SMA periods +/-20% and check if results degrade >50%."""
    variations = [
        (40, 160, "-20%"),
        (45, 180, "-10%"),
        (50, 200, "base"),
        (55, 220, "+10%"),
        (60, 240, "+20%"),
    ]

    results = []
    for short, long, label in variations:
        spy = dfs["SPY"].copy()
        spy["sma_50"] = spy["close"].rolling(short).mean()
        spy["sma_200"] = spy["close"].rolling(long).mean()

        delta = spy["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        rs = avg_gain / avg_loss
        spy["rsi_14"] = 100 - (100 / (1 + rs))

        ec, _, tc = run_backtest(dfs, precomputed_spy=spy)
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

    return results


def monte_carlo_simulation(equity_curve, n_sims=1000):
    """Shuffle daily returns 1000 times, report 5th percentile final equity and Sharpe.

    The point of Monte Carlo here is to test if the SEQUENCE of returns matters.
    We shuffle daily returns, rebuild equity curves, and check the distribution
    of final outcomes (total return, max drawdown, Sharpe).
    """
    daily_rets = equity_curve["equity"].pct_change().dropna().values
    initial_eq = equity_curve["equity"].iloc[0]

    sharpes = []
    final_returns = []
    max_drawdowns = []

    rng = np.random.default_rng(42)
    for _ in range(n_sims):
        shuffled = rng.permutation(daily_rets)
        # Rebuild equity curve
        eq = initial_eq * np.cumprod(1 + shuffled)
        total_ret = (eq[-1] / initial_eq - 1) * 100
        final_returns.append(total_ret)
        # Sharpe
        mean_r = shuffled.mean()
        std_r = shuffled.std()
        sharpe = (mean_r / std_r) * np.sqrt(252) if std_r > 0 else 0
        sharpes.append(sharpe)
        # Max drawdown
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


def cost_sensitivity(dfs):
    """Double costs and check if still profitable."""
    # Normal costs
    ec_normal, _, tc_normal = run_backtest(dfs, commission_pct=0.0010, slippage_pct=0.0005)
    ret_normal = (ec_normal["equity"].iloc[-1] / 100000 - 1) * 100

    # Double costs
    ec_double, _, tc_double = run_backtest(dfs, commission_pct=0.0020, slippage_pct=0.0010)
    ret_double = (ec_double["equity"].iloc[-1] / 100000 - 1) * 100

    # Triple costs
    ec_triple, _, tc_triple = run_backtest(dfs, commission_pct=0.0030, slippage_pct=0.0015)
    ret_triple = (ec_triple["equity"].iloc[-1] / 100000 - 1) * 100

    return {
        "normal_costs": {
            "commission": 0.10,
            "slippage": 0.05,
            "return_pct": round(ret_normal, 2),
        },
        "double_costs": {
            "commission": 0.20,
            "slippage": 0.10,
            "return_pct": round(ret_double, 2),
        },
        "triple_costs": {
            "commission": 0.30,
            "slippage": 0.15,
            "return_pct": round(ret_triple, 2),
        },
        "still_profitable_at_2x": ret_double > 0,
        "still_profitable_at_3x": ret_triple > 0,
    }


def main():
    print("=" * 70)
    print("RAVI VAM v3 BACKTEST — DataBento Real Data (2020-2025)")
    print("=" * 70)
    print()

    # Load data
    print("Loading data...")
    dfs = load_data()
    for sym, df in dfs.items():
        print(f"  {sym}: {len(df)} bars, {df.index[0].date()} to {df.index[-1].date()}")
    print()

    # Run main backtest
    print("Running VAM v3 backtest...")
    ec, trade_log, trade_count = run_backtest(dfs)
    metrics = compute_metrics(ec, trade_count)

    print("\n" + "=" * 70)
    print("PERFORMANCE METRICS — VAM v3 (DataBento 2020-2025)")
    print("=" * 70)
    print(f"  Total Return:        {metrics['total_return_pct']:.2f}%")
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
    print(f"  Best Year:           {metrics['best_year_pct']:.2f}%")
    print(f"  Worst Year:          {metrics['worst_year_pct']:.2f}%")
    print(f"  Final Equity:        ${metrics['final_equity']:,.2f}")
    print()

    print("Yearly Returns:")
    for year, ret in sorted(metrics["yearly_returns"].items()):
        print(f"  {year}: {ret:+.2f}%")
    print()

    # Run benchmark
    print("Running SPY Buy & Hold benchmark...")
    bench = run_benchmark(dfs)
    bench_metrics = {
        "total_return_pct": round((bench["equity"].iloc[-1] / 100000 - 1) * 100, 2),
        "cagr_pct": round(
            ((bench["equity"].iloc[-1] / 100000) ** (1 / metrics["n_years"]) - 1) * 100,
            2,
        ),
        "final_equity": round(bench["equity"].iloc[-1], 2),
    }
    bench_daily = bench["equity"].pct_change().dropna()
    bench_metrics["sharpe_ratio"] = round(
        (bench_daily.mean() / bench_daily.std()) * np.sqrt(252), 4
    )
    bench_cummax = bench["equity"].cummax()
    bench_dd = ((bench["equity"] - bench_cummax) / bench_cummax).min() * 100
    bench_metrics["max_drawdown_pct"] = round(bench_dd, 2)

    print(f"\n  SPY B&H Total Return:  {bench_metrics['total_return_pct']:.2f}%")
    print(f"  SPY B&H CAGR:          {bench_metrics['cagr_pct']:.2f}%")
    print(f"  SPY B&H Sharpe:         {bench_metrics['sharpe_ratio']:.4f}")
    print(f"  SPY B&H Max DD:         {bench_metrics['max_drawdown_pct']:.2f}%")
    print(f"  SPY B&H Final Equity:   ${bench_metrics['final_equity']:,.2f}")
    print()

    # Regime analysis
    print("=" * 70)
    print("REGIME ANALYSIS")
    print("=" * 70)
    regime_stats = regime_analysis(ec)
    for regime, stats in sorted(regime_stats.items(), key=lambda x: -x[1]["days"]):
        print(
            f"  {regime:15s} | {stats['days']:4d} days ({stats['pct_of_time']:5.1f}%) | "
            f"avg daily: {stats['avg_daily_return_pct']:+.4f}% | Sharpe: {stats['sharpe']:+.2f}"
        )
    print()

    # Walk-forward analysis
    print("=" * 70)
    print("WALK-FORWARD ANALYSIS (5 folds, 60% train / 40% test)")
    print("=" * 70)
    wf_results = walk_forward_analysis(dfs)
    for fold in wf_results:
        print(
            f"  Fold {fold['fold']}: {fold['test_start']} to {fold['test_end']} | "
            f"{fold['test_days']} days | Return: {fold['return_pct']:+.2f}% | Sharpe: {fold['sharpe']:.2f}"
        )
    avg_wf_sharpe = np.mean([f["sharpe"] for f in wf_results]) if wf_results else 0
    print(f"  Average OOS Sharpe: {avg_wf_sharpe:.2f}")
    print()

    # Parameter sensitivity
    print("=" * 70)
    print("PARAMETER SENSITIVITY (SMA periods +/-20%)")
    print("=" * 70)
    param_results = parameter_sensitivity(dfs)
    base_ret = [r for r in param_results if r["variation"] == "base"][0]["total_return_pct"]
    for r in param_results:
        degradation = (
            ((r["total_return_pct"] - base_ret) / abs(base_ret) * 100) if base_ret != 0 else 0
        )
        marker = " *** >50% DEGRADATION ***" if degradation < -50 else ""
        print(
            f"  SMA({r['sma_short']}/{r['sma_long']}) [{r['variation']:>4s}]: "
            f"Return {r['total_return_pct']:+.2f}% | Sharpe {r['sharpe']:.2f} | "
            f"vs base: {degradation:+.1f}%{marker}"
        )
    print()

    # Monte Carlo
    print("=" * 70)
    print("MONTE CARLO SIMULATION (1000 shuffles)")
    print("=" * 70)
    mc_results = monte_carlo_simulation(ec)
    print(f"  Median Sharpe:         {mc_results['median_sharpe']:.4f}")
    print(f"  5th Percentile Sharpe: {mc_results['p5_sharpe']:.4f}")
    print(f"  95th Percentile Sharpe:{mc_results['p95_sharpe']:.4f}")
    print(f"  % Positive Sharpe:     {mc_results['pct_positive_sharpe']:.1f}%")
    print()

    # Cost sensitivity
    print("=" * 70)
    print("COST SENSITIVITY")
    print("=" * 70)
    cost_results = cost_sensitivity(dfs)
    print(f"  Normal costs (0.10% + 0.05%):  {cost_results['normal_costs']['return_pct']:+.2f}%")
    print(f"  Double costs (0.20% + 0.10%):  {cost_results['double_costs']['return_pct']:+.2f}%")
    print(f"  Triple costs (0.30% + 0.15%):  {cost_results['triple_costs']['return_pct']:+.2f}%")
    print(
        f"  Still profitable at 2x costs:  {'YES' if cost_results['still_profitable_at_2x'] else 'NO'}"
    )
    print(
        f"  Still profitable at 3x costs:  {'YES' if cost_results['still_profitable_at_3x'] else 'NO'}"
    )
    print()

    # Qualification summary
    print("=" * 70)
    print("QUALIFICATION SUMMARY")
    print("=" * 70)

    checks = {
        "Sharpe > 0.5": metrics["sharpe_ratio"] > 0.5,
        "Sortino > 0.7": metrics["sortino_ratio"] > 0.7,
        "Max DD < -50%": metrics["max_drawdown_pct"] > -50,
        "Calmar > 0.3": metrics["calmar_ratio"] > 0.3,
        "Profit Factor > 1.2": metrics["profit_factor"] > 1.2,
        "Walk-Forward avg Sharpe > 0": avg_wf_sharpe > 0,
        "Monte Carlo p5 Sharpe > 0": mc_results["p5_sharpe"] > 0,
        "No param >50% degradation": (
            all(
                ((r["total_return_pct"] - base_ret) / abs(base_ret) * 100) > -50
                for r in param_results
                if r["variation"] != "base"
            )
            if base_ret != 0
            else True
        ),
        "Profitable at 2x costs": cost_results["still_profitable_at_2x"],
        "Profitable at 3x costs": cost_results["still_profitable_at_3x"],
    }

    passed = sum(checks.values())
    total = len(checks)
    for check, result in checks.items():
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {check}")

    verdict = "PASS" if passed == total else "CONDITIONAL" if passed >= total * 0.7 else "FAIL"
    print(f"\n  VERDICT: {verdict} ({passed}/{total} checks passed)")
    print()

    # Save results
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results_file = RESULTS_DIR / "ravi_vam_v3_databento_results.json"

    full_results = {
        "metadata": {
            "client": "Ravi Mareedu & Sudhir Vyakaranam",
            "strategy": "VAM v3 (Optimized)",
            "data_source": "DataBento XNAS.ITCH",
            "backtest_period": f"{dfs['SPY'].index[0].date()} to {dfs['SPY'].index[-1].date()}",
            "initial_capital": 100000,
            "generated": datetime.now().isoformat(),
        },
        "performance": metrics,
        "benchmark": bench_metrics,
        "regime_analysis": regime_stats,
        "walk_forward": wf_results,
        "parameter_sensitivity": param_results,
        "monte_carlo": mc_results,
        "cost_sensitivity": cost_results,
        "qualification": {
            "checks": {k: v for k, v in checks.items()},
            "passed": passed,
            "total": total,
            "verdict": verdict,
        },
        "trade_log_sample": trade_log[:20],
    }

    with open(results_file, "w") as f:
        json.dump(full_results, f, indent=2, default=str)
    print(f"Results saved to: {results_file}")

    return full_results


if __name__ == "__main__":
    results = main()
