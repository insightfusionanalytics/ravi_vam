"""
Ravi VAM Strategy Sweep — Batch test strategies on Ravi's DataBento ETF universe.

Tests 5 built-in strategies + parameter variants across 8 ETFs.
Generates leaderboard sorted by Sharpe, filtered against qualification gates.

Usage:
    cd _engine/code
    python scripts/ravi_strategy_sweep.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

# Fix imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.loader import load_csv
from src.engine.backtester import Backtester, Side, StrategyBase
from src.indicators.registry import IndicatorRegistry

# ─── Configuration ───────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).parent.parent / "data" / "databento" / "equities"
RESULTS_DIR = Path(__file__).parent.parent / "clients" / "ravi_vam" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Ravi's ETF universe
ETFS = ["SPY", "QQQ", "TQQQ", "SOXL", "TLT", "GLD", "UPRO", "SHY"]

# Minimum thresholds (from qualification pipeline)
MIN_SHARPE = 0.5
MIN_TRADES = 10  # per full period (5 years)
MAX_DD = -30.0  # percent
MIN_WIN_RATE = 45.0

INITIAL_CAPITAL = 100_000
COMMISSION = 0.001  # 10 bps
SLIPPAGE = 0.0005  # 5 bps


# ─── Strategy Variants ──────────────────────────────────────────────────────


class IBS_Reversion(StrategyBase):
    """IBS (Internal Bar Strength) Mean Reversion.

    IBS = (close - low) / (high - low)
    Buy when IBS < threshold (hammered down), exit after N days or IBS > exit_threshold.
    ETFs revert strongly after extreme IBS days.
    """

    name = "IBS_Reversion"
    warmup_period = 5

    def __init__(
        self,
        entry_threshold: float = 0.1,
        exit_threshold: float = 0.8,
        hold_days: int = 3,
    ) -> None:
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        self.hold_days = hold_days
        self.bars_in_trade = 0

    def setup(self, data: pd.DataFrame) -> None:
        h = data["high"]
        l = data["low"]
        c = data["close"]
        self.ibs = (c - l) / (h - l + 1e-10)

    def generate_signal(self, bar_index: int, history: pd.DataFrame) -> int:
        ibs = self.ibs.iloc[bar_index]
        if pd.isna(ibs):
            return -999
        if ibs < self.entry_threshold:
            self.bars_in_trade = 0
            return 1
        if ibs > self.exit_threshold:
            return 0
        self.bars_in_trade += 1
        if self.bars_in_trade >= self.hold_days:
            return 0
        return -999


class RSI_Bounce(StrategyBase):
    """RSI Bounce with ATR-based risk management."""

    name = "RSI_Bounce"
    warmup_period = 20

    def __init__(
        self,
        period: int = 14,
        entry_level: float = 30,
        exit_level: float = 60,
        atr_sl_mult: float = 2.0,
    ) -> None:
        self.period = period
        self.entry_level = entry_level
        self.exit_level = exit_level
        self.atr_sl_mult = atr_sl_mult
        self.registry = IndicatorRegistry()
        self.rsi = pd.Series(dtype=float)
        self.atr = pd.Series(dtype=float)

    def setup(self, data: pd.DataFrame) -> None:
        self.rsi = self.registry.compute("RSI", data, period=self.period)
        self.atr = self.registry.compute("ATR", data, period=14)

    def generate_signal(self, bar_index: int, history: pd.DataFrame) -> int:
        rsi = self.rsi.iloc[bar_index]
        if pd.isna(rsi):
            return -999
        if rsi < self.entry_level:
            return 1
        if rsi > self.exit_level:
            return 0
        return -999

    def get_exit_params(
        self, bar_index: int, history: pd.DataFrame, side: Side
    ) -> tuple[float | None, float | None]:
        price = history["close"].iloc[-1]
        atr = self.atr.iloc[bar_index]
        if pd.isna(atr):
            return None, None
        sl = price - self.atr_sl_mult * atr
        tp = price + (self.atr_sl_mult * 1.5) * atr
        return sl, tp


class EMA_Momentum(StrategyBase):
    """EMA crossover with ADX filter — only trade when trend is strong."""

    name = "EMA_Momentum"
    warmup_period = 35

    def __init__(self, fast: int = 9, slow: int = 21, adx_threshold: float = 20) -> None:
        self.fast_period = fast
        self.slow_period = slow
        self.adx_threshold = adx_threshold
        self.registry = IndicatorRegistry()
        self.fast_ema = pd.Series(dtype=float)
        self.slow_ema = pd.Series(dtype=float)
        self.adx = pd.Series(dtype=float)

    def setup(self, data: pd.DataFrame) -> None:
        self.fast_ema = self.registry.compute("EMA", data, period=self.fast_period)
        self.slow_ema = self.registry.compute("EMA", data, period=self.slow_period)
        adx_data = self.registry.compute("ADX", data, period=14)
        self.adx = adx_data if isinstance(adx_data, pd.Series) else adx_data["adx"]

    def generate_signal(self, bar_index: int, history: pd.DataFrame) -> int:
        if bar_index < 1:
            return -999
        fast_now = self.fast_ema.iloc[bar_index]
        slow_now = self.slow_ema.iloc[bar_index]
        fast_prev = self.fast_ema.iloc[bar_index - 1]
        slow_prev = self.slow_ema.iloc[bar_index - 1]
        adx = self.adx.iloc[bar_index]

        if pd.isna(fast_now) or pd.isna(adx):
            return -999

        # Only trade in trending markets
        if adx < self.adx_threshold:
            return -999

        if fast_prev <= slow_prev and fast_now > slow_now:
            return 1
        if fast_prev >= slow_prev and fast_now < slow_now:
            return 0
        return -999


class MACD_Histogram(StrategyBase):
    """MACD Histogram momentum — enter on histogram turning positive, exit on negative."""

    name = "MACD_Histogram"
    warmup_period = 35

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9) -> None:
        self.fast = fast
        self.slow = slow
        self.signal_period = signal
        self.registry = IndicatorRegistry()
        self.macd_data = pd.DataFrame()

    def setup(self, data: pd.DataFrame) -> None:
        self.macd_data = self.registry.compute(
            "MACD", data, fast=self.fast, slow=self.slow, signal=self.signal_period
        )

    def generate_signal(self, bar_index: int, history: pd.DataFrame) -> int:
        if bar_index < 1:
            return -999
        hist_now = self.macd_data["macd_hist"].iloc[bar_index]
        hist_prev = self.macd_data["macd_hist"].iloc[bar_index - 1]
        if pd.isna(hist_now) or pd.isna(hist_prev):
            return -999
        # Histogram crosses zero
        if hist_prev <= 0 and hist_now > 0:
            return 1
        if hist_prev >= 0 and hist_now < 0:
            return 0
        return -999


class SMA_RSI_Combo(StrategyBase):
    """Trend + confirmation: buy when above SMA AND RSI dips below threshold."""

    name = "SMA_RSI_Combo"
    warmup_period = 55

    def __init__(
        self,
        sma_period: int = 200,
        rsi_period: int = 14,
        rsi_entry: float = 40,
        rsi_exit: float = 70,
    ) -> None:
        self.sma_period = sma_period
        self.rsi_period = rsi_period
        self.rsi_entry = rsi_entry
        self.rsi_exit = rsi_exit
        self.registry = IndicatorRegistry()
        self.sma = pd.Series(dtype=float)
        self.rsi = pd.Series(dtype=float)

    def setup(self, data: pd.DataFrame) -> None:
        self.sma = self.registry.compute("SMA", data, period=self.sma_period)
        self.rsi = self.registry.compute("RSI", data, period=self.rsi_period)

    def generate_signal(self, bar_index: int, history: pd.DataFrame) -> int:
        close = history["close"].iloc[-1]
        sma = self.sma.iloc[bar_index]
        rsi = self.rsi.iloc[bar_index]
        if pd.isna(sma) or pd.isna(rsi):
            return -999
        # Buy: price above SMA (uptrend) + RSI dip (pullback)
        if close > sma and rsi < self.rsi_entry:
            return 1
        # Sell: RSI overbought OR price breaks below SMA
        if rsi > self.rsi_exit or close < sma * 0.98:
            return 0
        return -999


# Import built-in strategies
from src.engine.strategies import (
    BollingerBreakout,
    MACD_Signal,
    RSI_MeanReversion,
    SMA_Crossover,
    TripleSuperTrend,
)


def build_strategy_variants() -> list[tuple[str, StrategyBase]]:
    """Generate all strategy variants to test."""
    variants = []

    # ── Built-in strategies (default params) ──
    variants.append(("SMA_20_50", SMA_Crossover(20, 50)))
    variants.append(("SMA_10_30", SMA_Crossover(10, 30)))
    variants.append(("SMA_50_200", SMA_Crossover(50, 200)))
    variants.append(("RSI_MeanRev_30_70", RSI_MeanReversion(14, 30, 70)))
    variants.append(("RSI_MeanRev_25_75", RSI_MeanReversion(14, 25, 75)))
    variants.append(("RSI_MeanRev_20_80", RSI_MeanReversion(14, 20, 80)))
    variants.append(("Bollinger_20_2", BollingerBreakout(20, 2)))
    variants.append(("Bollinger_20_1.5", BollingerBreakout(20, 1.5)))
    variants.append(("MACD_12_26_9", MACD_Signal(12, 26, 9)))
    variants.append(("MACD_8_21_5", MACD_Signal(8, 21, 5)))
    variants.append(("TripleSuperTrend", TripleSuperTrend()))

    # ── IBS Reversion variants ──
    for entry in [0.05, 0.10, 0.15, 0.20]:
        for hold in [1, 2, 3, 5]:
            name = f"IBS_{int(entry * 100)}pct_{hold}d"
            variants.append((name, IBS_Reversion(entry, 0.8, hold)))

    # ── RSI Bounce variants ──
    for entry_level in [25, 30, 35]:
        for exit_level in [55, 60, 65, 70]:
            for atr_mult in [1.5, 2.0, 2.5]:
                name = f"RSI_Bounce_{entry_level}_{exit_level}_ATR{atr_mult}"
                variants.append((name, RSI_Bounce(14, entry_level, exit_level, atr_mult)))

    # ── EMA Momentum variants ──
    for fast, slow in [(5, 13), (9, 21), (12, 26), (20, 50)]:
        for adx_thresh in [15, 20, 25]:
            name = f"EMA_{fast}_{slow}_ADX{adx_thresh}"
            variants.append((name, EMA_Momentum(fast, slow, adx_thresh)))

    # ── MACD Histogram variants ──
    for fast, slow, sig in [(8, 21, 5), (12, 26, 9), (5, 13, 8)]:
        name = f"MACD_Hist_{fast}_{slow}_{sig}"
        variants.append((name, MACD_Histogram(fast, slow, sig)))

    # ── SMA+RSI Combo variants ──
    for sma in [50, 100, 200]:
        for rsi_entry in [35, 40, 45]:
            for rsi_exit in [65, 70, 75]:
                name = f"SMA{sma}_RSI_{rsi_entry}_{rsi_exit}"
                variants.append((name, SMA_RSI_Combo(sma, 14, rsi_entry, rsi_exit)))

    return variants


def run_backtest(strategy: StrategyBase, data: pd.DataFrame) -> dict | None:
    """Run a single backtest, return metrics or None on failure."""
    try:
        engine = Backtester(
            data=data,
            initial_capital=INITIAL_CAPITAL,
            commission_pct=COMMISSION,
            slippage_pct=SLIPPAGE,
        )
        result = engine.run(strategy)
        return result.metrics
    except Exception as e:
        logger.warning(f"Backtest failed for {strategy.name}: {e}")
        return None


def passes_minimum_gates(metrics: dict) -> bool:
    """Quick check against minimum quality thresholds."""
    if not metrics:
        return False
    sharpe = metrics.get("sharpe", 0)
    trades = metrics.get("total_trades", 0)
    max_dd = metrics.get("max_drawdown_pct", -100)
    win_rate = metrics.get("win_rate", 0)

    return (
        sharpe >= MIN_SHARPE
        and trades >= MIN_TRADES
        and max_dd >= MAX_DD
        and win_rate >= MIN_WIN_RATE
    )


def main():
    logger.remove()
    logger.add(sys.stderr, level="WARNING")

    print("=" * 80)
    print("IFA RAVI VAM STRATEGY SWEEP")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 80)

    # Load all ETF data
    etf_data: dict[str, pd.DataFrame] = {}
    for etf in ETFS:
        filepath = DATA_DIR / f"{etf}_daily.csv"
        if filepath.exists():
            df = load_csv(str(filepath))
            etf_data[etf] = df
            print(f"  Loaded {etf}: {len(df)} bars ({df.index[0].date()} to {df.index[-1].date()})")
        else:
            print(f"  ⚠️ Missing: {filepath}")

    if not etf_data:
        print("No data loaded. Exiting.")
        return

    # Build strategy variants
    variants = build_strategy_variants()
    print(
        f"\n📊 Testing {len(variants)} strategy variants × {len(etf_data)} ETFs = {len(variants) * len(etf_data)} backtests"
    )
    print("-" * 80)

    # Run all backtests
    results = []
    total = len(variants) * len(etf_data)
    done = 0

    for var_name, strategy in variants:
        for etf_name, data in etf_data.items():
            done += 1
            if done % 50 == 0:
                print(f"  Progress: {done}/{total} ({done * 100 / total:.0f}%)")

            # Need to re-instantiate strategy for each ETF (clean state)
            # Clone by creating new instance with same params
            try:
                metrics = run_backtest(strategy, data)
            except Exception:
                metrics = None

            if metrics:
                # Correct key mappings from backtester output
                total_return = metrics.get("total_return_pct", 0)
                cagr = metrics.get("annual_return_pct", 0)  # backtester calls it annual_return_pct
                sharpe = metrics.get("sharpe", 0)
                sortino = metrics.get("sortino", 0)
                calmar = metrics.get("calmar", 0)
                max_dd = metrics.get("max_drawdown_pct", 0)
                win_rate = metrics.get("win_rate", 0)
                n_trades = metrics.get("total_trades", 0)
                pf = metrics.get("profit_factor", 0)
                avg_win = metrics.get("avg_win", 0)
                avg_loss = metrics.get("avg_loss", 0)
                avg_bars = metrics.get("avg_bars_held", 0)

                # Compute avg trade P&L %
                if n_trades > 0 and metrics.get("final_capital", 0) > 0:
                    avg_trade_pct = total_return / n_trades
                else:
                    avg_trade_pct = 0

                # ── IFA Score (composite ranking metric) ──
                # Weighted formula: higher = better strategy
                # Sharpe (30%) + Calmar (20%) + Profit Factor (15%) +
                # Win Rate (15%) + CAGR (10%) + Trade Frequency (10%)
                #
                # Each component is capped to avoid one metric dominating:
                s_sharpe = min(max(sharpe, 0), 3.0) / 3.0  # 0-1, capped at 3
                s_calmar = min(max(calmar, 0), 3.0) / 3.0  # 0-1, capped at 3
                s_pf = min(max(pf, 0), 10.0) / 10.0  # 0-1, capped at 10
                s_wr = max(win_rate - 40, 0) / 60.0  # 0-1, 40% = 0, 100% = 1
                s_cagr = min(max(cagr, 0), 50.0) / 50.0  # 0-1, capped at 50%
                s_trades = min(n_trades, 50) / 50.0  # 0-1, capped at 50 trades

                ifa_score = round(
                    s_sharpe * 0.30
                    + s_calmar * 0.20
                    + s_pf * 0.15
                    + s_wr * 0.15
                    + s_cagr * 0.10
                    + s_trades * 0.10,
                    4,
                )

                results.append(
                    {
                        "strategy": var_name,
                        "etf": etf_name,
                        "ifa_score": ifa_score,
                        "sharpe": round(sharpe, 4),
                        "total_return_pct": round(total_return, 2),
                        "cagr_pct": round(cagr, 2),
                        "max_drawdown_pct": round(max_dd, 2),
                        "calmar": round(calmar, 3),
                        "sortino": round(sortino, 3),
                        "win_rate": round(win_rate, 2),
                        "total_trades": n_trades,
                        "profit_factor": round(pf, 4),
                        "avg_trade_pct": round(avg_trade_pct, 2),
                        "avg_win": round(avg_win, 2),
                        "avg_loss": round(avg_loss, 2),
                        "avg_bars_held": round(avg_bars, 1),
                        "passes_gates": passes_minimum_gates(metrics),
                    }
                )

    # Build results DataFrame
    df_results = pd.DataFrame(results)

    if df_results.empty:
        print("\n❌ No results generated. Check data and strategies.")
        return

    # Sort by IFA Score (composite metric), then Sharpe as tiebreaker
    df_results = df_results.sort_values(["ifa_score", "sharpe"], ascending=[False, False])

    # Save full results
    csv_path = RESULTS_DIR / "strategy_sweep_full.csv"
    df_results.to_csv(csv_path, index=False)
    print(f"\n💾 Full results saved: {csv_path}")

    # Filter to passing strategies
    passing = df_results[df_results["passes_gates"]]

    print(f"\n{'=' * 80}")
    print("RESULTS SUMMARY")
    print(f"{'=' * 80}")
    print(f"Total backtests: {len(df_results)}")
    print(f"Passing minimum gates: {len(passing)}")
    print(f"Unique strategies passing: {passing['strategy'].nunique() if len(passing) > 0 else 0}")

    # Top 20 leaderboard
    print(f"\n{'=' * 80}")
    print("TOP 20 STRATEGY-ETF COMBOS (by IFA Score)")
    print(f"{'=' * 80}")
    top20 = df_results.head(20)
    for _, row in top20.iterrows():
        gate_icon = "✅" if row["passes_gates"] else "❌"
        print(
            f"  {gate_icon} {row['strategy']:30s} | {row['etf']:5s} | "
            f"IFA={row['ifa_score']:.3f} | Sharpe={row['sharpe']:5.2f} | "
            f"CAGR={row['cagr_pct']:5.1f}% | DD={row['max_drawdown_pct']:5.1f}% | "
            f"PF={row['profit_factor']:5.2f} | Trades={row['total_trades']:3d} | "
            f"WR={row['win_rate']:5.1f}%"
        )

    # Best per ETF
    print(f"\n{'=' * 80}")
    print("BEST STRATEGY PER ETF")
    print(f"{'=' * 80}")
    for etf in ETFS:
        etf_results = df_results[df_results["etf"] == etf]
        if len(etf_results) > 0:
            best = etf_results.iloc[0]
            gate_icon = "✅" if best["passes_gates"] else "❌"
            print(
                f"  {gate_icon} {etf:5s}: {best['strategy']:30s} | "
                f"Sharpe={best['sharpe']:6.2f} | Return={best['total_return_pct']:7.1f}% | "
                f"DD={best['max_drawdown_pct']:6.1f}%"
            )

    # v5b comparison
    print(f"\n{'=' * 80}")
    print("COMPARISON VS RAVI v5b BASELINE")
    print(f"{'=' * 80}")
    print("  v5b baseline: Sharpe=0.74, Return=+70.2%, DD=-25.3%, Trades=5")
    better_sharpe = df_results[df_results["sharpe"] > 0.74]
    print(f"  Strategies beating v5b Sharpe (0.74): {len(better_sharpe)}")
    if len(better_sharpe) > 0:
        print(
            f"  Best Sharpe found: {better_sharpe.iloc[0]['sharpe']:.2f} ({better_sharpe.iloc[0]['strategy']} on {better_sharpe.iloc[0]['etf']})"
        )

    # Save passing strategies as JSON for further analysis
    if len(passing) > 0:
        passing_json = passing.to_dict(orient="records")
        json_path = RESULTS_DIR / "strategy_sweep_passing.json"
        with open(json_path, "w") as f:
            json.dump(passing_json, f, indent=2)
        print(f"\n💾 Passing strategies saved: {json_path}")

    # Save summary
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_backtests": len(df_results),
        "total_passing": len(passing),
        "unique_strategies_tested": df_results["strategy"].nunique(),
        "unique_strategies_passing": (passing["strategy"].nunique() if len(passing) > 0 else 0),
        "etfs_tested": list(etf_data.keys()),
        "v5b_baseline": {
            "sharpe": 0.74,
            "return_pct": 70.2,
            "max_dd": -25.3,
            "trades": 5,
        },
        "top_5": (
            df_results.head(5).to_dict(orient="records")
            if len(df_results) >= 5
            else df_results.to_dict(orient="records")
        ),
        "gates": {
            "min_sharpe": MIN_SHARPE,
            "min_trades": MIN_TRADES,
            "max_drawdown": MAX_DD,
            "min_win_rate": MIN_WIN_RATE,
        },
    }
    summary_path = RESULTS_DIR / "strategy_sweep_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"💾 Summary saved: {summary_path}")

    print(f"\n{'=' * 80}")
    print(f"Completed: {datetime.now().isoformat()}")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
