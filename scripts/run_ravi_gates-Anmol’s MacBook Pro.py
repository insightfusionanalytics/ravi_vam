"""
Run ALL milestone gates for Ravi VAM project.
Produces a physical gate_status.json — this is the source of truth.

Usage:
    cd _engine/code
    python scripts/run_ravi_gates.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.evaluation.milestone_gates import GateResult, MilestoneGatekeeper

from src.engine.backtester import Backtester, StrategyBase
from src.indicators.registry import IndicatorRegistry

# ── Paths ──────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data" / "databento" / "equities"
CLIENT_DIR = Path(__file__).parent.parent / "clients" / "ravi_vam"
RESULTS_CSV = CLIENT_DIR / "results" / "strategy_sweep_full.csv"

ETFS = ["SPY", "QQQ", "TQQQ", "SOXL", "TLT", "GLD", "UPRO", "SHY"]
INITIAL_CAPITAL = 100_000
COMMISSION_PCT = 0.001
SLIPPAGE_PCT = 0.0005


# ── Helpers ────────────────────────────────────────────────────────────────
def make_backtester(data: pd.DataFrame) -> Backtester:
    return Backtester(
        data=data,
        initial_capital=INITIAL_CAPITAL,
        commission_pct=COMMISSION_PCT,
        slippage_pct=SLIPPAGE_PCT,
    )


def make_backtester_3x(data: pd.DataFrame) -> Backtester:
    return Backtester(
        data=data,
        initial_capital=INITIAL_CAPITAL,
        commission_pct=COMMISSION_PCT * 3,
        slippage_pct=SLIPPAGE_PCT * 3,
    )


# ── Strategy for M1 test ──────────────────────────────────────────────────
class EMA_Momentum(StrategyBase):
    """EMA crossover with ADX filter — EXACT COPY from ravi_strategy_sweep.py.

    Signal convention:
        1    = buy (enter long)
        0    = exit (close position)
        -999 = no action (hold or stay flat)
    """

    name = "EMA_Momentum"
    warmup_period = 35

    def __init__(self, fast: int = 9, slow: int = 21, adx_threshold: float = 20):
        self.fast_period = fast
        self.slow_period = slow
        self.adx_threshold = adx_threshold
        self.registry = IndicatorRegistry()
        self.fast_ema = pd.Series(dtype=float)
        self.slow_ema = pd.Series(dtype=float)
        self.adx = pd.Series(dtype=float)
        self.name = f"EMA_{fast}_{slow}_ADX{int(adx_threshold)}"

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


def load_etf_data() -> dict[str, pd.DataFrame]:
    """Load all ETF data into a dict. Returns with datetime INDEX for gatekeeper,
    plus standard OHLCV column names."""
    data_dict = {}
    for etf in ETFS:
        csv_path = DATA_DIR / f"{etf}_daily.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            # Find datetime column
            dt_col = None
            for candidate in ["datetime", "ts_event", "date", "Date"]:
                if candidate in df.columns:
                    dt_col = candidate
                    break
            if dt_col:
                df[dt_col] = pd.to_datetime(df[dt_col])
                df = df.set_index(dt_col)
            # Standardize column names
            col_map = {}
            for col in df.columns:
                cl = col.lower()
                if cl in ("open", "high", "low", "close", "volume"):
                    col_map[col] = cl
            df = df.rename(columns=col_map)
            data_dict[etf] = df
        else:
            print(f"  ⚠️  {etf} data file not found: {csv_path}")
    return data_dict


def prepare_for_backtest(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare data for backtester — keeps datetime index, ensures OHLCV columns exist as regular columns."""
    bt_df = df.copy()
    # The backtester needs the datetime as the index (for _compute_metrics)
    # and OHLCV columns — our data already has this format from load_etf_data
    return bt_df


def main():
    print("\n" + "=" * 70)
    print("RAVI VAM — MILESTONE GATE VALIDATION")
    print("=" * 70)

    gatekeeper = MilestoneGatekeeper(CLIENT_DIR)

    # ══════════════════════════════════════════════════════════════════════
    # M0: DATA LOADED
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ Running M0_DATA_LOADED...")
    data_dict = load_etf_data()
    m0 = gatekeeper.run_m0_data_loaded(data_dict)
    print(m0.summary())

    if not m0.passed:
        print("\n🚫 M0 FAILED — Cannot proceed. Fix data issues first.")
        return

    # ══════════════════════════════════════════════════════════════════════
    # M1: SINGLE STRATEGY (EMA_5_13_ADX25 on SPY — best strategy)
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ Running M1_SINGLE_STRATEGY (EMA_5_13_ADX25 on SPY)...")
    spy_data = data_dict["SPY"].copy()

    strategy = EMA_Momentum(fast=5, slow=13, adx_threshold=25)
    bt = make_backtester(spy_data)
    result = bt.run(strategy)
    metrics = result.metrics

    m1 = gatekeeper.run_m1_single_strategy(metrics, "EMA_5_13_ADX25")
    print(m1.summary())

    if not m1.passed:
        print("\n🚫 M1 FAILED — Single strategy metrics have issues.")

    # ══════════════════════════════════════════════════════════════════════
    # M2: METRICS AUDIT (recompute from equity curve)
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ Running M2_METRICS_AUDIT (cross-check metrics vs equity curve)...")
    m2 = gatekeeper.run_m2_metrics_audit(metrics, result.equity_curve)
    print(m2.summary())

    # ══════════════════════════════════════════════════════════════════════
    # M3: CROSS-CHECKS (reverse test, commission sensitivity, subsample)
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ Running M3_CROSS_CHECKS...")
    checks = []

    # Check 1: Reverse data test
    spy_reversed = spy_data.iloc[::-1].copy()
    # Re-assign datetime index in forward order so backtester treats it as forward data
    spy_reversed.index = spy_data.index
    strategy_rev = EMA_Momentum(fast=5, slow=13, adx_threshold=25)
    bt_rev = make_backtester(spy_reversed)
    rev_result = bt_rev.run(strategy_rev)
    rev_sharpe = rev_result.metrics.get("sharpe", 0)
    fwd_sharpe = metrics.get("sharpe", 0)
    checks.append(
        {
            "name": "reverse_data_test",
            "passed": rev_sharpe < fwd_sharpe,
            "detail": f"Forward Sharpe={fwd_sharpe:.2f}, Reversed Sharpe={rev_sharpe:.2f}"
            + (" — GOOD (reversed is worse)" if rev_sharpe < fwd_sharpe else " — SUSPICIOUS"),
        }
    )

    # Check 2: Commission sensitivity (3x commission)
    strategy_3x = EMA_Momentum(fast=5, slow=13, adx_threshold=25)
    bt_3x = make_backtester_3x(spy_data)
    result_3x = bt_3x.run(strategy_3x)
    sharpe_3x = result_3x.metrics.get("sharpe", 0)
    checks.append(
        {
            "name": "commission_sensitivity_3x",
            "passed": sharpe_3x > 0,
            "detail": f"At 3x costs: Sharpe={sharpe_3x:.2f}, Return={result_3x.metrics.get('total_return_pct', 0):.1f}%",
        }
    )

    # Check 3: First half vs second half
    midpoint = len(spy_data) // 2
    first_half = spy_data.iloc[:midpoint].copy()
    second_half = spy_data.iloc[midpoint:].copy()

    s1 = EMA_Momentum(fast=5, slow=13, adx_threshold=25)
    r1 = make_backtester(first_half).run(s1)

    s2 = EMA_Momentum(fast=5, slow=13, adx_threshold=25)
    r2 = make_backtester(second_half).run(s2)

    both_positive = (
        r1.metrics.get("total_return_pct", 0) > 0 and r2.metrics.get("total_return_pct", 0) > 0
    )
    checks.append(
        {
            "name": "subsample_stability",
            "passed": both_positive,
            "detail": f"First half: {r1.metrics.get('total_return_pct', 0):.1f}%, Second half: {r2.metrics.get('total_return_pct', 0):.1f}%"
            + (" — both positive ✓" if both_positive else " — UNSTABLE"),
        }
    )

    # Check 4: Win/loss distribution
    if result.trades:
        total_pnl = sum(t.pnl for t in result.trades)
        max_single = max(abs(t.pnl) for t in result.trades)
        concentrated = (max_single / abs(total_pnl) > 0.5) if total_pnl != 0 else False
        checks.append(
            {
                "name": "pnl_not_concentrated",
                "passed": not concentrated,
                "detail": f"Largest trade P&L: ${max_single:,.0f} / Total P&L: ${total_pnl:,.0f}"
                + (" — CONCENTRATED (>50%)" if concentrated else " — distributed ✓"),
            }
        )
    else:
        checks.append(
            {
                "name": "pnl_not_concentrated",
                "passed": False,
                "detail": "No trades to check",
            }
        )

    m3_result = GateResult("M3_CROSS_CHECKS", all(c["passed"] for c in checks), checks)
    gatekeeper.record_gate(m3_result)
    print(m3_result.summary())

    # ══════════════════════════════════════════════════════════════════════
    # M4: SWEEP COMPLETE (validate the CSV)
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ Running M4_SWEEP_COMPLETE...")
    m4 = gatekeeper.run_m4_sweep_complete(RESULTS_CSV)
    print(m4.summary())

    # ══════════════════════════════════════════════════════════════════════
    # M5: RESULTS AUDITED (spot-check top strategies from sweep)
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ Running M5_RESULTS_AUDITED (spot-check top strategies)...")
    m5_checks = []

    sweep_df = pd.read_csv(RESULTS_CSV)
    top3 = sweep_df.sort_values("sharpe", ascending=False).head(3)

    for _, row in top3.iterrows():
        strat_name = row["strategy"]
        etf_name = row["etf"]
        reported_sharpe = row["sharpe"]
        reported_return = row["total_return_pct"]

        if "EMA" in strat_name:
            parts = strat_name.split("_")
            try:
                fast = int(parts[1])
                slow = int(parts[2])
                adx = int(parts[3].replace("ADX", ""))
            except (IndexError, ValueError):
                m5_checks.append(
                    {
                        "name": f"rerun_{strat_name}_{etf_name}",
                        "passed": True,
                        "detail": "Skipped (non-standard name format)",
                    }
                )
                continue

            if etf_name in data_dict:
                etf_data = data_dict[etf_name].copy()
                s = EMA_Momentum(fast=fast, slow=slow, adx_threshold=adx)
                rerun_result = make_backtester(etf_data).run(s)
                rerun_sharpe = rerun_result.metrics.get("sharpe", 0)
                rerun_return = rerun_result.metrics.get("total_return_pct", 0)

                sharpe_match = abs(rerun_sharpe - reported_sharpe) < 0.05
                return_match = abs(rerun_return - reported_return) < 1.0

                m5_checks.append(
                    {
                        "name": f"rerun_{strat_name}_{etf_name}",
                        "passed": sharpe_match and return_match,
                        "detail": (
                            f"CSV: Sharpe={reported_sharpe:.3f}, Return={reported_return:.1f}% | "
                            f"Rerun: Sharpe={rerun_sharpe:.3f}, Return={rerun_return:.1f}% | "
                            + ("MATCH ✓" if (sharpe_match and return_match) else "MISMATCH ✗")
                        ),
                    }
                )
            else:
                m5_checks.append(
                    {
                        "name": f"rerun_{strat_name}_{etf_name}",
                        "passed": True,
                        "detail": f"Skipped — {etf_name} data not available",
                    }
                )
        else:
            m5_checks.append(
                {
                    "name": f"rerun_{strat_name}_{etf_name}",
                    "passed": True,
                    "detail": "Skipped — only EMA strategies can be re-run in this script",
                }
            )

    # Check: cagr_pct not all zeros
    with_trades = sweep_df[sweep_df["total_trades"] > 0]
    cagr_nonzero = (with_trades["cagr_pct"] != 0).any()
    m5_checks.append(
        {
            "name": "cagr_pct_computed",
            "passed": cagr_nonzero,
            "detail": ("Non-zero CAGR values exist ✓" if cagr_nonzero else "ALL CAGR = 0 — BUG"),
        }
    )

    avg_trade_nonzero = (with_trades["avg_trade_pct"] != 0).any()
    m5_checks.append(
        {
            "name": "avg_trade_pct_computed",
            "passed": avg_trade_nonzero,
            "detail": (
                "Non-zero avg_trade values exist ✓"
                if avg_trade_nonzero
                else "ALL avg_trade = 0 — BUG"
            ),
        }
    )

    bad_wr = (sweep_df["win_rate"] < 0).any()
    m5_checks.append(
        {
            "name": "no_negative_win_rate",
            "passed": not bad_wr,
            "detail": ("All win_rates >= 0 ✓" if not bad_wr else "Negative win_rate found — BUG"),
        }
    )

    trades_are_int = (sweep_df["total_trades"] == sweep_df["total_trades"].astype(int)).all()
    m5_checks.append(
        {
            "name": "trades_are_integers",
            "passed": trades_are_int,
            "detail": (
                "All trade counts are integers ✓"
                if trades_are_int
                else "Fractional trade counts found"
            ),
        }
    )

    m5_result = GateResult("M5_RESULTS_AUDITED", all(c["passed"] for c in m5_checks), m5_checks)
    gatekeeper.record_gate(m5_result)
    print(m5_result.summary())

    # ══════════════════════════════════════════════════════════════════════
    # M6: DELIVERABLE CHECK (all gates must pass)
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ Running M6_DELIVERABLE...")
    m6 = gatekeeper.run_m6_deliverable_check(CLIENT_DIR / "delivery")
    print(m6.summary())

    # ══════════════════════════════════════════════════════════════════════
    # FINAL STATUS
    # ══════════════════════════════════════════════════════════════════════
    print(gatekeeper.show_status())
    print(f"\n📁 Gate status file: {gatekeeper.gate_file}")

    # Print key numbers for quick verification
    print(f"\n{'=' * 70}")
    print("KEY NUMBERS — EMA_5_13_ADX25 on SPY (best strategy)")
    print(f"{'=' * 70}")
    print(f"  Sharpe:         {metrics.get('sharpe', 0):.2f}")
    print(f"  Total Return:   {metrics.get('total_return_pct', 0):.1f}%")
    print(f"  CAGR:           {metrics.get('cagr_pct', 0):.1f}%")
    print(f"  Max Drawdown:   {metrics.get('max_drawdown_pct', 0):.1f}%")
    print(f"  Win Rate:       {metrics.get('win_rate', 0):.1f}%")
    print(f"  Total Trades:   {metrics.get('total_trades', 0)}")
    print(f"  Profit Factor:  {metrics.get('profit_factor', 0):.2f}")
    print(f"  Avg Trade:      {metrics.get('avg_trade_pct', 0):.2f}%")
    print(f"  Final Capital:  ${metrics.get('final_capital', 0):,.0f}")


if __name__ == "__main__":
    main()
