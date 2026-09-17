"""
VAM Step 4 — Safety Valve (SVIX) Backtest on DataBento Production Data
Client: Ravi Mareedu & Sudhir Vyakaranam

Strategy 3: Safety Valve (Recovery Booster)
- Activates ONLY when Strategy 1 (VAM Split) state = CASH
- Initial entry: 10% of tactical cash into SVIX when:
    * VIX in 30-40 range
    * VIX curving DOWN: VIX[t] < VIX[t-1] for 2 consecutive days
- Panic entry: Add more to reach 30% total if VIX spikes to 50+
- Buffer rule: Always keep >= 70% of tactical cash in SGOV
- Exit: Sell ALL SVIX when VIX drops below 20

Architecture:
- Reads Step 2 daily portfolio values to determine CASH periods
- Does NOT re-run Steps 1, 2, or 3 — imports their outputs
- Same T+1 pending_trade pattern as Steps 2 and 3
- SVIX only exists from 2022-03-30 — strategy inactive before that date

!! DO NOT USE YAHOO FINANCE DATA FOR CLIENT DELIVERY !!
DataBento provides the authoritative production data for all client backtests.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

ENGINE_ROOT = Path(__file__).resolve().parent.parent  # repo root (was parents[3] in original monorepo layout)  # _engine/code/
DATA_DIR = ENGINE_ROOT / "data"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# =============================================================================
# CONSTANTS — Domain Lock: all critical dates/thresholds hardcoded, not inferred
# =============================================================================

# SVIX launched on 2022-02-22 but DataBento data starts 2022-03-30.
# Use actual data start as the guard date — hardcoded per Domain Lock rule.
SVIX_LAUNCH_DATE: str = "2022-03-30"

# =============================================================================
# === STRATEGY 3: ENTIRE SPEC IS IFA ASSUMPTION — NOT CONFIRMED BY CLIENT ===
# Ravi mentioned "two short strategies" in mar1_meeting.srt 00:24:12-23 but
# only discussed SPXU in detail. SVIX was never mentioned in any transcript.
# ALL parameters below must be confirmed with Ravi before go-live.
# =============================================================================
SVIX_ENTRY_VIX_LOW: float = 30.0    # Flag: VIX lower bound for initial SVIX entry.
SVIX_ENTRY_VIX_HIGH: float = 40.0   # Flag: VIX upper bound for initial SVIX entry.
SVIX_PANIC_VIX: float = 50.0        # Flag: VIX level triggering panic escalation.
SVIX_INITIAL_ALLOC: float = 0.10    # Flag: Initial SVIX allocation (10% of tactical cash).
SVIX_PANIC_ALLOC: float = 0.30      # Flag: Panic SVIX allocation (30% total if VIX >= 50).
SVIX_SGOV_BUFFER: float = 0.70      # Flag: Minimum % to keep in cash/SGOV at all times.
SVIX_EXIT_VIX: float = 20.0              # Flag: VIX level to exit all SVIX positions.
USE_SGOV = True                      # Flag 8: Not mentioned. IFA default: idle cash in SGOV.
COMMISSION_MODEL = "IBKR"           # Flag 10: Not discussed. Using IBKR tiered.

# Internal constants
VIX_CURVE_DAYS: int = 2         # Consecutive falling VIX days required (Domain Lock)
SVIX_ALLOC_CAP: float = 0.99    # Hard allocation cap (never 100% of capital)
SLIPPAGE_BPS_SVIX: float = 20.0  # 20 bps — SVIX entry is always in elevated-VIX environment
INITIAL_CAPITAL: float = 100_000.0
RISK_FREE_RATE: float = 0.04     # 4% annual (same as Steps 2 and 3)


# =============================================================================
# DATA LOADING
# =============================================================================


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    """IBKR tiered commission: $0.005/share, min $1, max 1% of trade value."""
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def load_databento_csv(filepath: Path) -> pd.DataFrame:
    """Load a DataBento daily CSV. Strips UTC timezone if present."""
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def load_step4_svix_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load all data required for Step 4 SVIX strategy.

    Returns:
        df: Merged daily DataFrame (SPY date index, SVIX prices, VIX + indicators).
        step2_daily: Step 2 portfolio values with state column.
    """
    spy = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPY_daily.csv")
    svix = load_databento_csv(DATA_DIR / "databento" / "equities" / "SVIX_daily.csv")
    vix = load_databento_csv(DATA_DIR / "cboe" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SVIX_Close"] = svix["close"].reindex(spy.index)
    df["SVIX_Open"] = svix["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df = df.ffill().dropna(subset=["VIX"])

    # Pre-compute "VIX curving down" signal (Domain Lock: exactly 2 consecutive days)
    df["vix_fell"] = df["VIX"].diff() < 0
    df["vix_curve_down"] = df["vix_fell"] & df["vix_fell"].shift(1).fillna(False)

    # Load Step 2 daily portfolio values (source of truth for CASH periods)
    step2_daily = pd.read_csv(
        RESULTS_DIR / "step2_databento_portfolio_values.csv",
        parse_dates=["date"],
        index_col="date",
    )
    return df, step2_daily


def get_step2_state(date: pd.Timestamp, step2_daily: pd.DataFrame) -> str:
    """Look up Step 2's state on the given date."""
    date_key = date.normalize()
    if date_key in step2_daily.index:
        return str(step2_daily.loc[date_key, "state"])
    return "UNKNOWN"


def is_svix_available(date: pd.Timestamp) -> bool:
    """Return True if SVIX data is available on this date (after launch)."""
    return date >= pd.Timestamp(SVIX_LAUNCH_DATE)


# =============================================================================
# BACKTEST ENGINE
# =============================================================================


def _execute_buy_init(
    date: pd.Timestamp,
    signal_date: str,
    cash: float,
    svix_shares: float,
    svix_exec: float,
    svix_price: float,
    reason: str,
    trades: list[dict],
) -> tuple[float, float, str]:
    """Execute SVIX initial BUY (10% allocation). Returns (new_cash, new_shares, mode)."""
    pv = cash + svix_shares * svix_exec
    invest = min(pv * SVIX_INITIAL_ALLOC, pv * (1.0 - SVIX_SGOV_BUFFER))
    slip = invest * (SLIPPAGE_BPS_SVIX / 10_000)
    comm = ibkr_commission(invest, svix_exec)
    new_shares = svix_shares + invest / svix_exec
    new_cash = pv - new_shares * svix_exec - slip - comm

    trades.append(_build_trade_record(
        len(trades) + 1, signal_date, date, "BUY_INIT", invest,
        svix_shares, new_shares, cash, new_cash,
        svix_exec, svix_price, pv, new_cash + new_shares * svix_price,
        slip, SVIX_INITIAL_ALLOC, reason, comm,
    ))
    return new_cash, new_shares, "INIT"


def _execute_buy_panic(
    date: pd.Timestamp,
    signal_date: str,
    cash: float,
    svix_shares: float,
    svix_exec: float,
    svix_price: float,
    reason: str,
    trades: list[dict],
) -> tuple[float, float, str]:
    """Execute SVIX panic BUY to reach 30% total. Returns (new_cash, new_shares, mode)."""
    pv = cash + svix_shares * svix_exec
    current_svix_alloc = (svix_shares * svix_exec) / pv if pv > 0 else 0.0
    target_alloc = min(SVIX_PANIC_ALLOC, 1.0 - SVIX_SGOV_BUFFER)
    add_alloc = max(0.0, target_alloc - current_svix_alloc)
    invest = pv * add_alloc
    if invest <= 0:
        return cash, svix_shares, "PANIC"  # Already at or above target

    slip = invest * (SLIPPAGE_BPS_SVIX / 10_000)
    comm = ibkr_commission(invest, svix_exec)
    old_shares = svix_shares
    new_shares = svix_shares + invest / svix_exec
    new_cash = cash - invest - slip - comm

    trades.append(_build_trade_record(
        len(trades) + 1, signal_date, date, "BUY_PANIC", invest,
        old_shares, new_shares, cash, new_cash,
        svix_exec, svix_price, pv, new_cash + new_shares * svix_price,
        slip, target_alloc, reason, comm,
    ))
    return new_cash, new_shares, "PANIC"


def _execute_sell_all(
    date: pd.Timestamp,
    signal_date: str,
    cash: float,
    svix_shares: float,
    svix_exec: float,
    svix_price: float,
    reason: str,
    trades: list[dict],
) -> tuple[float, float, str]:
    """Execute SVIX full SELL. Returns (new_cash, new_shares=0, mode='NONE')."""
    if svix_shares <= 0:
        return cash, 0.0, "NONE"

    sell_val = svix_shares * svix_exec
    slip = sell_val * (SLIPPAGE_BPS_SVIX / 10_000)
    comm = ibkr_commission(sell_val, svix_exec)
    pv = cash + svix_shares * svix_exec
    new_cash = cash + sell_val - slip - comm

    trades.append(_build_trade_record(
        len(trades) + 1, signal_date, date, "SELL_ALL", sell_val,
        svix_shares, 0.0, cash, new_cash,
        svix_exec, svix_price, pv, new_cash,
        slip, 0.0, reason, comm,
    ))
    return new_cash, 0.0, "NONE"


def _build_trade_record(
    trade_num: int, signal_date: str, exec_date: pd.Timestamp,
    action: str, trade_value: float,
    shares_before: float, shares_after: float,
    cash_before: float, cash_after: float,
    exec_price: float, close_price: float,
    pv_before: float, pv_after: float,
    slip: float, target_alloc: float, reason: str, comm: float,
) -> dict:
    """Build a standardised trade record dict."""
    return {
        "trade_number": trade_num,
        "signal_date": signal_date,
        "execution_date": exec_date.strftime("%Y-%m-%d"),
        "execution_timing": "T+1 (signal at prev close, execute at today open)",
        "action": action,
        "instrument": "SVIX",
        "trigger_reason": reason,
        "target_allocation_pct": round(target_alloc * 100, 1),
        "exec_price": round(exec_price, 4),
        "close_same_day": round(close_price, 4),
        "shares_before": round(shares_before, 4),
        "shares_after": round(shares_after, 4),
        "shares_delta": round(shares_after - shares_before, 4),
        "cash_before": round(cash_before, 2),
        "cash_after": round(cash_after, 2),
        "trade_value_dollars": round(trade_value, 2),
        "slippage_bps_used": SLIPPAGE_BPS_SVIX,
        "slippage_dollars": round(slip, 2),
        "commission_dollars": round(comm, 2),
        "total_cost_dollars": round(slip + comm, 2),
        "portfolio_value_before_trade": round(pv_before, 2),
        "portfolio_value_at_close": round(pv_after, 2),
    }


def run_step4_svix_backtest(
    df: pd.DataFrame, step2_daily: pd.DataFrame
) -> tuple[list[dict], list[dict], dict]:
    """Run the Safety Valve (SVIX) standalone backtest.

    Operates only during CASH periods from Step 2.
    T+1 execution: signal at today's close, executed at next open.
    Initial entry: 10% when VIX in 30-40 and curving down for 2 days.
    Panic entry: 30% total when VIX >= 50 (add 20% more).
    Exit: VIX < 20 or Step 2 exits CASH.
    """
    cash: float = INITIAL_CAPITAL
    svix_shares: float = 0.0
    svix_mode: str = "NONE"   # NONE / INIT / PANIC
    pending_trade: tuple[str, str] | None = None
    trades: list[dict] = []
    daily_log: list[dict] = []
    prev_step2_state: str = "UNKNOWN"

    valid_rows = df.dropna(subset=["VIX"])

    for date, row in valid_rows.iterrows():
        vix = row["VIX"]
        svix_close = row["SVIX_Close"]
        svix_open = row["SVIX_Open"]
        vix_curve_down = bool(row["vix_curve_down"])
        svix_avail = is_svix_available(date) and not pd.isna(svix_close) and svix_close > 0
        svix_exec = svix_open if svix_avail and not pd.isna(svix_open) and svix_open > 0 else svix_close
        step2_state = get_step2_state(date, step2_daily)
        currently_cash = step2_state == "CASH"
        in_svix = svix_shares > 0

        # Force-exit SVIX if Step 2 left CASH while we hold positions
        if not currently_cash and in_svix and pending_trade is None:
            pending_trade = ("SELL_ALL", "FORCED_EXIT: Step 2 exited CASH state")

        # Execute pending trade from yesterday's signal at today's open
        if pending_trade is not None and svix_avail:
            action, reason = pending_trade
            pending_trade = None
            sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

            if action == "BUY_INIT":
                cash, svix_shares, svix_mode = _execute_buy_init(
                    date, sig_date, cash, svix_shares, svix_exec, svix_close, reason, trades
                )
            elif action == "BUY_PANIC":
                cash, svix_shares, svix_mode = _execute_buy_panic(
                    date, sig_date, cash, svix_shares, svix_exec, svix_close, reason, trades
                )
            elif action == "SELL_ALL":
                cash, svix_shares, svix_mode = _execute_sell_all(
                    date, sig_date, cash, svix_shares, svix_exec, svix_close, reason, trades
                )
        elif pending_trade is not None and not svix_avail:
            pending_trade = None  # Drop trade — SVIX not available yet

        in_svix = svix_shares > 0

        # Generate signal for tomorrow based on today's close
        if currently_cash and svix_avail:
            if not in_svix:
                # Initial entry: VIX in 30-40 AND curving down for 2 consecutive days
                if SVIX_ENTRY_VIX_LOW <= vix <= SVIX_ENTRY_VIX_HIGH and vix_curve_down:
                    pending_trade = (
                        "BUY_INIT",
                        f"SVIX_INIT: VIX={vix:.1f} in [30,40], falling 2 days",
                    )
            else:
                # Exit check first (higher priority than panic escalation)
                if vix < SVIX_EXIT_VIX:
                    pending_trade = ("SELL_ALL", f"EXIT: VIX={vix:.1f}<{SVIX_EXIT_VIX}")
                elif svix_mode == "INIT" and vix >= SVIX_PANIC_VIX:
                    # Panic escalation: add more to reach 30% total
                    pending_trade = (
                        "BUY_PANIC",
                        f"SVIX_PANIC: VIX={vix:.1f}>={SVIX_PANIC_VIX} escalating to 30%",
                    )

        # Portfolio snapshot at close
        svix_value = svix_shares * svix_close if svix_avail else svix_shares * 0
        portfolio_value = cash + svix_value
        svix_alloc_pct = (svix_value / portfolio_value * 100) if portfolio_value > 0 else 0.0

        if in_svix and svix_mode == "PANIC":
            step4_state = "CASH_SVIX_PANIC"
        elif in_svix:
            step4_state = "CASH_SVIX_INIT"
        elif currently_cash:
            step4_state = "CASH_IDLE"
        else:
            step4_state = "INACTIVE"

        daily_log.append({
            "date": date.strftime("%Y-%m-%d"),
            "day_number": len(daily_log) + 1,
            "step2_state": step2_state,
            "step4_active": "YES" if currently_cash else "NO",
            "svix_available": "YES" if svix_avail else "NO",
            "step4_state": step4_state,
            "pending_signal_for_tomorrow": pending_trade[1] if pending_trade else "NONE",
            "vix": round(vix, 2),
            "vix_in_30_40": "YES" if SVIX_ENTRY_VIX_LOW <= vix <= SVIX_ENTRY_VIX_HIGH else "NO",
            "vix_above_50": "YES" if vix >= SVIX_PANIC_VIX else "NO",
            "vix_below_20": "YES" if vix < SVIX_EXIT_VIX else "NO",
            "vix_curve_down": "YES" if vix_curve_down else "NO",
            "svix_close": round(svix_close, 4) if svix_avail else 0.0,
            "svix_open": round(svix_exec, 4) if svix_avail else 0.0,
            "svix_shares": round(svix_shares, 4),
            "svix_value": round(svix_value, 2),
            "svix_allocation_pct": round(svix_alloc_pct, 1),
            "cash": round(cash, 2),
            "cash_allocation_pct": round(100.0 - svix_alloc_pct, 1),
            "portfolio_value": round(portfolio_value, 2),
            "daily_pnl": round(
                portfolio_value - (daily_log[-1]["portfolio_value"] if daily_log else INITIAL_CAPITAL),
                2,
            ),
            "cumulative_return_pct": round((portfolio_value / INITIAL_CAPITAL - 1) * 100, 2),
        })

        prev_step2_state = step2_state

    metrics = calculate_step4_svix_metrics(daily_log, trades)
    return trades, daily_log, metrics


# =============================================================================
# METRICS
# =============================================================================


def calculate_step4_svix_metrics(daily_log: list[dict], trades: list[dict]) -> dict:
    """Calculate performance metrics for Step 4 SVIX standalone."""
    daily = pd.DataFrame(daily_log)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.set_index("date")
    daily["daily_return"] = daily["portfolio_value"].pct_change()
    daily = daily.dropna(subset=["daily_return"])

    total_days = (daily.index[-1] - daily.index[0]).days
    years = total_days / 365.25
    start_val = INITIAL_CAPITAL
    end_val = daily["portfolio_value"].iloc[-1]
    total_return = (end_val / start_val) - 1
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0.0

    daily_rf = RISK_FREE_RATE / 252
    excess = daily["daily_return"] - daily_rf
    sharpe = float((excess.mean() / excess.std()) * np.sqrt(252)) if excess.std() > 0 else 0.0
    downside = excess[excess < 0].std()
    sortino = float((excess.mean() / downside) * np.sqrt(252)) if downside > 0 else 0.0

    cummax = daily["portfolio_value"].cummax()
    drawdown = (daily["portfolio_value"] - cummax) / cummax
    max_dd = float(drawdown.min())
    max_dd_date = drawdown.idxmin().strftime("%Y-%m-%d")
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0.0

    active = daily[daily["step4_active"] == "YES"]
    svix_days = daily[daily["step4_state"].isin(["CASH_SVIX_INIT", "CASH_SVIX_PANIC"])]
    panic_days = daily[daily["step4_state"] == "CASH_SVIX_PANIC"]
    inactive = daily[daily["step4_state"] == "INACTIVE"]

    buy_trades = [t for t in trades if t["action"] in ("BUY_INIT", "BUY_PANIC")]
    sell_trades = [t for t in trades if t["action"] == "SELL_ALL"]
    round_trips = min(len([t for t in trades if t["action"] == "BUY_INIT"]), len(sell_trades))

    return {
        "step": "Step 4 — Safety Valve / SVIX standalone (DataBento)",
        "approach": "Activates only during Step 2 CASH periods; SVIX available from 2022-03-30",
        "svix_launch_date_used": SVIX_LAUNCH_DATE,
        "start_date": daily.index[0].strftime("%Y-%m-%d"),
        "end_date": daily.index[-1].strftime("%Y-%m-%d"),
        "years": round(years, 2),
        "initial_capital": start_val,
        "final_value": round(end_val, 2),
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3),
        "sortino": round(sortino, 3),
        "calmar": round(calmar, 3),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "max_drawdown_date": max_dd_date,
        "total_trades": len(trades),
        "buy_init_trades": len([t for t in trades if t["action"] == "BUY_INIT"]),
        "buy_panic_trades": len([t for t in trades if t["action"] == "BUY_PANIC"]),
        "sell_trades": len(sell_trades),
        "completed_round_trips": round_trips,
        "total_commissions": round(sum(t["commission_dollars"] for t in trades), 2),
        "total_slippage": round(sum(t["slippage_dollars"] for t in trades), 2),
        "days_active_in_cash": len(active),
        "days_in_svix_init": len(svix_days) - len(panic_days),
        "days_in_svix_panic": len(panic_days),
        "days_inactive": len(inactive),
        "pct_time_active": round(len(active) / len(daily) * 100, 1) if len(daily) > 0 else 0.0,
        "pct_time_in_svix": round(len(svix_days) / len(daily) * 100, 1) if len(daily) > 0 else 0.0,
        "note_pre_svix_period": (
            f"Safety Valve inactive before {SVIX_LAUNCH_DATE} — SVIX did not exist. "
            "No proxy or interpolation used. Strategy simply did not operate."
        ),
        "note_sgov_yield": (
            "SGOV yield on the 70%+ cash buffer is NOT included in these metrics. "
            "Approx 5% annual on ~80-90% of capital during CASH periods = separate line."
        ),
    }


# =============================================================================
# OUTPUT
# =============================================================================


def save_step4_svix_results(
    trades: list[dict], daily_log: list[dict], metrics: dict
) -> None:
    """Save all Step 4 SVIX results to the results directory."""
    pd.DataFrame(trades).to_csv(
        RESULTS_DIR / "step4_svix_databento_trade_log.csv", index=False
    )
    pd.DataFrame(daily_log).to_csv(
        RESULTS_DIR / "step4_svix_databento_portfolio_values.csv", index=False
    )
    with open(RESULTS_DIR / "step4_svix_databento_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"  Saved: step4_svix_databento_trade_log.csv ({len(trades)} trades)")
    print(f"  Saved: step4_svix_databento_portfolio_values.csv ({len(daily_log)} days)")
    print(f"  Saved: step4_svix_databento_metrics.json")


def print_step4_svix_summary(metrics: dict, trades: list[dict]) -> None:
    """Print compact performance summary for Step 4 SVIX."""
    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"  {metrics['approach']}")
    print(f"{'=' * 60}")
    print(f"  Period:      {metrics['start_date']} to {metrics['end_date']} ({metrics['years']}y)")
    print(f"  SVIX launch: {metrics['svix_launch_date_used']}")
    print(f"  Final Value: ${metrics['final_value']:,.2f}")
    print(f"  Total Return:{metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:        {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:      {metrics['sharpe']:.3f}")
    print(f"  Sortino:     {metrics['sortino']:.3f}")
    print(f"  Calmar:      {metrics['calmar']:.3f}")
    print(f"  Max Drawdown:{metrics['max_drawdown_pct']:.2f}% ({metrics['max_drawdown_date']})")
    print(f"\n  Total Trades:  {metrics['total_trades']}")
    print(f"  BUY_INIT:      {metrics['buy_init_trades']}")
    print(f"  BUY_PANIC:     {metrics['buy_panic_trades']}")
    print(f"  SELL_ALL:      {metrics['sell_trades']}")
    print(f"  Round Trips:   {metrics['completed_round_trips']}")
    print(f"\n  Days active (CASH): {metrics['days_active_in_cash']} ({metrics['pct_time_active']:.1f}%)")
    print(f"  Days in SVIX:       {metrics['days_active_in_cash'] - metrics.get('days_inactive',0)}")
    print(f"  Days inactive:      {metrics['days_inactive']}")
    print()
    for t in trades:
        print(f"  {t['execution_date']}: {t['action']} SVIX @ ${t['exec_price']:.2f} — {t['trigger_reason']}")
    print(f"{'=' * 60}")


# =============================================================================
# VALIDATION
# =============================================================================


def run_step4_svix_phase_gate_tests(
    trades: list[dict], daily_log: list[dict], metrics: dict
) -> None:
    """Run all phase-gate assertions for Step 4 SVIX standalone."""
    print("\n[Step 4 SVIX] Running phase-gate tests...")
    daily_df = pd.DataFrame(daily_log)

    # All trades must be SVIX instrument
    non_svix = [t for t in trades if t["instrument"] != "SVIX"]
    assert len(non_svix) == 0, f"Non-SVIX trades found: {non_svix}"
    print("  PASS: All trades are SVIX only")

    # No trades before SVIX launch date
    if trades:
        daily_df["date_dt"] = pd.to_datetime(daily_df["date"])
        svix_launch_ts = pd.Timestamp(SVIX_LAUNCH_DATE)
        trade_dates = pd.to_datetime([t["execution_date"] for t in trades])
        early_trades = trade_dates[trade_dates < svix_launch_ts]
        assert len(early_trades) == 0, f"SVIX trades before launch: {early_trades.tolist()}"
        print(f"  PASS: No SVIX trades before launch date ({SVIX_LAUNCH_DATE})")

    # All BUY_INIT trades happen during CASH periods
    buy_init_dates = {t["execution_date"] for t in trades if t["action"] == "BUY_INIT"}
    for bdate in buy_init_dates:
        rows = daily_df[daily_df["date"] == bdate]
        if not rows.empty:
            active = rows.iloc[0]["step4_active"]
            assert active == "YES", (
                f"BUY_INIT on {bdate} but step4_active={active} (Step 2 NOT in CASH)"
            )
    print(f"  PASS: All BUY_INIT trades in CASH periods ({len(buy_init_dates)} trades)")

    # Allocation cap never violated (< 99%)
    svix_alloc_max = daily_df["svix_allocation_pct"].max()
    assert svix_alloc_max <= 99.0, f"Allocation cap violated: {svix_alloc_max:.1f}%"
    print(f"  PASS: Max SVIX allocation = {svix_alloc_max:.1f}% (cap: 99%)")

    # Portfolio value always positive
    assert daily_df["portfolio_value"].min() > 0, "Portfolio value hit zero or negative"
    print(f"  PASS: Portfolio value always positive (min ${daily_df['portfolio_value'].min():,.2f})")

    # Total trades is non-negative
    assert metrics["total_trades"] >= 0
    print(f"  PASS: trade count = {metrics['total_trades']}")

    print("[Step 4 SVIX] All phase-gate tests PASSED\n")


# =============================================================================
# MAIN
# =============================================================================


def main() -> dict:
    """Run Step 4 Safety Valve (SVIX) standalone backtest and return metrics."""
    print("\n[Step 4 SVIX] Loading DataBento data (SVIX, VIX + Step 2 results)...")
    df, step2_daily = load_step4_svix_data()
    print(f"  Data range: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} bars)")

    cash_days = step2_daily[step2_daily["state"] == "CASH"]
    print(f"  Step 2 CASH days: {len(cash_days)} ({len(cash_days)/len(step2_daily)*100:.1f}% of time)")
    print(f"  SVIX available from: {SVIX_LAUNCH_DATE} (hardcoded launch date)")

    print("\n[Step 4 SVIX] Running Safety Valve backtest...")
    trades, daily_log, metrics = run_step4_svix_backtest(df, step2_daily)

    print_step4_svix_summary(metrics, trades)
    save_step4_svix_results(trades, daily_log, metrics)
    run_step4_svix_phase_gate_tests(trades, daily_log, metrics)

    return metrics


if __name__ == "__main__":
    main()
