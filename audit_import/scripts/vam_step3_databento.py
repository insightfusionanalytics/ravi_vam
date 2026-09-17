"""
VAM Step 3 — Predatory Short (SPXU) Backtest on DataBento Production Data
Client: Ravi Mareedu & Sudhir Vyakaranam

Strategy 2: Crash Offense (Predatory Short)
- Activates ONLY when Strategy 1 (VAM Split) state = CASH
- Entry: SPX < 200-Day SMA AND VIX > 30 → 50% of cash into SPXU (T+1)
- Exit: VIX drops below 30 OR SPX reclaims 50-Day SMA (T+1)
- Remaining 50% stays in cash (SGOV yield not counted here)
- SPXU slippage: always 20 bps (crash conditions)

Architecture:
- Reads Step 2 daily portfolio values to determine CASH periods
- Does NOT re-run Steps 1 or 2 — imports their outputs
- Same T+1 pending_trade pattern as Step 2
- SPXU 1:4 reverse split (2023-01-13) already applied in source CSV

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
# CONSTANTS
# =============================================================================

# =============================================================================
# === CONFIGURABLE PARAMETERS (flags from strategy_extraction.md) ===
# Parameters below are NOT confirmed by Ravi — confirm before go-live.
# Change defaults here; they propagate everywhere automatically.
# =============================================================================
SPXU_EXIT_VIX = 30           # Flag 7: Not discussed. IFA default: exit when VIX < 30.
SPXU_EXIT_SPY_50SMA = True   # Flag 7b: Exit when SPY reclaims 50-SMA.
USE_SGOV = True              # Flag 8: Not mentioned. IFA default: idle cash in SGOV.
COMMISSION_MODEL = "IBKR"   # Flag 10: Not discussed. Using IBKR tiered.

# Internal constants (confirmed by client or locked by code architecture)
SLIPPAGE_BPS_SPXU = 20.0     # Always 20 bps for SPXU (crash conditions)
INITIAL_CAPITAL = 100_000.0  # Initial portfolio value (confirmed: mar1 00:33:23)
RISK_FREE_RATE = 0.04        # 4% annual (same as Step 2)
VIX_ENTRY = 30.0             # VIX threshold to enter SPXU (confirmed: mar1 00:38:10-22)
SPXU_ALLOCATION = 0.50       # 50% of available cash into SPXU (confirmed: mar1 00:38:25)
SPXU_ALLOC_CAP = 0.99        # Hard cap on SPXU allocation

# SPXU 1:4 reverse split on 2023-01-13 (price went UP 4x).
# Backward adjustment: pre-split prices multiplied by 4.
# VERIFY: ratio of close[2023-01-13] / close[2023-01-12] must be in (0.85, 1.15).
SPXU_SPLIT_DATE = "2023-01-13"
SPXU_REVERSE_SPLIT_RATIO = 4


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


def verify_spxu_split_adjustment(spxu: pd.DataFrame) -> None:
    """Assert SPXU 1:4 reverse split (2023-01-13) is correctly backward-adjusted."""
    dates = spxu.index.strftime("%Y-%m-%d").tolist()
    if SPXU_SPLIT_DATE not in dates or "2023-01-12" not in dates:
        print("  WARN: SPXU split verification dates not found — skipping check")
        return
    price_before = spxu.loc["2023-01-12", "close"]
    price_after = spxu.loc["2023-01-13", "close"]
    ratio = price_after / price_before
    assert 0.85 < ratio < 1.15, (
        f"SPXU split NOT adjusted: 2023-01-12={price_before:.4f} → "
        f"2023-01-13={price_after:.4f} (ratio={ratio:.4f}, expected ~1.0)"
    )
    print(
        f"  SPXU split verified: {price_before:.4f} → {price_after:.4f} "
        f"(ratio={ratio:.4f}) ✓"
    )


def load_step3_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load all data required for Step 3 and merge into a single DataFrame.

    Returns:
        df: Merged daily DataFrame (SPY, SPXU, VIX + indicators).
        step2_daily: Step 2 portfolio values with state column.
    """
    spy = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPY_daily.csv")
    spxu = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPXU_daily.csv")
    vix = load_databento_csv(DATA_DIR / "cboe" / "VIX_daily.csv")

    verify_spxu_split_adjustment(spxu)

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["SPXU_Close"] = spxu["close"].reindex(spy.index)
    df["SPXU_Open"] = spxu["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)

    df = df.ffill().dropna()

    # Indicators for entry/exit signals
    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()

    # Load Step 2 daily portfolio values (source of truth for CASH periods)
    step2_daily = pd.read_csv(
        RESULTS_DIR / "step2_databento_portfolio_values.csv",
        parse_dates=["date"],
        index_col="date",
    )
    # step2_daily index is timezone-naive dates (YYYY-MM-DD)
    return df, step2_daily


# =============================================================================
# BACKTEST ENGINE
# =============================================================================


def get_step2_state(date: pd.Timestamp, step2_daily: pd.DataFrame) -> str:
    """Look up Step 2's state on the given date."""
    date_key = date.normalize()
    if date_key in step2_daily.index:
        return str(step2_daily.loc[date_key, "state"])
    return "UNKNOWN"


def run_step3_backtest(
    df: pd.DataFrame, step2_daily: pd.DataFrame
) -> tuple[list[dict], list[dict], dict]:
    """Run the Predatory Short backtest.

    Only operates during CASH periods from Step 2.
    T+1 execution: signal generated on today's close, executed at next open.
    50% of portfolio into SPXU when both entry conditions are met.
    SPXU slippage always 20 bps (crash conditions per proposal).
    """
    trading_df = df.dropna(subset=["SPY_SMA50", "SPY_SMA200"])
    if trading_df.empty:
        raise ValueError("No valid data after SMA warmup period")

    cash = INITIAL_CAPITAL
    spxu_shares = 0.0
    in_spxu = False
    pending_trade: tuple[str, str] | None = None  # (action: BUY/SELL, reason)
    trades: list[dict] = []
    daily_log: list[dict] = []
    prev_step2_state = "UNKNOWN"

    for date, row in trading_df.iterrows():
        spxu_price = row["SPXU_Close"]
        spxu_exec = row["SPXU_Open"]
        vix = row["VIX"]
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]

        step2_state = get_step2_state(date, step2_daily)
        currently_cash = step2_state == "CASH"

        # Safety: if Step 2 just left CASH and we still have SPXU, force-exit now
        # (inject a SELL at today's open before processing any other pending trade)
        if not currently_cash and in_spxu and pending_trade is None:
            pending_trade = ("SELL", "FORCED_EXIT: Step 2 exited CASH state")

        # STEP 1: Execute pending trade from yesterday's signal at today's open
        if pending_trade is not None:
            action, reason = pending_trade
            pending_trade = None

            pv_before_trade = cash + spxu_shares * spxu_exec

            if action == "BUY" and spxu_exec > 0:
                invest_amount = min(pv_before_trade * SPXU_ALLOCATION, pv_before_trade * SPXU_ALLOC_CAP)
                slip = invest_amount * (SLIPPAGE_BPS_SPXU / 10_000)
                comm = ibkr_commission(invest_amount, spxu_exec)

                old_shares = spxu_shares
                old_cash = cash
                new_shares = invest_amount / spxu_exec
                spxu_shares = new_shares
                cash = pv_before_trade - invest_amount - slip - comm
                in_spxu = True

                pv_at_close = cash + spxu_shares * spxu_price
                sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

                trades.append({
                    "trade_number": len(trades) + 1,
                    "signal_date": sig_date,
                    "execution_date": date.strftime("%Y-%m-%d"),
                    "execution_timing": "T+1 (signal at prev close, execute at today open)",
                    "action": "BUY",
                    "instrument": "SPXU",
                    "state_from": "CASH_IDLE",
                    "state_to": "CASH_SPXU",
                    "trigger_reason": reason,
                    "target_allocation_pct": round(SPXU_ALLOCATION * 100, 1),
                    "exec_price": round(spxu_exec, 4),
                    "close_same_day": round(spxu_price, 4),
                    "overnight_gap_pct": round((spxu_exec / spxu_price - 1) * 100, 2) if spxu_price > 0 else 0,
                    "shares_before": round(old_shares, 4),
                    "shares_delta": round(new_shares, 4),
                    "shares_after": round(spxu_shares, 4),
                    "cash_before": round(old_cash, 2),
                    "cash_after": round(cash, 2),
                    "portfolio_value_before_trade": round(pv_before_trade, 2),
                    "portfolio_value_at_close": round(pv_at_close, 2),
                    "trade_value_dollars": round(invest_amount, 2),
                    "commission_dollars": round(comm, 2),
                    "slippage_bps_used": SLIPPAGE_BPS_SPXU,
                    "slippage_dollars": round(slip, 2),
                    "total_cost_dollars": round(slip + comm, 2),
                    "slippage_type": "STRESS (crash environment, always 20bps)",
                    "check_pv_equals_cash_plus_positions": round(
                        pv_at_close - (cash + spxu_shares * spxu_price), 2
                    ),
                })

            elif action == "SELL" and spxu_shares > 0:
                sell_val = spxu_shares * spxu_exec
                slip = sell_val * (SLIPPAGE_BPS_SPXU / 10_000)
                comm = ibkr_commission(sell_val, spxu_exec)

                old_shares = spxu_shares
                old_cash = cash
                pv_before_trade = cash + spxu_shares * spxu_exec
                cash = cash + sell_val - slip - comm
                spxu_shares = 0.0
                in_spxu = False

                pv_at_close = cash
                sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

                trades.append({
                    "trade_number": len(trades) + 1,
                    "signal_date": sig_date,
                    "execution_date": date.strftime("%Y-%m-%d"),
                    "execution_timing": "T+1 (signal at prev close, execute at today open)",
                    "action": "SELL",
                    "instrument": "SPXU",
                    "state_from": "CASH_SPXU",
                    "state_to": "CASH_IDLE",
                    "trigger_reason": reason,
                    "target_allocation_pct": 0.0,
                    "exec_price": round(spxu_exec, 4),
                    "close_same_day": round(spxu_price, 4),
                    "overnight_gap_pct": round((spxu_exec / spxu_price - 1) * 100, 2) if spxu_price > 0 else 0,
                    "shares_before": round(old_shares, 4),
                    "shares_delta": round(-old_shares, 4),
                    "shares_after": 0.0,
                    "cash_before": round(old_cash, 2),
                    "cash_after": round(cash, 2),
                    "portfolio_value_before_trade": round(pv_before_trade, 2),
                    "portfolio_value_at_close": round(pv_at_close, 2),
                    "trade_value_dollars": round(sell_val, 2),
                    "commission_dollars": round(comm, 2),
                    "slippage_bps_used": SLIPPAGE_BPS_SPXU,
                    "slippage_dollars": round(slip, 2),
                    "total_cost_dollars": round(slip + comm, 2),
                    "slippage_type": "STRESS (crash environment, always 20bps)",
                    "check_pv_equals_cash_plus_positions": 0.0,
                })

        # STEP 2: Generate signal for TOMORROW based on today's close
        # (Only when Step 2 is in CASH — Predatory Short is inactive otherwise)
        if currently_cash:
            spy_below_200 = spy_close < spy_sma200
            spy_above_50 = spy_close > spy_sma50
            vix_above_entry = vix > VIX_ENTRY
            vix_below_exit = vix < SPXU_EXIT_VIX

            if not in_spxu:
                # Entry: BOTH conditions required simultaneously (confirmed: mar1 00:38:10)
                if spy_below_200 and vix_above_entry:
                    pending_trade = (
                        "BUY",
                        f"PREDATORY_SHORT: SPY<200SMA({spy_sma200:.0f}) + VIX={vix:.1f}>{VIX_ENTRY}",
                    )
            else:
                # Exit: VIX drops below SPXU_EXIT_VIX, or (if SPXU_EXIT_SPY_50SMA) SPY reclaims 50-SMA
                exit_parts = []
                if vix_below_exit:
                    exit_parts.append(f"VIX={vix:.1f}<{SPXU_EXIT_VIX}")
                if SPXU_EXIT_SPY_50SMA and spy_above_50:
                    exit_parts.append(f"SPY>{spy_sma50:.0f}(50SMA)")
                if exit_parts:
                    pending_trade = ("SELL", "EXIT: " + " | ".join(exit_parts))

        # Portfolio snapshot at close
        portfolio_value = cash + spxu_shares * spxu_price
        spxu_value = spxu_shares * spxu_price
        spxu_alloc_pct = (spxu_value / portfolio_value * 100) if portfolio_value > 0 else 0

        if in_spxu:
            step3_state = "CASH_SPXU"
        elif currently_cash:
            step3_state = "CASH_IDLE"
        else:
            step3_state = "INACTIVE"

        daily_log.append({
            # Date & State
            "date": date.strftime("%Y-%m-%d"),
            "day_number": len(daily_log) + 1,
            "step2_state": step2_state,
            "step3_active": "YES" if currently_cash else "NO",
            "step3_state": step3_state,
            "pending_signal_for_tomorrow": pending_trade[1] if pending_trade else "NONE",
            # Prices
            "spy_close": round(spy_close, 2),
            "spy_sma50": round(spy_sma50, 2),
            "spy_sma200": round(spy_sma200, 2),
            "spy_vs_sma200": "BELOW" if spy_close < spy_sma200 else "ABOVE",
            "spy_vs_sma50": "BELOW" if spy_close < spy_sma50 else "ABOVE",
            "vix": round(vix, 2),
            "vix_above_30": "YES" if vix > VIX_ENTRY else "NO",
            "spxu_open": round(spxu_exec, 4),
            "spxu_close": round(spxu_price, 4),
            # Entry conditions (only meaningful when step2_state = CASH)
            "entry_condition_spy_below_200": "YES" if spy_close < spy_sma200 else "NO",
            "entry_condition_vix_above_30": "YES" if vix > VIX_ENTRY else "NO",
            "both_entry_conditions_met": (
                "YES" if (spy_close < spy_sma200 and vix > VIX_ENTRY) else "NO"
            ),
            # Portfolio
            "spxu_shares": round(spxu_shares, 4),
            "spxu_value": round(spxu_value, 2),
            "cash": round(cash, 2),
            "portfolio_value": round(portfolio_value, 2),
            "spxu_allocation_pct": round(spxu_alloc_pct, 1),
            "cash_allocation_pct": round(100 - spxu_alloc_pct, 1),
            "daily_pnl": round(
                portfolio_value - (daily_log[-1]["portfolio_value"] if daily_log else INITIAL_CAPITAL),
                2,
            ),
            "cumulative_return_pct": round((portfolio_value / INITIAL_CAPITAL - 1) * 100, 2),
        })

        prev_step2_state = step2_state

    metrics = calculate_metrics(daily_log, trades)
    return trades, daily_log, metrics


# =============================================================================
# METRICS
# =============================================================================


def calculate_metrics(daily_log: list[dict], trades: list[dict]) -> dict:
    """Calculate performance metrics for Step 3."""
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
    cagr = (end_val / start_val) ** (1 / years) - 1 if years > 0 else 0

    daily_returns = daily["daily_return"]
    daily_rf = RISK_FREE_RATE / 252
    excess_returns = daily_returns - daily_rf
    sharpe = (
        (excess_returns.mean() / excess_returns.std()) * np.sqrt(252)
        if excess_returns.std() > 0
        else 0
    )
    downside = excess_returns[excess_returns < 0].std()
    sortino = (excess_returns.mean() / downside) * np.sqrt(252) if downside > 0 else 0

    cummax = daily["portfolio_value"].cummax()
    drawdown = (daily["portfolio_value"] - cummax) / cummax
    max_dd = drawdown.min()
    max_dd_date = drawdown.idxmin().strftime("%Y-%m-%d")
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    active_days = daily[daily["step3_active"] == "YES"]
    cash_spxu_days = daily[daily["step3_state"] == "CASH_SPXU"]
    cash_idle_days = daily[daily["step3_state"] == "CASH_IDLE"]
    inactive_days = daily[daily["step3_state"] == "INACTIVE"]

    buy_trades = [t for t in trades if t["action"] == "BUY"]
    sell_trades = [t for t in trades if t["action"] == "SELL"]

    # P&L per trade pair
    trade_pnl = []
    for i, buy in enumerate(buy_trades):
        if i < len(sell_trades):
            sell = sell_trades[i]
            pnl = sell["portfolio_value_at_close"] - buy["portfolio_value_before_trade"]
            trade_pnl.append(pnl)

    win_trades = sum(1 for p in trade_pnl if p > 0)
    win_rate = win_trades / len(trade_pnl) if trade_pnl else 0

    return {
        "step": "Step 3 — Predatory Short / SPXU (DataBento)",
        "approach": "Activates only during Step 2 CASH periods",
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
        "buy_trades": len(buy_trades),
        "sell_trades": len(sell_trades),
        "completed_round_trips": len(trade_pnl),
        "win_rate_pct": round(win_rate * 100, 1),
        "total_commissions": round(sum(t["commission_dollars"] for t in trades), 2),
        "total_slippage": round(sum(t["slippage_dollars"] for t in trades), 2),
        "days_active_in_cash": len(active_days),
        "days_in_cash_spxu": len(cash_spxu_days),
        "days_in_cash_idle": len(cash_idle_days),
        "days_inactive": len(inactive_days),
        "pct_time_in_cash": round(len(active_days) / len(daily) * 100, 1),
        "pct_time_in_spxu": round(len(cash_spxu_days) / len(daily) * 100, 1),
        "note_sgov_yield": (
            "SGOV yield on the 50% cash portion is NOT included in these metrics. "
            "Approx 5% annual on 50% of capital during CASH periods = separate accounting line."
        ),
    }


# =============================================================================
# OUTPUT
# =============================================================================


def save_results(trades: list[dict], daily_log: list[dict], metrics: dict) -> None:
    """Save all Step 3 results."""
    trade_df = pd.DataFrame(trades)
    trade_df.to_csv(RESULTS_DIR / "step3_databento_trade_log.csv", index=False)

    daily_df = pd.DataFrame(daily_log)
    daily_df.to_csv(RESULTS_DIR / "step3_databento_portfolio_values.csv", index=False)

    with open(RESULTS_DIR / "step3_databento_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"\n  Saved: step3_databento_trade_log.csv ({len(trades)} trades)")
    print(f"  Saved: step3_databento_portfolio_values.csv ({len(daily_log)} days)")
    print(f"  Saved: step3_databento_metrics.json")


def print_summary(metrics: dict, trades: list[dict]) -> None:
    """Print compact performance summary."""
    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"  {metrics['approach']}")
    print(f"{'=' * 60}")
    print(f"  Period:      {metrics['start_date']} to {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value: ${metrics['final_value']:,.2f}")
    print(f"  Total Return:{metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:        {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:      {metrics['sharpe']:.3f}")
    print(f"  Sortino:     {metrics['sortino']:.3f}")
    print(f"  Calmar:      {metrics['calmar']:.3f}")
    print(f"  Max Drawdown:{metrics['max_drawdown_pct']:.2f}% ({metrics['max_drawdown_date']})")
    print(f"\n  Trades:      {metrics['total_trades']} ({metrics['completed_round_trips']} round trips)")
    print(f"  Win Rate:    {metrics['win_rate_pct']:.1f}%")
    print(f"  Costs:       ${metrics['total_commissions'] + metrics['total_slippage']:.2f}")
    print(f"\n  Days in CASH (all): {metrics['days_active_in_cash']} ({metrics['pct_time_in_cash']:.1f}%)")
    print(f"  Days in SPXU:       {metrics['days_in_cash_spxu']} ({metrics['pct_time_in_spxu']:.1f}%)")
    print(f"  Days inactive:      {metrics['days_inactive']}")
    print(f"\n  CASH periods w/ trades:")
    buys = [t for t in trades if t["action"] == "BUY"]
    for t in buys:
        print(f"    {t['execution_date']}: BUY SPXU @ ${t['exec_price']:.2f} — {t['trigger_reason']}")
    sells = [t for t in trades if t["action"] == "SELL"]
    for t in sells:
        print(f"    {t['execution_date']}: SELL SPXU @ ${t['exec_price']:.2f} — {t['trigger_reason']}")
    print(f"{'=' * 60}")


# =============================================================================
# VALIDATION
# =============================================================================


def run_phase_gate_tests(trades: list[dict], metrics: dict) -> None:
    """Run all phase-gate tests from the Chat 2 spec. Assert PASS or raise."""
    print("\n[Step 3] Running phase-gate tests...")

    # Test: all trades are SPXU only
    non_spxu = [t for t in trades if t["instrument"] != "SPXU"]
    assert len(non_spxu) == 0, f"Non-SPXU trades found: {non_spxu}"
    print("  PASS: All trades are SPXU only")

    # Test: no buy/sell when step2_state != CASH
    # (We track this in the daily log — Step 3 should never trade in INACTIVE)
    buy_dates = {t["execution_date"] for t in trades if t["action"] == "BUY"}
    if buy_dates:
        # Load the daily log back to verify
        daily_df = pd.read_csv(RESULTS_DIR / "step3_databento_portfolio_values.csv")
        # Check that on every BUY execution date, step3_active = YES
        for bdate in buy_dates:
            row = daily_df[daily_df["date"] == bdate]
            if not row.empty:
                active = row.iloc[0]["step3_active"]
                assert active == "YES", (
                    f"SPXU BUY on {bdate} but step3_active = {active} "
                    f"(Step 2 was NOT in CASH)"
                )
        print("  PASS: All SPXU buys occurred during Step 2 CASH periods")
    else:
        print("  INFO: No SPXU trades occurred (CASH periods had no qualifying conditions)")

    # Test: total_trades is non-negative
    assert metrics["total_trades"] >= 0, "Negative trade count"
    print(f"  PASS: trade count = {metrics['total_trades']}")

    # Test: SPXU split verification (ratio in 0.85-1.15)
    spxu_df = pd.read_csv(DATA_DIR / "databento" / "equities" / "SPXU_daily.csv")
    spxu_df["datetime"] = pd.to_datetime(spxu_df["datetime"])
    spxu_df = spxu_df.set_index("datetime").sort_index()
    if spxu_df.index.tz is not None:
        spxu_df.index = spxu_df.index.tz_localize(None)
    dates = spxu_df.index.strftime("%Y-%m-%d").tolist()
    if "2023-01-12" in dates and "2023-01-13" in dates:
        p1 = spxu_df.loc["2023-01-12", "close"]
        p2 = spxu_df.loc["2023-01-13", "close"]
        ratio = p2 / p1
        assert 0.85 < ratio < 1.15, f"SPXU split not adjusted: ratio={ratio:.4f}"
        print(f"  PASS: SPXU split ratio = {ratio:.4f} (within 0.85-1.15)")
    else:
        print("  WARN: SPXU split date not in data range — skipping ratio check")

    print("[Step 3] All phase-gate tests PASSED\n")


# =============================================================================
# MAIN
# =============================================================================


def main() -> dict:
    """Run Step 3 Predatory Short backtest and return metrics."""
    print("\n[Step 3] Loading DataBento data (SPY, SPXU, VIX + Step 2 results)...")
    df, step2_daily = load_step3_data()
    print(f"  Data range: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} bars)")

    cash_days = step2_daily[step2_daily["state"] == "CASH"]
    print(f"  Step 2 CASH days: {len(cash_days)} ({len(cash_days)/len(step2_daily)*100:.1f}% of time)")

    print("\n[Step 3] Running Predatory Short backtest...")
    trades, daily_log, metrics = run_step3_backtest(df, step2_daily)

    print_summary(metrics, trades)
    save_results(trades, daily_log, metrics)
    run_phase_gate_tests(trades, metrics)

    return metrics


if __name__ == "__main__":
    main()
