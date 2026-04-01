"""
VAM Step 2 — UPRO + TQQQ Backtest on DataBento Production Data
Client: Ravi Mareedu & Sudhir Vyakaranam

6-State Machine (proposal calls it "7-state" counting the split):
  BULL_FULL     — 75% UPRO + 25% TQQQ (SPY & QQQ above 50-SMA, VIX < 30)
  BULL_TRIMMED  — 75% of bull allocation (RSI > 75)
  DEF_SPY       — SPY below 50-SMA: UPRO sleeve halved, TQQQ sleeve intact
  DEF_QQQ       — QQQ below 50-SMA: TQQQ sleeve halved, UPRO sleeve intact
  DEF_BOTH      — Both below 50-SMA: both sleeves halved
  CASH          — Kill switch: 100% cash

Signals: SPY SMA-50/200, QQQ SMA-50, SPY RSI-14, VIX
Positions: UPRO (3x SPY) + TQQQ (3x QQQ)
"""

import sys
import json
from pathlib import Path
from enum import Enum

import pandas as pd
import numpy as np

ENGINE_ROOT = Path(__file__).resolve().parents[3]  # _engine/code/
DATA_DIR = ENGINE_ROOT / "data"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


# =============================================================================
# DATA LOADING
# =============================================================================


def load_databento_csv(filepath: Path) -> pd.DataFrame:
    """Load a DataBento daily CSV."""
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df


# DataBento data is NOT split-adjusted. Apply in-memory.
UPRO_SPLITS = [
    ("2018-05-24", 3),
    ("2022-01-13", 2),
]

TQQQ_SPLITS = [
    ("2021-01-21", 2),
    ("2022-01-13", 2),
    ("2025-11-20", 2),
]

PRICE_COLS = ["open", "high", "low", "close"]


def adjust_for_splits(df: pd.DataFrame, splits: list[tuple[str, int]]) -> pd.DataFrame:
    """Adjust historical prices for forward stock splits."""
    df = df.copy()
    for split_date, ratio in splits:
        mask = df.index < pd.Timestamp(split_date)
        for col in PRICE_COLS:
            if col in df.columns:
                df.loc[mask, col] = df.loc[mask, col] / ratio
    return df


def load_step2_data() -> pd.DataFrame:
    """Load SPY, QQQ, UPRO, TQQQ, VIX and merge into a single aligned DataFrame.

    Includes next-day opens for T+1 execution (no look-ahead bias).
    """
    spy = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPY_daily.csv")
    qqq = load_databento_csv(DATA_DIR / "databento" / "equities" / "QQQ_daily.csv")
    upro = load_databento_csv(DATA_DIR / "databento" / "equities" / "UPRO_daily.csv")
    upro = adjust_for_splits(upro, UPRO_SPLITS)
    tqqq = load_databento_csv(DATA_DIR / "databento" / "equities" / "TQQQ_daily.csv")
    tqqq = adjust_for_splits(tqqq, TQQQ_SPLITS)
    vix = load_databento_csv(DATA_DIR / "cboe" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["QQQ_Close"] = qqq["close"].reindex(spy.index)
    df["UPRO_Close"] = upro["close"].reindex(spy.index)
    df["UPRO_Open"] = upro["open"].reindex(spy.index)
    df["TQQQ_Close"] = tqqq["close"].reindex(spy.index)
    df["TQQQ_Open"] = tqqq["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)

    df = df.ffill().dropna()
    return df


# =============================================================================
# INDICATORS
# =============================================================================


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI using EWM."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def consecutive_streak(condition: pd.Series) -> pd.Series:
    """Count consecutive True days."""
    streak = condition.astype(int).copy()
    for i in range(1, len(streak)):
        if streak.iloc[i] == 1:
            streak.iloc[i] = streak.iloc[i - 1] + 1
        else:
            streak.iloc[i] = 0
    return streak


def add_step2_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate indicators for both SPY and QQQ signals."""
    # SPY indicators
    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()
    df["SPY_RSI"] = calculate_rsi(df["SPY_Close"], 14)
    df["SPY_below_50_streak"] = consecutive_streak(df["SPY_Close"] < df["SPY_SMA50"])
    df["SPY_above_50_streak"] = consecutive_streak(df["SPY_Close"] > df["SPY_SMA50"])

    # QQQ indicators
    df["QQQ_SMA50"] = df["QQQ_Close"].rolling(50).mean()
    df["QQQ_below_50_streak"] = consecutive_streak(df["QQQ_Close"] < df["QQQ_SMA50"])
    df["QQQ_above_50_streak"] = consecutive_streak(df["QQQ_Close"] > df["QQQ_SMA50"])

    return df


# =============================================================================
# STATE MACHINE (6-state, Step 2)
# =============================================================================


class State(Enum):
    BULL_FULL = "BULL_FULL"
    BULL_TRIMMED = "BULL_TRIMMED"
    DEF_SPY = "DEF_SPY"
    DEF_QQQ = "DEF_QQQ"
    DEF_BOTH = "DEF_BOTH"
    CASH = "CASH"


# Allocations: (UPRO_pct, TQQQ_pct)
# Bull: 74.25% UPRO + 24.75% TQQQ = 99% invested (1% reserved for costs)
# Trimmed: sell 25% of total to cash = 56.25% UPRO + 18.75% TQQQ
# DEF_SPY: UPRO sleeve halved = 37.5% UPRO + 25% TQQQ
# DEF_QQQ: TQQQ sleeve halved = 75% UPRO + 12.5% TQQQ
# DEF_BOTH: both halved = 37.5% UPRO + 12.5% TQQQ
STATE_ALLOCATION: dict[State, tuple[float, float]] = {
    State.BULL_FULL: (0.7425, 0.2475),
    State.BULL_TRIMMED: (0.5625, 0.1875),
    State.DEF_SPY: (0.375, 0.25),
    State.DEF_QQQ: (0.75, 0.125),
    State.DEF_BOTH: (0.375, 0.125),
    State.CASH: (0.0, 0.0),
}

VIX_KILL = 30.0
SMA_CONFIRM_DAYS = 2
RSI_SELL = 75.0
RSI_REBUY = 60.0
COMMISSION = 1.0
SLIPPAGE_BPS_NORMAL = 5.0  # Normal market conditions
SLIPPAGE_BPS_STRESS = 20.0  # VIX > 25 or kill switch days
INITIAL_CAPITAL = 100_000.0
RISK_FREE_RATE = 0.04  # ~4% avg 2020-2025 (Fed funds 0-5.5%)


def next_state(
    current: State,
    spy_close: float,
    spy_sma50: float,
    spy_sma200: float,
    spy_rsi: float,
    vix: float,
    spy_below_streak: int,
    spy_above_streak: int,
    qqq_close: float,
    qqq_sma50: float,
    qqq_below_streak: int,
    qqq_above_streak: int,
) -> tuple[State, str]:
    """
    6-state machine with independent SPY/QQQ defensive triggers.
    Priority: Kill > Defensive > RSI
    """
    # PRIORITY 1: Kill Switch (from any non-CASH state)
    if current != State.CASH:
        if vix > VIX_KILL or spy_close < spy_sma200:
            return State.CASH, f"KILL: VIX={vix:.1f}, SPY vs 200SMA"

    spy_below = spy_below_streak >= SMA_CONFIRM_DAYS
    qqq_below = qqq_below_streak >= SMA_CONFIRM_DAYS
    spy_above = spy_above_streak >= SMA_CONFIRM_DAYS
    qqq_above = qqq_above_streak >= SMA_CONFIRM_DAYS

    # Bull states
    if current in (State.BULL_FULL, State.BULL_TRIMMED):
        # PRIORITY 2: Check defensive triggers
        if spy_below and qqq_below:
            return State.DEF_BOTH, "DEFENSIVE BOTH: SPY & QQQ below 50-SMA"
        if spy_below:
            return State.DEF_SPY, "DEFENSIVE SPY: SPY below 50-SMA"
        if qqq_below:
            return State.DEF_QQQ, "DEFENSIVE QQQ: QQQ below 50-SMA"

        # PRIORITY 3: RSI
        if current == State.BULL_FULL and spy_rsi > RSI_SELL:
            return State.BULL_TRIMMED, f"RSI TRIM: RSI={spy_rsi:.1f}"
        if current == State.BULL_TRIMMED and spy_rsi < RSI_REBUY:
            return State.BULL_FULL, f"RSI RECOVERY: RSI={spy_rsi:.1f}"

        return current, "HOLD"

    # Defensive states — check for recovery or worsening
    if current == State.DEF_SPY:
        if qqq_below:
            return State.DEF_BOTH, "WORSENING: QQQ also below 50-SMA"
        if spy_above and vix < VIX_KILL:
            return State.BULL_FULL, "RECOVERY: SPY reclaimed 50-SMA"
        return State.DEF_SPY, "HOLD"

    if current == State.DEF_QQQ:
        if spy_below:
            return State.DEF_BOTH, "WORSENING: SPY also below 50-SMA"
        if qqq_above and vix < VIX_KILL:
            return State.BULL_FULL, "RECOVERY: QQQ reclaimed 50-SMA"
        return State.DEF_QQQ, "HOLD"

    if current == State.DEF_BOTH:
        if spy_above and qqq_above and vix < VIX_KILL:
            return State.BULL_FULL, "FULL RECOVERY: both above 50-SMA"
        if spy_above and not qqq_above:
            return State.DEF_QQQ, "PARTIAL: SPY recovered, QQQ still below"
        if qqq_above and not spy_above:
            return State.DEF_SPY, "PARTIAL: QQQ recovered, SPY still below"
        return State.DEF_BOTH, "HOLD"

    # CASH — re-entry
    if current == State.CASH:
        if (
            spy_close > spy_sma200
            and vix < VIX_KILL
            and spy_close > spy_sma50
            and qqq_close > qqq_sma50
        ):
            return State.BULL_FULL, "RE-ENTRY: all conditions met"
        return State.CASH, "HOLD"

    return current, "NO_TRANSITION"


# =============================================================================
# BACKTEST ENGINE
# =============================================================================


def get_slippage_bps(vix: float, is_kill: bool) -> float:
    """Dynamic slippage: higher on stress days (VIX > 25 or kill switch)."""
    if is_kill or vix > 25:
        return SLIPPAGE_BPS_STRESS
    return SLIPPAGE_BPS_NORMAL


def run_step2_backtest(df: pd.DataFrame) -> tuple[list[dict], list[dict], dict]:
    """
    Run the 6-state UPRO+TQQQ backtest on DataBento data.

    T+1 execution: signal generated on today's close, trade executed at
    next day's open. This eliminates look-ahead bias per the proposal:
    "signal at 4 PM EST close, order placed next day 30 min after open."
    Tracks two positions: UPRO shares and TQQQ shares.
    """
    trading_df = df.dropna(subset=["SPY_SMA200", "SPY_RSI", "QQQ_SMA50", "UPRO_Open", "TQQQ_Open"])
    if trading_df.empty:
        raise ValueError("No valid data after warmup")

    cash = INITIAL_CAPITAL
    upro_shares = 0.0
    tqqq_shares = 0.0
    state = State.CASH
    pending_trade: tuple[State, str] | None = None  # (new_state, reason)
    trades: list[dict] = []
    daily_log: list[dict] = []

    for date, row in trading_df.iterrows():
        upro_price = row["UPRO_Close"]
        tqqq_price = row["TQQQ_Close"]
        upro_exec = row["UPRO_Open"]
        tqqq_exec = row["TQQQ_Open"]
        vix = row["VIX"]

        # STEP 1: Execute pending trade from yesterday's signal at today's open
        if pending_trade is not None:
            new_state, reason = pending_trade
            pending_trade = None

            pv_before = cash + upro_shares * upro_exec + tqqq_shares * tqqq_exec
            upro_alloc, tqqq_alloc = STATE_ALLOCATION[new_state]
            is_kill = "KILL" in reason
            slip_bps = get_slippage_bps(vix, is_kill)
            slip_type = "STRESS (VIX>25 or kill)" if slip_bps > 5 else "NORMAL"

            target_upro = (pv_before * upro_alloc) / upro_exec if upro_exec > 0 else 0
            target_tqqq = (pv_before * tqqq_alloc) / tqqq_exec if tqqq_exec > 0 else 0

            # UPRO rebalance
            upro_delta = target_upro - upro_shares
            upro_trade_val = abs(upro_delta * upro_exec)
            upro_comm = COMMISSION if upro_trade_val > 0 else 0
            upro_slip_dollars = upro_trade_val * (slip_bps / 10000)

            # TQQQ rebalance
            tqqq_delta = target_tqqq - tqqq_shares
            tqqq_trade_val = abs(tqqq_delta * tqqq_exec)
            tqqq_comm = COMMISSION if tqqq_trade_val > 0 else 0
            tqqq_slip_dollars = tqqq_trade_val * (slip_bps / 10000)

            total_cost = upro_comm + upro_slip_dollars + tqqq_comm + tqqq_slip_dollars

            old_upro = upro_shares
            old_tqqq = tqqq_shares
            old_cash = cash
            old_state_val = state.value
            upro_shares = target_upro
            tqqq_shares = target_tqqq
            cash = pv_before - (target_upro * upro_exec) - (target_tqqq * tqqq_exec) - total_cost
            state = new_state

            pv_at_close = cash + upro_shares * upro_price + tqqq_shares * tqqq_price
            exec_date_str = date.strftime("%Y-%m-%d")
            signal_date_str = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

            if upro_trade_val > 0:
                trades.append({
                    # ── Identity ──
                    "trade_number": len(trades) + 1,
                    "signal_date": signal_date_str,
                    "execution_date": exec_date_str,
                    "execution_timing": "T+1 (signal at prev close, execute at today open)",
                    # ── Action ──
                    "action": "BUY" if upro_delta > 0 else "SELL",
                    "instrument": "UPRO",
                    "state_from": old_state_val,
                    "state_to": new_state.value,
                    "trigger_reason": reason,
                    "target_allocation_pct": round(upro_alloc * 100, 2),
                    # ── Execution prices ──
                    "exec_price": round(upro_exec, 4),
                    "close_same_day": round(upro_price, 4),
                    "overnight_gap_pct": round((upro_exec / upro_price - 1) * 100, 2) if upro_price > 0 else 0,
                    # ── Position before ──
                    "shares_before": round(old_upro, 4),
                    "cash_before": round(old_cash, 2),
                    "portfolio_value_before_trade": round(pv_before, 2),
                    # ── Trade math ──
                    "shares_delta": round(upro_delta, 4),
                    "trade_value_dollars": round(upro_trade_val, 2),
                    "commission_dollars": round(upro_comm, 2),
                    "slippage_bps_used": round(slip_bps, 1),
                    "slippage_dollars": round(upro_slip_dollars, 2),
                    "total_cost_dollars": round(upro_comm + upro_slip_dollars, 2),
                    "slippage_type": slip_type,
                    # ── Position after ──
                    "shares_after": round(target_upro, 4),
                    "cash_after": round(cash, 2),
                    "portfolio_value_at_close": round(pv_at_close, 2),
                    # ── Verification ──
                    "check_pv_equals_cash_plus_positions": round(
                        pv_at_close - (cash + upro_shares * upro_price + tqqq_shares * tqqq_price), 2
                    ),
                    "check_upro_alloc_pct_actual": round(
                        (upro_shares * upro_exec / pv_before) * 100, 2
                    ) if pv_before > 0 else 0,
                })

            if tqqq_trade_val > 0:
                trades.append({
                    # ── Identity ──
                    "trade_number": len(trades) + 1,
                    "signal_date": signal_date_str,
                    "execution_date": exec_date_str,
                    "execution_timing": "T+1 (signal at prev close, execute at today open)",
                    # ── Action ──
                    "action": "BUY" if tqqq_delta > 0 else "SELL",
                    "instrument": "TQQQ",
                    "state_from": old_state_val,
                    "state_to": new_state.value,
                    "trigger_reason": reason,
                    "target_allocation_pct": round(tqqq_alloc * 100, 2),
                    # ── Execution prices ──
                    "exec_price": round(tqqq_exec, 4),
                    "close_same_day": round(tqqq_price, 4),
                    "overnight_gap_pct": round((tqqq_exec / tqqq_price - 1) * 100, 2) if tqqq_price > 0 else 0,
                    # ── Position before ──
                    "shares_before": round(old_tqqq, 4),
                    "cash_before": round(old_cash, 2),
                    "portfolio_value_before_trade": round(pv_before, 2),
                    # ── Trade math ──
                    "shares_delta": round(tqqq_delta, 4),
                    "trade_value_dollars": round(tqqq_trade_val, 2),
                    "commission_dollars": round(tqqq_comm, 2),
                    "slippage_bps_used": round(slip_bps, 1),
                    "slippage_dollars": round(tqqq_slip_dollars, 2),
                    "total_cost_dollars": round(tqqq_comm + tqqq_slip_dollars, 2),
                    "slippage_type": slip_type,
                    # ── Position after ──
                    "shares_after": round(target_tqqq, 4),
                    "cash_after": round(cash, 2),
                    "portfolio_value_at_close": round(pv_at_close, 2),
                    # ── Verification ──
                    "check_pv_equals_cash_plus_positions": round(
                        pv_at_close - (cash + upro_shares * upro_price + tqqq_shares * tqqq_price), 2
                    ),
                    "check_tqqq_alloc_pct_actual": round(
                        (tqqq_shares * tqqq_exec / pv_before) * 100, 2
                    ) if pv_before > 0 else 0,
                })

        # STEP 2: Generate signal for TOMORROW based on today's close
        old_state = state
        spy_close = row["SPY_Close"]
        qqq_close = row["QQQ_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]
        spy_rsi = row["SPY_RSI"]
        qqq_sma50 = row["QQQ_SMA50"]
        spy_below_streak = int(row["SPY_below_50_streak"])
        spy_above_streak = int(row["SPY_above_50_streak"])
        qqq_below_streak = int(row["QQQ_below_50_streak"])
        qqq_above_streak = int(row["QQQ_above_50_streak"])

        new_state, reason = next_state(
            current=old_state,
            spy_close=spy_close,
            spy_sma50=spy_sma50,
            spy_sma200=spy_sma200,
            spy_rsi=spy_rsi,
            vix=vix,
            spy_below_streak=spy_below_streak,
            spy_above_streak=spy_above_streak,
            qqq_close=qqq_close,
            qqq_sma50=qqq_sma50,
            qqq_below_streak=qqq_below_streak,
            qqq_above_streak=qqq_above_streak,
        )

        if new_state != old_state:
            pending_trade = (new_state, reason)

        # ── Classify every signal for the daily log ──
        vix_kill_active = vix > VIX_KILL
        spy_below_200 = spy_close < spy_sma200
        kill_switch_on = vix_kill_active or spy_below_200
        spy_defensive_trigger = spy_below_streak >= SMA_CONFIRM_DAYS
        qqq_defensive_trigger = qqq_below_streak >= SMA_CONFIRM_DAYS
        rsi_hot = spy_rsi > RSI_SELL
        rsi_cool = spy_rsi < RSI_REBUY

        # Portfolio snapshot at close
        portfolio_value = cash + upro_shares * upro_price + tqqq_shares * tqqq_price
        upro_value = upro_shares * upro_price
        tqqq_value = tqqq_shares * tqqq_price
        upro_alloc_pct = (upro_value / portfolio_value * 100) if portfolio_value > 0 else 0
        tqqq_alloc_pct = (tqqq_value / portfolio_value * 100) if portfolio_value > 0 else 0

        daily_log.append({
            # ── Date & State ──
            "date": date.strftime("%Y-%m-%d"),
            "day_number": len(daily_log) + 1,
            "state": state.value,
            "state_changed_today": "YES" if (
                len(trades) > 0
                and trades[-1]["execution_date"] == date.strftime("%Y-%m-%d")
            ) else "NO",
            "pending_signal_for_tomorrow": pending_trade[1] if pending_trade else "NONE",
            # ── Prices ──
            "spy_close": round(spy_close, 2),
            "qqq_close": round(qqq_close, 2),
            "upro_open": round(upro_exec, 4),
            "upro_close": round(upro_price, 4),
            "tqqq_open": round(tqqq_exec, 4),
            "tqqq_close": round(tqqq_price, 4),
            # ── Signal 1: Kill Switch (HIGHEST PRIORITY) ──
            "vix": round(vix, 2),
            "vix_threshold": VIX_KILL,
            "vix_above_threshold": "YES" if vix_kill_active else "NO",
            "spy_sma200": round(spy_sma200, 2),
            "spy_vs_sma200": "ABOVE" if spy_close > spy_sma200 else "BELOW",
            "kill_switch_active": "YES" if kill_switch_on else "NO",
            "kill_reason": (
                ("VIX>" + str(VIX_KILL) if vix_kill_active else "")
                + (" | " if vix_kill_active and spy_below_200 else "")
                + ("SPY<200SMA" if spy_below_200 else "")
            ) if kill_switch_on else "N/A",
            # ── Signal 2: SPY Defensive ──
            "spy_sma50": round(spy_sma50, 2),
            "spy_vs_sma50": "ABOVE" if spy_close > spy_sma50 else "BELOW",
            "days_below_sma50_spy": spy_below_streak,
            "days_above_sma50_spy": spy_above_streak,
            "spy_defensive_trigger": "YES" if spy_defensive_trigger else f"NO (need {SMA_CONFIRM_DAYS}d, have {spy_below_streak}d)",
            # ── Signal 3: QQQ Defensive ──
            "qqq_sma50": round(qqq_sma50, 2),
            "qqq_vs_sma50": "ABOVE" if qqq_close > qqq_sma50 else "BELOW",
            "days_below_sma50_qqq": qqq_below_streak,
            "days_above_sma50_qqq": qqq_above_streak,
            "qqq_defensive_trigger": "YES" if qqq_defensive_trigger else f"NO (need {SMA_CONFIRM_DAYS}d, have {qqq_below_streak}d)",
            # ── Signal 4: RSI ──
            "spy_rsi_14": round(spy_rsi, 2),
            "rsi_zone": "HOT (>75)" if rsi_hot else ("COOL (<60)" if rsi_cool else "NEUTRAL (60-75)"),
            # ── Portfolio ──
            "upro_shares": round(upro_shares, 4),
            "tqqq_shares": round(tqqq_shares, 4),
            "cash": round(cash, 2),
            "upro_value": round(upro_value, 2),
            "tqqq_value": round(tqqq_value, 2),
            "portfolio_value": round(portfolio_value, 2),
            "upro_allocation_pct": round(upro_alloc_pct, 1),
            "tqqq_allocation_pct": round(tqqq_alloc_pct, 1),
            "cash_allocation_pct": round(100 - upro_alloc_pct - tqqq_alloc_pct, 1),
            "daily_pnl": round(
                portfolio_value - (daily_log[-1]["portfolio_value"] if daily_log else INITIAL_CAPITAL), 2
            ),
            "cumulative_return_pct": round((portfolio_value / INITIAL_CAPITAL - 1) * 100, 2),
        })

    metrics = calculate_metrics(daily_log, trades)
    return trades, daily_log, metrics


# =============================================================================
# METRICS
# =============================================================================


def calculate_metrics(daily_log: list[dict], trades: list[dict]) -> dict:
    """Calculate performance metrics."""
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

    state_counts = daily["state"].value_counts()
    total_days_traded = len(daily)
    state_pct = {s: round(c / total_days_traded * 100, 1) for s, c in state_counts.items()}

    daily["year"] = daily.index.year
    annual = daily.groupby("year")["portfolio_value"].agg(["first", "last"])
    annual["return"] = (annual["last"] / annual["first"]) - 1

    spy_start = daily["spy_close"].iloc[0]
    spy_end = daily["spy_close"].iloc[-1]
    spy_total = (spy_end / spy_start) - 1
    spy_cagr = (spy_end / spy_start) ** (1 / years) - 1 if years > 0 else 0

    upro_trades = [t for t in trades if t["instrument"] == "UPRO"]
    tqqq_trades = [t for t in trades if t["instrument"] == "TQQQ"]

    return {
        "step": "Step 2 — UPRO + TQQQ (DataBento)",
        "approach": "Normal (6-state machine)",
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
        "upro_trades": len(upro_trades),
        "tqqq_trades": len(tqqq_trades),
        "total_commissions": round(sum(t["commission_dollars"] for t in trades), 2),
        "total_slippage": round(sum(t["slippage_dollars"] for t in trades), 2),
        "state_distribution_pct": state_pct,
        "annual_returns": {str(y): round(r * 100, 2) for y, r in annual["return"].items()},
        "benchmark_spy_cagr_pct": round(spy_cagr * 100, 2),
        "benchmark_spy_total_return_pct": round(spy_total * 100, 2),
        "alpha_vs_spy_pct": round((cagr - spy_cagr) * 100, 2),
    }


# =============================================================================
# OUTPUT
# =============================================================================


def save_results(trades: list[dict], daily_log: list[dict], metrics: dict) -> None:
    """Save all Step 2 results."""
    trade_df = pd.DataFrame(trades)
    trade_df.to_csv(RESULTS_DIR / "step2_databento_trade_log.csv", index=False)

    daily_df = pd.DataFrame(daily_log)
    daily_df.to_csv(RESULTS_DIR / "step2_databento_portfolio_values.csv", index=False)

    with open(RESULTS_DIR / "step2_databento_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"\n  Saved: step2_databento_trade_log.csv ({len(trades)} trades)")
    print(f"  Saved: step2_databento_portfolio_values.csv ({len(daily_log)} days)")
    print(f"  Saved: step2_databento_metrics.json")


def print_summary(metrics: dict) -> None:
    """Print compact performance summary."""
    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"  {metrics['approach']}")
    print(f"{'=' * 60}")
    print(f"  Period:       {metrics['start_date']} to {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value:  ${metrics['final_value']:,.2f}")
    print(f"  Total Return: {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:         {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:       {metrics['sharpe']:.3f}")
    print(f"  Sortino:      {metrics['sortino']:.3f}")
    print(f"  Calmar:       {metrics['calmar']:.3f}")
    print(f"  Max Drawdown: {metrics['max_drawdown_pct']:.2f}% ({metrics['max_drawdown_date']})")
    print(
        f"  Trades:       {metrics['total_trades']} (UPRO: {metrics['upro_trades']}, TQQQ: {metrics['tqqq_trades']})"
    )
    print(f"  Costs:        ${metrics['total_commissions'] + metrics['total_slippage']:.2f}")
    print(f"\n  SPY B&H CAGR: {metrics['benchmark_spy_cagr_pct']:+.2f}%")
    print(f"  Alpha vs SPY: {metrics['alpha_vs_spy_pct']:+.2f}%")
    print(f"\n  Annual Returns:")
    for yr, ret in sorted(metrics["annual_returns"].items()):
        print(f"    {yr}: {ret:+.2f}%")
    print(f"\n  State Distribution:")
    for st, pct in sorted(metrics["state_distribution_pct"].items()):
        print(f"    {st}: {pct:.1f}%")
    print(f"{'=' * 60}")


def main() -> dict:
    """Run Step 2 backtest and return metrics."""
    print("\n[Step 2] Loading DataBento data (SPY, QQQ, UPRO, TQQQ, VIX)...")
    df = load_step2_data()
    print(f"  Data range: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} bars)")

    print("\n[Step 2] Calculating indicators...")
    df = add_step2_indicators(df)

    print("\n[Step 2] Running 6-state backtest...")
    trades, daily_log, metrics = run_step2_backtest(df)

    print_summary(metrics)
    save_results(trades, daily_log, metrics)

    return metrics


if __name__ == "__main__":
    main()
