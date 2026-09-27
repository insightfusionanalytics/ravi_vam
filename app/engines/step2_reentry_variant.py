"""
EXPERIMENTAL variant of Step 2 — NOT wired into the dashboard, the optimizer,
or any strategies/*.json file.

Tests the re-entry side of the whipsaw problem instead of the exit side
(see app/engines/step2_killfix_variant.py, which tested the exit side and
failed the sealed holdout). The decomposition in the optimization report
(Section 4) found that within a whipsaw episode, the market typically
bottoms in the first 15-20% of the episode and recovers +5.9% to +7.1%
before re-entry finally fires, roughly double the -2.3% to -3.0% of extra
decline the exit correctly avoided. That points at re-entry lag, not exit
twitchiness, as the larger cost -- and it was never tested on its own.

Today's re-entry from CASH requires ALL FOUR conditions simultaneously:
  1. SPY close > SPY 200-SMA   (the kill switch's own recovery condition)
  2. VIX < vix_kill
  3. SPY close > SPY 50-SMA
  4. QQQ close > QQQ 50-SMA
(plus the separate cooldown gate, unchanged here). Requiring all four to
clear on the same day is what causes the lag: after a sharp decline, price
typically reclaims its 200-SMA before it reclaims the faster-moving 50-SMA,
so the strategy sits out waiting for a condition that's structurally slower
to confirm than the recovery itself.

New param: reentryMinConditions (2, 3, or 4). Re-entry fires once at least
this many of the four conditions are true, instead of requiring all four.
Default (4) reproduces today's behavior exactly.
"""

import pandas as pd

import app.engines.step2 as _step2

_mod = _step2._mod


def _next_state_reentry(
    current,
    spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
    spy_below_streak, spy_above_streak,
    qqq_close, qqq_sma50, qqq_below_streak, qqq_above_streak,
    *,
    vix_kill, sma_confirm_days, rsi_sell, rsi_rebuy,
    spy_trim_immune: bool = False,
    qqq_trim_immune: bool = False,
    kill_cooldown_active: bool = False,
    reentry_min_conditions: int = 4,
):
    State = _mod.State

    if current != State.CASH:
        if vix > vix_kill or spy_close < spy_sma200:
            return State.CASH, f"KILL: VIX={vix:.1f}, SPY vs 200SMA"

    spy_below = spy_below_streak >= sma_confirm_days and not spy_trim_immune
    qqq_below = qqq_below_streak >= sma_confirm_days and not qqq_trim_immune
    spy_above = spy_above_streak >= sma_confirm_days
    qqq_above = qqq_above_streak >= sma_confirm_days

    if current in (State.BULL_FULL, State.BULL_TRIMMED):
        if spy_below and qqq_below:
            return State.DEF_BOTH, "DEFENSIVE BOTH"
        if spy_below:
            return State.DEF_SPY, "DEFENSIVE SPY"
        if qqq_below:
            return State.DEF_QQQ, "DEFENSIVE QQQ"
        if current == State.BULL_FULL and spy_rsi > rsi_sell:
            return State.BULL_TRIMMED, f"RSI TRIM: RSI={spy_rsi:.1f}"
        if current == State.BULL_TRIMMED and spy_rsi < rsi_rebuy:
            return State.BULL_FULL, f"RSI RECOVERY: RSI={spy_rsi:.1f}"
        return current, "HOLD"

    if current == State.DEF_SPY:
        if qqq_below:
            return State.DEF_BOTH, "WORSENING"
        if spy_above and vix < vix_kill:
            return State.BULL_FULL, "RECOVERY SPY"
        return State.DEF_SPY, "HOLD"

    if current == State.DEF_QQQ:
        if spy_below:
            return State.DEF_BOTH, "WORSENING"
        if qqq_above and vix < vix_kill:
            return State.BULL_FULL, "RECOVERY QQQ"
        return State.DEF_QQQ, "HOLD"

    if current == State.DEF_BOTH:
        if spy_above and qqq_above and vix < vix_kill:
            return State.BULL_FULL, "FULL RECOVERY"
        if spy_above and not qqq_above:
            return State.DEF_QQQ, "PARTIAL SPY"
        if qqq_above and not spy_above:
            return State.DEF_SPY, "PARTIAL QQQ"
        return State.DEF_BOTH, "HOLD"

    if current == State.CASH:
        conditions_met = sum([
            spy_close > spy_sma200,
            vix < vix_kill,
            spy_close > spy_sma50,
            qqq_close > qqq_sma50,
        ])
        if conditions_met >= reentry_min_conditions and not kill_cooldown_active:
            return State.BULL_FULL, f"RE-ENTRY: {conditions_met}/4 conditions met"
        return State.CASH, "HOLD"

    return current, "NO_TRANSITION"


def run(params: dict, initial_capital: float = 100_000.0) -> dict:
    from app.config import ensure_data_available
    data_dir = ensure_data_available()
    _mod.DATA_DIR = data_dir

    vix_kill = float(params.get("vixThreshold", 30.0))
    sma_confirm_days = int(params.get("confirmDays", 2))
    rsi_sell = float(params.get("rsiOB", 75.0))
    rsi_rebuy = float(params.get("rsiRe", 60.0))
    upro_pct = float(params.get("uproSplit", 75.0)) / 100.0
    tqqq_pct = 1.0 - upro_pct
    commission = float(params.get("commission", 1.0))
    slip_normal = float(params.get("slippage_bps_normal", 5.0))
    slip_stress = float(params.get("slippage_bps_stress", 20.0))
    reentry_immunity_days = int(params.get("reentryImmunityDays", 60))
    full_history = bool(params.get("fullHistory", False))
    start_date = params.get("startDate")
    end_date = params.get("endDate")
    sma_kill_period = int(params.get("smaKill", 200))
    sma_def_period = int(params.get("smaDef", 50))
    rsi_period = int(params.get("rsiPeriod", 14))
    def_sell_pct = float(params.get("defSell", 50.0)) / 100.0
    rsi_trim_pct = float(params.get("rsiTrim", 25.0)) / 100.0
    def_factor = 1.0 - def_sell_pct
    trim_factor = 1.0 - rsi_trim_pct
    cooldown_days = int(params.get("cooldown", 5))

    # --- The experimental fix ---
    reentry_min_conditions = int(params.get("reentryMinConditions", 4))

    State = _mod.State
    STATE_ALLOCATION = {
        State.BULL_FULL:    (upro_pct * 0.99, tqqq_pct * 0.99),
        State.BULL_TRIMMED: (upro_pct * trim_factor, tqqq_pct * trim_factor),
        State.DEF_SPY:      (upro_pct * def_factor, tqqq_pct),
        State.DEF_QQQ:      (upro_pct, tqqq_pct * def_factor),
        State.DEF_BOTH:     (upro_pct * def_factor, tqqq_pct * def_factor),
        State.CASH:         (0.0, 0.0),
    }

    df = _step2._get_indicator_frame(data_dir, full_history, sma_def_period, sma_kill_period, rsi_period)
    trading_df = df.dropna(subset=["SPY_SMA200", "SPY_RSI", "QQQ_SMA50", "UPRO_Open", "TQQQ_Open"])
    if start_date:
        trading_df = trading_df[trading_df.index >= pd.Timestamp(start_date)]
    if end_date:
        trading_df = trading_df[trading_df.index <= pd.Timestamp(end_date)]
    if trading_df.empty:
        raise ValueError("No valid trading data after warmup")

    cash = initial_capital
    upro_shares = 0.0
    tqqq_shares = 0.0
    state = State.CASH
    pending_trade = None
    trades: list[dict] = []
    daily_log: list[dict] = []
    days_since_spy_trim = None
    days_since_qqq_trim = None
    days_since_reentry = None
    days_since_kill = None

    for row in trading_df.itertuples():
        date = row.Index
        upro_price = row.UPRO_Close
        tqqq_price = row.TQQQ_Close
        upro_exec = row.UPRO_Open
        tqqq_exec = row.TQQQ_Open
        vix = row.VIX

        if pending_trade is not None:
            new_state, reason, signal_snapshot = pending_trade
            pending_trade = None

            pv_before = cash + upro_shares * upro_exec + tqqq_shares * tqqq_exec
            upro_alloc, tqqq_alloc = STATE_ALLOCATION[new_state]
            is_kill = "KILL" in reason
            slip_bps = slip_stress if (is_kill or vix > 25) else slip_normal

            target_upro = (pv_before * upro_alloc) / upro_exec if upro_exec > 0 else 0
            target_tqqq = (pv_before * tqqq_alloc) / tqqq_exec if tqqq_exec > 0 else 0
            upro_delta = target_upro - upro_shares
            tqqq_delta = target_tqqq - tqqq_shares
            upro_trade_val = abs(upro_delta * upro_exec)
            tqqq_trade_val = abs(tqqq_delta * tqqq_exec)
            upro_comm = commission if upro_trade_val > 0 else 0
            tqqq_comm = commission if tqqq_trade_val > 0 else 0
            total_cost = (
                upro_comm + upro_trade_val * (slip_bps / 10000)
                + tqqq_comm + tqqq_trade_val * (slip_bps / 10000)
            )

            old_state_val = state.value
            upro_shares = target_upro
            tqqq_shares = target_tqqq
            cash = pv_before - (target_upro * upro_exec) - (target_tqqq * tqqq_exec) - total_cost
            state = new_state

            old_spy_defensive = old_state_val in (State.DEF_SPY.value, State.DEF_BOTH.value)
            old_qqq_defensive = old_state_val in (State.DEF_QQQ.value, State.DEF_BOTH.value)
            if new_state in (State.DEF_SPY, State.DEF_BOTH) and not old_spy_defensive:
                days_since_spy_trim = 0
            if new_state in (State.DEF_QQQ, State.DEF_BOTH) and not old_qqq_defensive:
                days_since_qqq_trim = 0
            if new_state == State.BULL_FULL and old_state_val == State.CASH.value:
                days_since_reentry = 0
            if new_state == State.CASH and "KILL" in reason:
                days_since_kill = 0

            exec_date_str = date.strftime("%Y-%m-%d")
            signal_date_str = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")
            for instrument, delta, trade_val, comm, exec_p in [
                ("UPRO", upro_delta, upro_trade_val, upro_comm, upro_exec),
                ("TQQQ", tqqq_delta, tqqq_trade_val, tqqq_comm, tqqq_exec),
            ]:
                if trade_val > 0:
                    trades.append({
                        "trade_number": len(trades) + 1,
                        "signal_date": signal_date_str,
                        "execution_date": exec_date_str,
                        "action": "BUY" if delta > 0 else "SELL",
                        "instrument": instrument,
                        "state_from": old_state_val,
                        "state_to": new_state.value,
                        "trigger_reason": reason,
                        "exec_price": round(exec_p, 4),
                        "shares_delta": round(delta, 4),
                        "trade_value_dollars": round(trade_val, 2),
                    })

        old_state = state
        spy_close = row.SPY_Close
        qqq_close = row.QQQ_Close
        spy_sma50 = row.SPY_SMA50
        spy_sma200 = row.SPY_SMA200
        spy_rsi = row.SPY_RSI
        qqq_sma50 = row.QQQ_SMA50
        spy_below = int(row.SPY_below_50_streak)
        spy_above = int(row.SPY_above_50_streak)
        qqq_below = int(row.QQQ_below_50_streak)
        qqq_above = int(row.QQQ_above_50_streak)

        if days_since_spy_trim is not None:
            days_since_spy_trim += 1
        if days_since_qqq_trim is not None:
            days_since_qqq_trim += 1
        if days_since_reentry is not None:
            days_since_reentry += 1
        if days_since_kill is not None:
            days_since_kill += 1

        spy_trim_immune = (
            (days_since_spy_trim is not None and days_since_spy_trim < reentry_immunity_days)
            or (days_since_reentry is not None and days_since_reentry < reentry_immunity_days)
        )
        qqq_trim_immune = (
            (days_since_qqq_trim is not None and days_since_qqq_trim < reentry_immunity_days)
            or (days_since_reentry is not None and days_since_reentry < reentry_immunity_days)
        )
        kill_cooldown_active = (
            days_since_kill is not None and days_since_kill < cooldown_days
        )

        new_state, reason = _next_state_reentry(
            old_state,
            spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
            spy_below, spy_above, qqq_close, qqq_sma50, qqq_below, qqq_above,
            vix_kill=vix_kill, sma_confirm_days=sma_confirm_days,
            rsi_sell=rsi_sell, rsi_rebuy=rsi_rebuy,
            spy_trim_immune=spy_trim_immune, qqq_trim_immune=qqq_trim_immune,
            kill_cooldown_active=kill_cooldown_active,
            reentry_min_conditions=reentry_min_conditions,
        )

        if new_state != old_state:
            pending_trade = (new_state, reason, {})

        portfolio_value = cash + upro_shares * upro_price + tqqq_shares * tqqq_price
        daily_log.append({
            "date": date.strftime("%Y-%m-%d"),
            "state": state.value,
            "upro_close": round(upro_price, 4),
            "portfolio_value": round(portfolio_value, 2),
            "spy_close": round(spy_close, 2),
        })

    metrics = _step2._compute_metrics(daily_log, trades, initial_capital)
    return {"trades": trades, "daily_log": daily_log, "metrics": metrics}
