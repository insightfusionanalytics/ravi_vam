"""
EXPERIMENTAL variant of Step 2 — NOT wired into the dashboard, the optimizer,
or any strategies/*.json file. This exists purely to test two proposed fixes
for the kill-switch whipsaw pattern documented in the optimization report
(Section 4): the SPY/200-SMA leg of the kill switch fires on a single day's
close with zero confirmation and no buffer, which repeatedly chopped the
strategy in and out during the 2022 bear market (see the 2021-11-22 ->
2023-03-10 drawdown's trade log: 7 kill-switch exits, two of them the day
immediately after a re-entry).

Two independent fixes, each toggleable independently via params so they can
be tested alone or combined:

1. killBufferPct + killConfirmDays: instead of firing the instant SPY closes
   below its 200-SMA, require SPY to close `killBufferPct`% *below* the
   average, and hold there for `killConfirmDays` consecutive days, before the
   kill switch's SPY leg fires. The VIX leg is left untouched (VIX>threshold
   still fires immediately) -- the diagnosed whipsaw was driven by the SPY
   leg chopping around its own average, not by VIX spikes.
   Defaults (0, 1) reproduce today's behavior exactly.

2. killReentryImmunityDays: after a fresh CASH->BULL_FULL re-entry, suppress
   the SPY/200-SMA leg specifically (not VIX) for this many trading days --
   the same *kind* of protection the defensive trim already gets for 60 days,
   sized much shorter here since this is meant to stop the "re-entered
   Monday, killed Tuesday" pattern, not to blind the kill switch for months.
   Default (0) reproduces today's behavior exactly.

Everything else (data loading, indicator computation, trim/RSI logic, cost
model, metrics) is unchanged from app/engines/step2.py -- only imported, not
duplicated, except the exit-condition portion of _next_state and the main
loop's tracking of the new streak/immunity counters.
"""

import pandas as pd

import app.engines.step2 as _step2

_mod = _step2._mod


def _next_state_killfix(
    current,
    spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
    spy_below_streak, spy_above_streak,
    qqq_close, qqq_sma50, qqq_below_streak, qqq_above_streak,
    *,
    vix_kill, sma_confirm_days, rsi_sell, rsi_rebuy,
    spy_trim_immune: bool = False,
    qqq_trim_immune: bool = False,
    kill_cooldown_active: bool = False,
    kill_spy_leg_active: bool,  # replaces the bare "spy_close < spy_sma200" check
    kill_reentry_immune: bool = False,
):
    State = _mod.State

    if current != State.CASH:
        if vix > vix_kill or (kill_spy_leg_active and not kill_reentry_immune):
            return State.CASH, f"KILL: VIX={vix:.1f}, SPY vs 200SMA (buffered/confirmed)"

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
        if (spy_close > spy_sma200 and vix < vix_kill
                and spy_close > spy_sma50 and qqq_close > qqq_sma50
                and not kill_cooldown_active):
            return State.BULL_FULL, "RE-ENTRY: all conditions met"
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

    # --- The two experimental fixes ---
    # killBufferBps (integer, basis points) is the search-space-facing param --
    # Optuna's suggest_params only does integers (see search_space.py). 100bps
    # = 1.0%. killBufferPct is kept as a fallback for direct manual calls.
    if "killBufferBps" in params:
        kill_buffer_pct = float(params["killBufferBps"]) / 100.0
    else:
        kill_buffer_pct = float(params.get("killBufferPct", 0.0))
    kill_confirm_days = int(params.get("killConfirmDays", 1))
    kill_reentry_immunity_days = int(params.get("killReentryImmunityDays", 0))

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
    spy_below_buffered_streak = 0  # new: consecutive days SPY has closed >killBufferPct% below its 200-SMA

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
        kill_reentry_immune = (
            days_since_reentry is not None and days_since_reentry < kill_reentry_immunity_days
        )

        # Buffered/confirmed SPY leg of the kill switch
        buffered_threshold = spy_sma200 * (1 - kill_buffer_pct / 100.0)
        if spy_close < buffered_threshold:
            spy_below_buffered_streak += 1
        else:
            spy_below_buffered_streak = 0
        kill_spy_leg_active = spy_below_buffered_streak >= kill_confirm_days

        new_state, reason = _next_state_killfix(
            old_state,
            spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
            spy_below, spy_above, qqq_close, qqq_sma50, qqq_below, qqq_above,
            vix_kill=vix_kill, sma_confirm_days=sma_confirm_days,
            rsi_sell=rsi_sell, rsi_rebuy=rsi_rebuy,
            spy_trim_immune=spy_trim_immune, qqq_trim_immune=qqq_trim_immune,
            kill_cooldown_active=kill_cooldown_active,
            kill_spy_leg_active=kill_spy_leg_active,
            kill_reentry_immune=kill_reentry_immune,
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
