"""
Step 2 engine wrapper — 6-state UPRO+TQQQ backtest with parameterized thresholds.

Wraps scripts/vam_step2_databento.py without modifying it.
The JS engine in the browser handles live recalc for this strategy;
this Python wrapper is used when server-side backtest is requested.
"""

import sys
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

import vam_step2_databento as _mod
from app.config import DATA_DIR

if DATA_DIR is not None:
    _mod.DATA_DIR = DATA_DIR

# 2011-2025 merged history (Polygon 2011-2019 + DataBento 2020-2025, splits
# already adjusted). Only covers up to 2025-12-30, but is the only place in
# the app where the honest long-horizon result is available to look at.
def _load_full_history_data_uncached(data_dir: Path) -> pd.DataFrame:
    merged_dir = data_dir / "merged"

    def _load(name: str) -> pd.DataFrame:
        df = pd.read_csv(merged_dir / f"{name}_daily_full.csv", parse_dates=["datetime"])
        df = df.set_index("datetime").sort_index()
        return df[~df.index.duplicated(keep="first")]

    spy = _load("SPY")
    qqq = _load("QQQ")
    upro = _load("UPRO")
    tqqq = _load("TQQQ")
    vix = _load("VIX")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["QQQ_Close"] = qqq["close"].reindex(spy.index)
    df["UPRO_Close"] = upro["close"].reindex(spy.index)
    df["UPRO_Open"] = upro["open"].reindex(spy.index)
    df["TQQQ_Close"] = tqqq["close"].reindex(spy.index)
    df["TQQQ_Open"] = tqqq["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    return df.ffill().dropna()


# --- Caching for the optimizer -- see app/engines/step1.py for the full
# explanation. Two layers: raw price data keyed by (data_dir, full_history),
# and indicator-enriched frames additionally keyed by the three params that
# affect indicator computation (sma_def/sma_kill/rsi_period).
@lru_cache(maxsize=8)
def _load_raw_data_cached(data_dir_str: str, full_history: bool) -> pd.DataFrame:
    data_dir = Path(data_dir_str)
    if full_history:
        return _load_full_history_data_uncached(data_dir)
    return _mod.load_step2_data()


_INDICATOR_CACHE: dict[tuple, pd.DataFrame] = {}


def _get_indicator_frame(data_dir: Path, full_history: bool, sma_def: int, sma_kill: int, rsi_period: int) -> pd.DataFrame:
    """Raw data + indicators for this (dataset, periods) combination.

    Always returns a fresh copy -- add_step2_indicators mutates its argument
    in place, and callers further slice/filter the frame, so nothing here
    may hand out a reference to the cached object itself.
    """
    key = (str(data_dir), full_history, sma_def, sma_kill, rsi_period)
    cached = _INDICATOR_CACHE.get(key)
    if cached is None:
        raw = _load_raw_data_cached(str(data_dir), full_history)
        enriched = _mod.add_step2_indicators(raw.copy(), sma_def=sma_def, sma_kill=sma_kill, rsi_period=rsi_period)
        _INDICATOR_CACHE[key] = enriched
        cached = enriched
    return cached.copy()


def _load_full_history_data(data_dir: Path) -> pd.DataFrame:
    """Kept for external callers that want raw data without indicators.
    Internally, run() below goes through the cached path."""
    return _load_full_history_data_uncached(data_dir)


def _next_state(
    current,
    spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
    spy_below_streak, spy_above_streak,
    qqq_close, qqq_sma50, qqq_below_streak, qqq_above_streak,
    *,
    vix_kill, sma_confirm_days, rsi_sell, rsi_rebuy,
    spy_trim_immune: bool = False,
    qqq_trim_immune: bool = False,
    kill_cooldown_active: bool = False,
):
    """Parameterized 6-state machine — no module globals.

    spy_trim_immune / qqq_trim_immune: the 60-day re-entry rule confirmed
    with the client on WhatsApp (2026-05-24). Each sleeve's SMA-50 defensive
    trim is suppressed for 60 days after either a fresh CASH->BULL_FULL
    re-entry (which grants immunity to both sleeves at once) or that sleeve's
    own trim last firing. The VIX / 200-SMA kill switch always fires.

    kill_cooldown_active: an unconfirmed, newly-implemented rule (the
    "Cooldown Days" slider existed in the strategy JSON but had never
    actually been built, in this engine or the browser one — see
    OURS_VS_CLIENT_COMPARISON.md). When True, blocks CASH->BULL_FULL
    re-entry even if conditions are otherwise met, for `cooldown` trading
    days after the kill switch most recently fired. Off by construction
    unless the caller passes a positive cooldown period.
    """
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
        if (spy_close > spy_sma200 and vix < vix_kill
                and spy_close > spy_sma50 and qqq_close > qqq_sma50
                and not kill_cooldown_active):
            return State.BULL_FULL, "RE-ENTRY: all conditions met"
        return State.CASH, "HOLD"

    return current, "NO_TRANSITION"


def run(params: dict, initial_capital: float = 100_000.0) -> dict:
    """Run Step 2 backtest with caller-supplied params.
    Accepts both JS-style (dashboard JSON config) and Python-style keys.
    """
    from app.config import ensure_data_available
    data_dir = ensure_data_available()
    _mod.DATA_DIR = data_dir

    vix_kill = float(params.get("vixThreshold", params.get("vix_kill", 30.0)))
    sma_confirm_days = int(params.get("confirmDays", params.get("sma_confirm_days", 2)))
    rsi_sell = float(params.get("rsiOB", params.get("rsi_sell", 75.0)))
    rsi_rebuy = float(params.get("rsiRe", params.get("rsi_rebuy", 60.0)))
    upro_pct = float(params.get("uproSplit", params.get("upro_pct", 75.0))) / 100.0
    tqqq_pct = 1.0 - upro_pct
    commission = float(params.get("commission", 1.0))
    slip_normal = float(params.get("slippage_bps_normal", 5.0))
    slip_stress = float(params.get("slippage_bps_stress", 20.0))
    reentry_immunity_days = int(params.get("reentryImmunityDays", params.get("reentry_immunity_days", 60)))
    full_history = bool(params.get("fullHistory", params.get("full_history", False)))
    start_date = params.get("startDate", params.get("start_date"))
    end_date = params.get("endDate", params.get("end_date"))
    # SMA/RSI periods — declared in the strategy JSON and honored by the
    # browser JS engine, but silently ignored here until now. Defaults match
    # Ravi's confirmed values.
    sma_kill_period = int(params.get("smaKill", params.get("sma_kill_period", 200)))
    sma_def_period = int(params.get("smaDef", params.get("sma_def_period", 50)))
    rsi_period = int(params.get("rsiPeriod", params.get("rsi_period", 14)))
    # defSell / rsiTrim: also previously ignored. At their defaults (50%,
    # 25%) these reproduce the module's original hardcoded 0.50/0.75 factors
    # exactly (see STATE_ALLOCATION below).
    def_sell_pct = float(params.get("defSell", params.get("def_sell_pct", 50.0))) / 100.0
    rsi_trim_pct = float(params.get("rsiTrim", params.get("rsi_trim_pct", 25.0))) / 100.0
    def_factor = 1.0 - def_sell_pct
    trim_factor = 1.0 - rsi_trim_pct
    # cooldown: an unconfirmed, newly-implemented rule (see _next_state
    # docstring and OURS_VS_CLIENT_COMPARISON.md) — trading days after the
    # kill switch fires during which re-entry is blocked even if conditions
    # clear. The "Cooldown Days" slider existed in the JSON but had never
    # actually been wired anywhere before this.
    cooldown_days = int(params.get("cooldown", params.get("cooldown_days", 5)))

    State = _mod.State
    # Build dynamic allocation from upro_pct/def_factor/trim_factor params.
    # At defaults (defSell=50 -> def_factor=0.50, rsiTrim=25 -> trim_factor=
    # 0.75) this is byte-identical to the original hardcoded ladder.
    STATE_ALLOCATION = {
        State.BULL_FULL:    (upro_pct * 0.99, tqqq_pct * 0.99),
        State.BULL_TRIMMED: (upro_pct * trim_factor, tqqq_pct * trim_factor),
        State.DEF_SPY:      (upro_pct * def_factor, tqqq_pct),
        State.DEF_QQQ:      (upro_pct, tqqq_pct * def_factor),
        State.DEF_BOTH:     (upro_pct * def_factor, tqqq_pct * def_factor),
        State.CASH:         (0.0, 0.0),
    }

    df = _get_indicator_frame(data_dir, full_history, sma_def_period, sma_kill_period, rsi_period)

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
    days_since_spy_trim: int | None = None
    days_since_qqq_trim: int | None = None
    days_since_reentry: int | None = None
    days_since_kill: int | None = None

    # itertuples() instead of iterrows(): see app/engines/step1.py for why --
    # same behavior, ~5-10x less per-row overhead.
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

            # Reset the 60-day immunity clocks on the events that start them.
            # Defined by state membership (not the reason string) so every path
            # into a defensive state is caught, including DEF_SPY/DEF_QQQ
            # worsening into DEF_BOTH.
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
                        "signal_spy_close": round(signal_snapshot["spy_close"], 2),
                        "signal_qqq_close": round(signal_snapshot["qqq_close"], 2),
                        "signal_vix": round(signal_snapshot["vix"], 2),
                        "signal_spy_sma50": round(signal_snapshot["spy_sma50"], 2),
                        "signal_spy_sma200": round(signal_snapshot["spy_sma200"], 2),
                        "signal_spy_rsi": round(signal_snapshot["spy_rsi"], 2),
                        "exec_price": round(exec_p, 4),
                        "shares_delta": round(delta, 4),
                        "trade_value_dollars": round(trade_val, 2),
                        "commission_dollars": round(comm, 2),
                        "slippage_bps_used": round(slip_bps, 1),
                        "slippage_dollars": round(trade_val * (slip_bps / 10000), 2),
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

        new_state, reason = _next_state(
            old_state,
            spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
            spy_below, spy_above, qqq_close, qqq_sma50, qqq_below, qqq_above,
            vix_kill=vix_kill, sma_confirm_days=sma_confirm_days,
            rsi_sell=rsi_sell, rsi_rebuy=rsi_rebuy,
            spy_trim_immune=spy_trim_immune, qqq_trim_immune=qqq_trim_immune,
            kill_cooldown_active=kill_cooldown_active,
        )

        if new_state != old_state:
            pending_trade = (
                new_state,
                reason,
                {
                    "spy_close": spy_close,
                    "qqq_close": qqq_close,
                    "spy_sma50": spy_sma50,
                    "spy_sma200": spy_sma200,
                    "spy_rsi": spy_rsi,
                    "vix": vix,
                },
            )

        portfolio_value = cash + upro_shares * upro_price + tqqq_shares * tqqq_price
        upro_val = upro_shares * upro_price
        tqqq_val = tqqq_shares * tqqq_price
        upro_alloc_pct = (upro_val / portfolio_value * 100) if portfolio_value > 0 else 0
        tqqq_alloc_pct = (tqqq_val / portfolio_value * 100) if portfolio_value > 0 else 0
        state_changed = (
            len(trades) > 0
            and trades[-1]["execution_date"] == date.strftime("%Y-%m-%d")
        )

        daily_log.append({
            "date": date.strftime("%Y-%m-%d"),
            "state": state.value,
            "state_changed_today": "YES" if state_changed else "NO",
            "spy_close": round(spy_close, 2),
            "qqq_close": round(qqq_close, 2),
            "upro_open": round(upro_exec, 4),
            "upro_close": round(upro_price, 4),
            "tqqq_open": round(tqqq_exec, 4),
            "tqqq_close": round(tqqq_price, 4),
            "vix": round(vix, 2),
            "vix_threshold": vix_kill,
            "spy_sma200": round(spy_sma200, 2),
            "spy_sma50": round(spy_sma50, 2),
            "spy_sma_def": round(spy_sma50, 2),
            "spy_sma_kill": round(spy_sma200, 2),
            "days_below_sma50_spy": spy_below,
            "days_above_sma50_spy": spy_above,
            "days_below_sma50_qqq": qqq_below,
            "days_above_sma50_qqq": qqq_above,
            "spy_rsi_14": round(spy_rsi, 2),
            "kill_switch_active": "YES" if (vix > vix_kill or spy_close < spy_sma200) else "NO",
            "portfolio_value": round(portfolio_value, 2),
            "upro_allocation_pct": round(upro_alloc_pct, 1),
            "tqqq_allocation_pct": round(tqqq_alloc_pct, 1),
            "cash_allocation_pct": round(100 - upro_alloc_pct - tqqq_alloc_pct, 1),
            "cumulative_return_pct": round((portfolio_value / initial_capital - 1) * 100, 2),
            # 60-day re-entry rule audit trail
            "spy_trim_immunity_active": "YES" if spy_trim_immune else "NO",
            "qqq_trim_immunity_active": "YES" if qqq_trim_immune else "NO",
            "days_since_last_spy_trim": days_since_spy_trim if days_since_spy_trim is not None else "N/A",
            "days_since_last_qqq_trim": days_since_qqq_trim if days_since_qqq_trim is not None else "N/A",
            "days_since_last_reentry": days_since_reentry if days_since_reentry is not None else "N/A",
            # Kill-switch cooldown audit trail (unconfirmed, newly-implemented rule)
            "kill_cooldown_active": "YES" if kill_cooldown_active else "NO",
            "days_since_last_kill": days_since_kill if days_since_kill is not None else "N/A",
        })

    metrics = _compute_metrics(daily_log, trades, initial_capital)
    metrics["reentry_immunity_days"] = reentry_immunity_days
    metrics["full_history_mode"] = full_history
    metrics["cooldown_days"] = cooldown_days
    return {"trades": trades, "daily_log": daily_log, "metrics": metrics}


def _compute_metrics(daily_log, trades, initial_capital):
    daily = pd.DataFrame(daily_log)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.set_index("date")
    daily["daily_return"] = daily["portfolio_value"].pct_change()
    daily = daily.dropna(subset=["daily_return"])

    years = (daily.index[-1] - daily.index[0]).days / 365.25
    end_val = daily["portfolio_value"].iloc[-1]
    cagr = ((end_val / initial_capital) ** (1 / years) - 1) * 100 if years > 0 else 0

    rets = daily["daily_return"]
    sharpe = (rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else 0
    downside_std = rets[rets < 0].std()
    sortino = (rets.mean() / downside_std * np.sqrt(252)) if downside_std > 0 else 0
    cummax = daily["portfolio_value"].cummax()
    max_dd = ((daily["portfolio_value"] - cummax) / cummax).min() * 100
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    spy_start = daily["spy_close"].iloc[0]
    spy_end = daily["spy_close"].iloc[-1]
    spy_cagr = ((spy_end / spy_start) ** (1 / years) - 1) * 100 if years > 0 else 0

    daily["year"] = daily.index.year
    yearly_returns = {}
    yearly_spy_returns = {}
    for yr, grp in daily.groupby("year"):
        strat_first, strat_last = grp["portfolio_value"].iloc[0], grp["portfolio_value"].iloc[-1]
        spy_first, spy_last = grp["spy_close"].iloc[0], grp["spy_close"].iloc[-1]
        yearly_returns[str(yr)] = round((strat_last / strat_first - 1) * 100, 2)
        yearly_spy_returns[str(yr)] = round((spy_last / spy_first - 1) * 100, 2)

    return {
        "final_value": round(end_val, 2),
        "total_return_pct": round((end_val / initial_capital - 1) * 100, 2),
        "cagr_pct": round(cagr, 2),
        "sharpe": round(sharpe, 3),
        "sortino": round(sortino, 3),
        "calmar": round(calmar, 3),
        "max_drawdown_pct": round(max_dd, 2),
        "total_trades": len(trades),
        "years": round(years, 2),
        "benchmark_spy_cagr_pct": round(spy_cagr, 2),
        "alpha_vs_spy_pct": round(cagr - spy_cagr, 2),
        "yearly_returns": yearly_returns,
        "yearly_spy_returns": yearly_spy_returns,
    }
