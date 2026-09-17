"""
Step 1 engine wrapper — 4-state UPRO-only backtest with parameterized thresholds.

The original script (scripts/vam_step1_databento.py) uses module-level constants.
This wrapper runs the same state machine logic with caller-supplied params,
leaving the original script 100% untouched.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add scripts/ to path so we can import data-loading and indicator functions
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

import vam_step1_databento as _mod
from app.config import DATA_DIR

# Override the module's DATA_DIR to the discovered data location
if DATA_DIR is not None:
    _mod.DATA_DIR = DATA_DIR

# 2011-2025 merged history (Polygon 2011-2019 + DataBento 2020-2025, splits
# already adjusted, verified continuous across every UPRO/TQQQ split date).
# Only covers up to 2025-12-30 -- older but far more honest than the default
# 2019-2026 window, which is the only window ever shown to the client.
FULL_HISTORY_START = "2011-01-01"


def _load_full_history_data(data_dir: Path) -> pd.DataFrame:
    merged_dir = data_dir / "merged"

    def _load(name: str) -> pd.DataFrame:
        df = pd.read_csv(merged_dir / f"{name}_daily_full.csv", parse_dates=["datetime"])
        df = df.set_index("datetime").sort_index()
        return df[~df.index.duplicated(keep="first")]

    spy = _load("SPY")
    upro = _load("UPRO")
    vix = _load("VIX")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["UPRO_Close"] = upro["close"].reindex(spy.index)
    df["UPRO_Open"] = upro["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    return df.ffill().dropna()


def _next_state(
    current,
    spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
    below_streak, above_streak,
    *,
    vix_kill, sma_confirm_days, rsi_sell, rsi_rebuy,
    trim_immune: bool = False,
):
    """Parameterized version of the Step 1 state machine — no module globals.

    trim_immune: when True, the SMA-50 defensive trim (below_streak based) is
    suppressed. This is the 60-day re-entry rule confirmed with the client on
    WhatsApp (2026-05-24): a fresh CASH->BULL_100 re-entry, or the trim having
    already fired recently, grants 60 days of immunity from this specific
    trigger so it can't immediately chop a fresh position back down. The VIX
    / 200-SMA kill switch is never affected by this — it always fires.
    """
    State = _mod.State

    if current != State.CASH:
        if vix > vix_kill or spy_close < spy_sma200:
            return State.CASH, f"KILL: VIX={vix:.1f}, SPY vs 200SMA"

    if current == State.BULL_100:
        if below_streak >= sma_confirm_days and not trim_immune:
            return State.DEFENSIVE, f"DEFENSIVE: {sma_confirm_days}d below 50-SMA"
        if spy_rsi > rsi_sell:
            return State.BULL_TRIMMED, f"RSI TRIM: RSI={spy_rsi:.1f}>{rsi_sell}"
        return State.BULL_100, "HOLD"

    if current == State.BULL_TRIMMED:
        if below_streak >= sma_confirm_days and not trim_immune:
            return State.DEFENSIVE, "DEFENSIVE from TRIMMED"
        if spy_rsi < rsi_rebuy:
            return State.BULL_100, f"RSI RECOVERY: RSI={spy_rsi:.1f}<{rsi_rebuy}"
        return State.BULL_TRIMMED, "HOLD"

    if current == State.DEFENSIVE:
        if above_streak >= sma_confirm_days and vix < vix_kill:
            return State.BULL_100, f"DEF EXIT: {sma_confirm_days}d above 50-SMA"
        return State.DEFENSIVE, "HOLD"

    if current == State.CASH:
        if spy_close > spy_sma200 and vix < vix_kill and spy_close > spy_sma50:
            return State.BULL_100, f"RE-ENTRY: SPY above 200SMA+50SMA, VIX<{vix_kill}"
        return State.CASH, "HOLD"

    return current, "NO_TRANSITION"


def run(params: dict, initial_capital: float = 100_000.0) -> dict:
    """Run Step 1 backtest with caller-supplied params.
    Accepts both JS-style keys (from dashboard JSON config) and Python-style keys.
    """
    from app.config import ensure_data_available
    data_dir = ensure_data_available()
    _mod.DATA_DIR = data_dir

    # Accept both JS-style (dashboard) and Python-style (API) keys
    vix_kill = float(params.get("vixThreshold", params.get("vix_kill", 30.0)))
    sma_confirm_days = int(params.get("confirmDays", params.get("sma_confirm_days", 2)))
    rsi_sell = float(params.get("rsiOB", params.get("rsi_sell", 75.0)))
    rsi_rebuy = float(params.get("rsiRe", params.get("rsi_rebuy", 60.0)))
    commission = float(params.get("commission", 1.0))
    slip_normal = float(params.get("slippage_bps_normal", 5.0))
    slip_stress = float(params.get("slippage_bps_stress", 20.0))
    # 60-day re-entry rule (confirmed with client on WhatsApp, 2026-05-24):
    # the SMA-50 defensive trim is suppressed for this many days after either
    # (a) a fresh CASH->BULL_100 re-entry, or (b) the trim last fired.
    reentry_immunity_days = int(params.get("reentryImmunityDays", params.get("reentry_immunity_days", 60)))
    # fullHistory: use the 2011-2025 merged dataset instead of the default
    # 2019-2026 window. This is the only place in the app where the honest,
    # long-horizon result (where the strategy's edge over SPY nearly
    # disappears) is available to look at, rather than buried in an audit log.
    full_history = bool(params.get("fullHistory", params.get("full_history", False)))
    start_date = params.get("startDate", params.get("start_date"))
    end_date = params.get("endDate", params.get("end_date"))

    State = _mod.State
    STATE_ALLOCATION = _mod.STATE_ALLOCATION

    if full_history:
        df = _load_full_history_data(data_dir)
        df = _mod.add_step1_indicators(df)
    else:
        df = _mod.load_step1_data()
        df = _mod.add_step1_indicators(df)

    trading_df = df.dropna(subset=["SPY_SMA200", "SPY_RSI", "UPRO_Open"])
    if start_date:
        trading_df = trading_df[trading_df.index >= pd.Timestamp(start_date)]
    if end_date:
        trading_df = trading_df[trading_df.index <= pd.Timestamp(end_date)]
    if trading_df.empty:
        raise ValueError("No valid trading data after indicator warmup / date filtering")

    cash = initial_capital
    shares = 0.0
    state = State.CASH
    pending_trade = None
    trades: list[dict] = []
    daily_log: list[dict] = []
    # None = event never happened yet, so immunity does not apply
    days_since_trim: int | None = None
    days_since_reentry: int | None = None

    for date, row in trading_df.iterrows():
        upro_price = row["UPRO_Close"]
        upro_open = row["UPRO_Open"]
        vix = row["VIX"]

        # Execute pending trade at today's open (T+1)
        if pending_trade is not None:
            new_state, reason = pending_trade
            pending_trade = None

            old_state_val = state.value
            target_alloc = STATE_ALLOCATION[new_state]
            pv_before = cash + shares * upro_open
            target_shares = (pv_before * target_alloc) / upro_open if upro_open > 0 else 0

            shares_delta = target_shares - shares
            trade_value = abs(shares_delta * upro_open)
            comm = commission if trade_value > 0 else 0
            is_kill = "KILL" in reason
            slip_bps = slip_stress if (is_kill or vix > 25) else slip_normal
            slip = trade_value * (slip_bps / 10000)

            old_shares = shares
            old_cash = cash
            shares = target_shares
            cash = pv_before - (target_shares * upro_open) - comm - slip
            state = new_state

            # Reset the 60-day immunity clocks on the events that start them
            if new_state == State.DEFENSIVE:
                days_since_trim = 0
            if new_state == State.BULL_100 and old_state_val == State.CASH.value:
                days_since_reentry = 0

            pv_after_close = cash + shares * upro_price
            trades.append({
                "trade_number": len(trades) + 1,
                "signal_date": (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d"),
                "execution_date": date.strftime("%Y-%m-%d"),
                "action": "BUY" if shares_delta > 0 else "SELL",
                "instrument": "UPRO",
                "state_from": old_state_val,
                "state_to": new_state.value,
                "trigger_reason": reason,
                "target_allocation_pct": round(target_alloc * 100, 2),
                "exec_price": round(upro_open, 4),
                "shares_before": round(old_shares, 4),
                "cash_before": round(old_cash, 2),
                "portfolio_value_before_trade": round(pv_before, 2),
                "shares_delta": round(shares_delta, 4),
                "trade_value_dollars": round(trade_value, 2),
                "commission_dollars": round(comm, 2),
                "slippage_bps_used": round(slip_bps, 1),
                "slippage_dollars": round(slip, 2),
                "shares_after": round(target_shares, 4),
                "cash_after": round(cash, 2),
                "portfolio_value_at_close": round(pv_after_close, 2),
            })

        # Generate signal for tomorrow
        old_state = state
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]
        spy_rsi = row["SPY_RSI"]
        below_streak = int(row["SPY_below_50_streak"])
        above_streak = int(row["SPY_above_50_streak"])

        if days_since_trim is not None:
            days_since_trim += 1
        if days_since_reentry is not None:
            days_since_reentry += 1
        trim_immune = (
            (days_since_trim is not None and days_since_trim < reentry_immunity_days)
            or (days_since_reentry is not None and days_since_reentry < reentry_immunity_days)
        )

        new_state, reason = _next_state(
            old_state, spy_close, spy_sma50, spy_sma200, spy_rsi, vix,
            below_streak, above_streak,
            vix_kill=vix_kill, sma_confirm_days=sma_confirm_days,
            rsi_sell=rsi_sell, rsi_rebuy=rsi_rebuy,
            trim_immune=trim_immune,
        )

        if new_state != old_state:
            pending_trade = (new_state, reason)

        portfolio_value = cash + shares * upro_price
        upro_alloc_pct = (shares * upro_price / portfolio_value * 100) if portfolio_value > 0 else 0
        state_changed = (
            len(trades) > 0
            and trades[-1]["execution_date"] == date.strftime("%Y-%m-%d")
        )

        daily_log.append({
            "date": date.strftime("%Y-%m-%d"),
            "state": state.value,
            "state_changed_today": "YES" if state_changed else "NO",
            "spy_close": round(spy_close, 2),
            # Step1 has no QQQ — JS engine expects these fields, use SPY fallback
            "qqq_close": round(spy_close, 2),
            "upro_open": round(upro_open, 4),
            "upro_close": round(upro_price, 4),
            "tqqq_open": 0.0,
            "tqqq_close": 0.0,
            "vix": round(vix, 2),
            "vix_threshold": vix_kill,
            "spy_sma200": round(spy_sma200, 2),
            "spy_sma50": round(spy_sma50, 2),
            # Field names used by JS computeIndicators cache
            "spy_sma_def": round(spy_sma50, 2),
            "spy_sma_kill": round(spy_sma200, 2),
            "days_below_sma50_spy": below_streak,
            "days_above_sma50_spy": above_streak,
            "days_below_sma50_qqq": 0,
            "days_above_sma50_qqq": 0,
            "spy_rsi_14": round(spy_rsi, 2),
            "kill_switch_active": "YES" if (vix > vix_kill or spy_close < spy_sma200) else "NO",
            "portfolio_value": round(portfolio_value, 2),
            "upro_allocation_pct": round(upro_alloc_pct, 1),
            "tqqq_allocation_pct": 0.0,
            "cash_allocation_pct": round(100 - upro_alloc_pct, 1),
            "cumulative_return_pct": round((portfolio_value / initial_capital - 1) * 100, 2),
            # 60-day re-entry rule audit trail
            "trim_immunity_active": "YES" if trim_immune else "NO",
            "days_since_last_trim": days_since_trim if days_since_trim is not None else "N/A",
            "days_since_last_reentry": days_since_reentry if days_since_reentry is not None else "N/A",
        })

    metrics = _compute_metrics(daily_log, trades, initial_capital)
    metrics["reentry_immunity_days"] = reentry_immunity_days
    metrics["full_history_mode"] = full_history
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

    # Year-by-year: strategy vs SPY buy & hold. Uses first/last portfolio
    # value and SPY close within each calendar year, so partial first/last
    # years are labeled honestly rather than presented as a full year.
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
