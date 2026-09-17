"""
Step 4 engine — Safety Valve (SVIX) overlay.

*** EXPERIMENTAL -- NOT CONFIRMED BY THE CLIENT ***
Ravi mentioned "two short strategies" in one meeting but only ever discussed
SPXU (Step 3) in specific, confirmed detail. SVIX and every threshold below
were never mentioned in any transcript -- this entire strategy is IFA's own
proposal for what a second, recovery-oriented short-vol play could look
like. It should never be presented to the client as something he asked for.
Every number here needs his explicit confirmation before it means anything.

Concept (activates ONLY when Step 2 is in CASH):
- Initial entry: 10% of tactical cash into SVIX when VIX is in [30, 40] and
  has been falling for 2 consecutive days (an "easing panic" signal).
- Panic escalation: add more to reach 30% total if VIX spikes to 50+.
- Exit: sell everything once VIX drops below 20.
- SVIX only existed from 2022-03-30 onward -- inactive before that date,
  no proxy or backfill used.
"""

from pathlib import Path

import numpy as np
import pandas as pd

SVIX_LAUNCH_DATE = "2022-03-30"


def _load_step4_data(data_dir: Path) -> pd.DataFrame:
    def _load(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path, parse_dates=["datetime"])
        df = df.set_index("datetime").sort_index()
        df = df[~df.index.duplicated(keep="first")]
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        return df

    spy = _load(data_dir / "databento" / "equities" / "SPY_daily.csv")
    svix = _load(data_dir / "databento" / "equities" / "SVIX_daily.csv")
    vix = _load(data_dir / "cboe" / "VIX_daily.csv") if (data_dir / "cboe" / "VIX_daily.csv").exists() \
        else _load(data_dir / "databento" / "equities" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SVIX_Close"] = svix["close"].reindex(spy.index)
    df["SVIX_Open"] = svix["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df = df.ffill().dropna(subset=["VIX"])

    df["vix_fell"] = df["VIX"].diff() < 0
    df["vix_curve_down"] = df["vix_fell"] & df["vix_fell"].shift(1).fillna(False)
    return df


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def _is_svix_available(date: pd.Timestamp) -> bool:
    return date >= pd.Timestamp(SVIX_LAUNCH_DATE)


def run(params: dict, initial_capital: float = 100_000.0) -> dict:
    """Run the Step 4 Safety Valve (SVIX) overlay. EXPERIMENTAL -- see module docstring."""
    from app.config import ensure_data_available
    data_dir = ensure_data_available()

    entry_vix_low = float(params.get("entryVixLow", params.get("entry_vix_low", 30.0)))
    entry_vix_high = float(params.get("entryVixHigh", params.get("entry_vix_high", 40.0)))
    panic_vix = float(params.get("panicVix", params.get("panic_vix", 50.0)))
    exit_vix = float(params.get("exitVix", params.get("exit_vix", 20.0)))
    initial_alloc = float(params.get("initialAlloc", params.get("initial_alloc", 10.0))) / 100.0
    panic_alloc = float(params.get("panicAlloc", params.get("panic_alloc", 30.0))) / 100.0
    sgov_buffer = float(params.get("sgovBuffer", params.get("sgov_buffer", 70.0))) / 100.0
    slippage_bps = float(params.get("slippage_bps", 20.0))
    full_history = bool(params.get("fullHistory", params.get("full_history", False)))
    start_date = params.get("startDate", params.get("start_date"))
    end_date = params.get("endDate", params.get("end_date"))

    # SVIX itself only exists from 2022-03-30 regardless of full_history, but
    # Step 2's CASH-day timeline needs to cover the same window so lookups
    # by date don't come back UNKNOWN.
    from app.engines.step2 import run as run_step2
    step2_result = run_step2(
        {"fullHistory": full_history, "startDate": start_date, "endDate": end_date},
        initial_capital,
    )
    step2_state_by_date = {row["date"]: row["state"] for row in step2_result["daily_log"]}

    df = _load_step4_data(data_dir)
    if start_date:
        df = df[df.index >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df.index <= pd.Timestamp(end_date)]

    cash = initial_capital
    svix_shares = 0.0
    svix_mode = "NONE"  # NONE / INIT / PANIC
    pending_trade: tuple[str, str] | None = None
    trades: list[dict] = []
    daily_log: list[dict] = []

    for date, row in df.iterrows():
        date_str = date.strftime("%Y-%m-%d")
        vix = row["VIX"]
        svix_close = row["SVIX_Close"]
        svix_open = row["SVIX_Open"]
        vix_curve_down = bool(row["vix_curve_down"])
        svix_avail = _is_svix_available(date) and not pd.isna(svix_close) and svix_close > 0
        svix_exec = svix_open if svix_avail and not pd.isna(svix_open) and svix_open > 0 else svix_close

        step2_state = step2_state_by_date.get(date_str, "UNKNOWN")
        currently_cash = step2_state == "CASH"
        in_svix = svix_shares > 0

        if not currently_cash and in_svix and pending_trade is None:
            pending_trade = ("SELL_ALL", "FORCED_EXIT: Step 2 exited CASH state")

        if pending_trade is not None and svix_avail:
            action, reason = pending_trade
            pending_trade = None
            pv = cash + svix_shares * svix_exec
            sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

            if action == "BUY_INIT":
                invest = min(pv * initial_alloc, pv * (1.0 - sgov_buffer))
                slip = invest * (slippage_bps / 10_000)
                comm = ibkr_commission(invest, svix_exec)
                old_shares = svix_shares
                svix_shares = svix_shares + invest / svix_exec
                cash = pv - svix_shares * svix_exec - slip - comm
                svix_mode = "INIT"
                trades.append(_trade_record(len(trades) + 1, sig_date, date_str, "BUY_INIT",
                                             invest, old_shares, svix_shares, svix_exec,
                                             initial_alloc, reason, slip, comm))
            elif action == "BUY_PANIC":
                current_alloc = (svix_shares * svix_exec) / pv if pv > 0 else 0.0
                target_alloc = min(panic_alloc, 1.0 - sgov_buffer)
                add_alloc = max(0.0, target_alloc - current_alloc)
                invest = pv * add_alloc
                if invest > 0:
                    slip = invest * (slippage_bps / 10_000)
                    comm = ibkr_commission(invest, svix_exec)
                    old_shares = svix_shares
                    svix_shares = svix_shares + invest / svix_exec
                    cash = cash - invest - slip - comm
                    svix_mode = "PANIC"
                    trades.append(_trade_record(len(trades) + 1, sig_date, date_str, "BUY_PANIC",
                                                 invest, old_shares, svix_shares, svix_exec,
                                                 target_alloc, reason, slip, comm))
            elif action == "SELL_ALL" and svix_shares > 0:
                sell_val = svix_shares * svix_exec
                slip = sell_val * (slippage_bps / 10_000)
                comm = ibkr_commission(sell_val, svix_exec)
                old_shares = svix_shares
                cash = cash + sell_val - slip - comm
                svix_shares = 0.0
                svix_mode = "NONE"
                trades.append(_trade_record(len(trades) + 1, sig_date, date_str, "SELL_ALL",
                                             sell_val, old_shares, 0.0, svix_exec,
                                             0.0, reason, slip, comm))
        elif pending_trade is not None and not svix_avail:
            pending_trade = None

        in_svix = svix_shares > 0

        if currently_cash and svix_avail:
            if not in_svix:
                if entry_vix_low <= vix <= entry_vix_high and vix_curve_down:
                    pending_trade = ("BUY_INIT", f"SVIX_INIT: VIX={vix:.1f} in [{entry_vix_low},{entry_vix_high}], falling 2 days")
            else:
                if vix < exit_vix:
                    pending_trade = ("SELL_ALL", f"EXIT: VIX={vix:.1f}<{exit_vix}")
                elif svix_mode == "INIT" and vix >= panic_vix:
                    pending_trade = ("BUY_PANIC", f"SVIX_PANIC: VIX={vix:.1f}>={panic_vix} escalating")

        svix_value = svix_shares * svix_close if svix_avail else 0.0
        portfolio_value = cash + svix_value
        svix_alloc_pct = (svix_value / portfolio_value * 100) if portfolio_value > 0 else 0.0

        if in_svix and svix_mode == "PANIC":
            state = "CASH_SVIX_PANIC"
        elif in_svix:
            state = "CASH_SVIX_INIT"
        elif currently_cash:
            state = "CASH_IDLE"
        else:
            state = "INACTIVE"

        daily_log.append({
            "date": date_str,
            "state": state,
            "step2_state": step2_state,
            "step4_active": "YES" if currently_cash else "NO",
            "svix_available": "YES" if svix_avail else "NO",
            "vix": round(vix, 2),
            "svix_close": round(svix_close, 4) if svix_avail else 0.0,
            "svix_shares": round(svix_shares, 4),
            "svix_value": round(svix_value, 2),
            "cash": round(cash, 2),
            "portfolio_value": round(portfolio_value, 2),
            "svix_allocation_pct": round(svix_alloc_pct, 1),
            "cash_allocation_pct": round(100 - svix_alloc_pct, 1),
            "cumulative_return_pct": round((portfolio_value / initial_capital - 1) * 100, 2),
        })

    metrics = _compute_metrics(daily_log, trades, initial_capital)
    metrics["full_history_mode"] = full_history
    return {"trades": trades, "daily_log": daily_log, "metrics": metrics}


def _trade_record(num, sig_date, exec_date, action, trade_value, shares_before, shares_after,
                   exec_price, target_alloc, reason, slip, comm):
    return {
        "trade_number": num,
        "signal_date": sig_date,
        "execution_date": exec_date,
        "action": action,
        "instrument": "SVIX",
        "trigger_reason": reason,
        "target_allocation_pct": round(target_alloc * 100, 1),
        "exec_price": round(exec_price, 4),
        "shares_before": round(shares_before, 4),
        "shares_after": round(shares_after, 4),
        "shares_delta": round(shares_after - shares_before, 4),
        "trade_value_dollars": round(trade_value, 2),
        "slippage_dollars": round(slip, 2),
        "commission_dollars": round(comm, 2),
    }


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

    svix_days = daily[daily["state"].isin(["CASH_SVIX_INIT", "CASH_SVIX_PANIC"])]

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
        "days_in_svix": len(svix_days),
        "svix_launch_date_used": SVIX_LAUNCH_DATE,
        "experimental_warning": (
            "EXPERIMENTAL: this entire strategy (SVIX, every threshold used here) "
            "was never mentioned by the client in any meeting. It is IFA's own "
            "proposal for a second short strategy. Do not present this as something "
            "Ravi asked for -- it needs his explicit confirmation before it means "
            "anything."
        ),
    }
