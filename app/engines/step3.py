"""
Step 3 engine — Predatory Short (SPXU) overlay.

Confirmed by the client directly (mar1 meeting): activates ONLY when Step 2
is in CASH. Entry: SPY < 200-SMA AND VIX > threshold -> put a slice of the
idle cash into SPXU. Exit: VIX drops back below threshold OR SPY reclaims
its 50-SMA.

Known strategy flaw (not fixed here, disclosed to the client instead): the
exit condition tends to sell this "insurance" once the market has already
calmed down, which is close to the worst time to sell it. This engine
implements the confirmed spec faithfully so the flaw is visible in the
numbers rather than fixed silently.

Unlike audit_import's offline version (which reads Step 2's results from a
saved CSV), this computes Step 2 live so the CASH-day timeline always
matches whatever Step 2 parameters are active right now.
"""

from pathlib import Path

import numpy as np
import pandas as pd


def _load_step3_data(data_dir: Path, full_history: bool = False) -> pd.DataFrame:
    """Load SPY, SPXU, VIX and compute the SMA indicators Step 3 needs.

    full_history: use the 2011-2025 merged dataset (Polygon + DataBento)
    instead of the default ~2019-2026 window.
    """

    def _load(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path, parse_dates=["datetime"])
        df = df.set_index("datetime").sort_index()
        df = df[~df.index.duplicated(keep="first")]
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        return df

    if full_history:
        merged_dir = data_dir / "merged"
        spy = _load(merged_dir / "SPY_daily_full.csv")
        spxu = _load(merged_dir / "SPXU_daily_full.csv")
        vix = _load(merged_dir / "VIX_daily_full.csv")
    else:
        spy = _load(data_dir / "databento" / "equities" / "SPY_daily.csv")
        spxu = _load(data_dir / "databento" / "equities" / "SPXU_daily.csv")
        vix = _load(data_dir / "cboe" / "VIX_daily.csv") if (data_dir / "cboe" / "VIX_daily.csv").exists() \
            else _load(data_dir / "databento" / "equities" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["SPXU_Close"] = spxu["close"].reindex(spy.index)
    df["SPXU_Open"] = spxu["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df = df.ffill().dropna()

    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()
    return df


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    """IBKR tiered commission: $0.005/share, min $1, max 1% of trade value."""
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def run(params: dict, initial_capital: float = 100_000.0) -> dict:
    """Run the Step 3 Predatory Short (SPXU) overlay.

    Accepts both JS-style (dashboard JSON config) and Python-style keys.
    Step 2 is run fresh (in-memory) with its own default params to source
    the CASH-day timeline this overlay activates during.
    """
    from app.config import ensure_data_available
    data_dir = ensure_data_available()

    vix_entry = float(params.get("vixEntry", params.get("vix_entry", 30.0)))
    exit_vix = float(params.get("exitVix", params.get("exit_vix", 30.0)))
    exit_spy_50sma = bool(params.get("exitSpy50sma", params.get("exit_spy_50sma", True)))
    spxu_allocation = float(params.get("spxuAllocation", params.get("spxu_allocation", 50.0))) / 100.0
    spxu_alloc_cap = 0.99
    commission = float(params.get("commission", 1.0))
    slippage_bps = float(params.get("slippage_bps", 20.0))  # always elevated — crash conditions
    full_history = bool(params.get("fullHistory", params.get("full_history", False)))
    start_date = params.get("startDate", params.get("start_date"))
    end_date = params.get("endDate", params.get("end_date"))

    # Pass the same date range through to Step 2 so its CASH-day timeline
    # covers the same window Step 3 is being asked to compute over.
    from app.engines.step2 import run as run_step2
    step2_result = run_step2(
        {"fullHistory": full_history, "startDate": start_date, "endDate": end_date},
        initial_capital,
    )
    step2_state_by_date = {row["date"]: row["state"] for row in step2_result["daily_log"]}

    df = _load_step3_data(data_dir, full_history=full_history)
    trading_df = df.dropna(subset=["SPY_SMA50", "SPY_SMA200"])
    if start_date:
        trading_df = trading_df[trading_df.index >= pd.Timestamp(start_date)]
    if end_date:
        trading_df = trading_df[trading_df.index <= pd.Timestamp(end_date)]
    if trading_df.empty:
        raise ValueError("No valid Step 3 data after SMA warmup period / date filtering")

    cash = initial_capital
    spxu_shares = 0.0
    in_spxu = False
    pending_trade: tuple[str, str] | None = None
    trades: list[dict] = []
    daily_log: list[dict] = []

    for date, row in trading_df.iterrows():
        date_str = date.strftime("%Y-%m-%d")
        spxu_price = row["SPXU_Close"]
        spxu_exec = row["SPXU_Open"]
        vix = row["VIX"]
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]

        step2_state = step2_state_by_date.get(date_str, "UNKNOWN")
        currently_cash = step2_state == "CASH"

        # Safety: if Step 2 just left CASH and we still hold SPXU, force-exit now
        if not currently_cash and in_spxu and pending_trade is None:
            pending_trade = ("SELL", "FORCED_EXIT: Step 2 exited CASH state")

        # Execute pending trade from yesterday's signal at today's open
        if pending_trade is not None:
            action, reason = pending_trade
            pending_trade = None
            pv_before = cash + spxu_shares * spxu_exec
            sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

            if action == "BUY" and spxu_exec > 0:
                invest = min(pv_before * spxu_allocation, pv_before * spxu_alloc_cap)
                slip = invest * (slippage_bps / 10_000)
                comm = ibkr_commission(invest, spxu_exec) if commission != 1.0 else commission
                old_shares = spxu_shares
                old_cash = cash
                spxu_shares = invest / spxu_exec
                cash = pv_before - invest - slip - comm
                in_spxu = True
                pv_after = cash + spxu_shares * spxu_price

                trades.append({
                    "trade_number": len(trades) + 1,
                    "signal_date": sig_date,
                    "execution_date": date_str,
                    "action": "BUY",
                    "instrument": "SPXU",
                    "state_from": "CASH_IDLE",
                    "state_to": "CASH_SPXU",
                    "trigger_reason": reason,
                    "target_allocation_pct": round(spxu_allocation * 100, 1),
                    "exec_price": round(spxu_exec, 4),
                    "shares_before": round(old_shares, 4),
                    "shares_delta": round(spxu_shares, 4),
                    "shares_after": round(spxu_shares, 4),
                    "cash_before": round(old_cash, 2),
                    "cash_after": round(cash, 2),
                    "portfolio_value_before_trade": round(pv_before, 2),
                    "portfolio_value_at_close": round(pv_after, 2),
                    "trade_value_dollars": round(invest, 2),
                    "commission_dollars": round(comm, 2),
                    "slippage_bps_used": slippage_bps,
                    "slippage_dollars": round(slip, 2),
                })
            elif action == "SELL" and spxu_shares > 0:
                sell_val = spxu_shares * spxu_exec
                slip = sell_val * (slippage_bps / 10_000)
                comm = ibkr_commission(sell_val, spxu_exec) if commission != 1.0 else commission
                old_shares = spxu_shares
                old_cash = cash
                cash = cash + sell_val - slip - comm
                spxu_shares = 0.0
                in_spxu = False

                trades.append({
                    "trade_number": len(trades) + 1,
                    "signal_date": sig_date,
                    "execution_date": date_str,
                    "action": "SELL",
                    "instrument": "SPXU",
                    "state_from": "CASH_SPXU",
                    "state_to": "CASH_IDLE",
                    "trigger_reason": reason,
                    "target_allocation_pct": 0.0,
                    "exec_price": round(spxu_exec, 4),
                    "shares_before": round(old_shares, 4),
                    "shares_delta": round(-old_shares, 4),
                    "shares_after": 0.0,
                    "cash_before": round(old_cash, 2),
                    "cash_after": round(cash, 2),
                    "portfolio_value_before_trade": round(pv_before, 2),
                    "portfolio_value_at_close": round(cash, 2),
                    "trade_value_dollars": round(sell_val, 2),
                    "commission_dollars": round(comm, 2),
                    "slippage_bps_used": slippage_bps,
                    "slippage_dollars": round(slip, 2),
                })

        # Generate signal for tomorrow (only meaningful while Step 2 is in CASH)
        if currently_cash:
            spy_below_200 = spy_close < spy_sma200
            spy_above_50 = spy_close > spy_sma50
            vix_above_entry = vix > vix_entry
            vix_below_exit = vix < exit_vix

            if not in_spxu:
                if spy_below_200 and vix_above_entry:
                    pending_trade = (
                        "BUY",
                        f"PREDATORY_SHORT: SPY<200SMA({spy_sma200:.0f}) + VIX={vix:.1f}>{vix_entry}",
                    )
            else:
                exit_parts = []
                if vix_below_exit:
                    exit_parts.append(f"VIX={vix:.1f}<{exit_vix}")
                if exit_spy_50sma and spy_above_50:
                    exit_parts.append(f"SPY>{spy_sma50:.0f}(50SMA)")
                if exit_parts:
                    pending_trade = ("SELL", "EXIT: " + " | ".join(exit_parts))

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
            "date": date_str,
            "state": step3_state,
            "step2_state": step2_state,
            "step3_active": "YES" if currently_cash else "NO",
            "spy_close": round(spy_close, 2),
            "spy_sma50": round(spy_sma50, 2),
            "spy_sma200": round(spy_sma200, 2),
            "vix": round(vix, 2),
            "spxu_open": round(spxu_exec, 4),
            "spxu_close": round(spxu_price, 4),
            "spxu_shares": round(spxu_shares, 4),
            "spxu_value": round(spxu_value, 2),
            "cash": round(cash, 2),
            "portfolio_value": round(portfolio_value, 2),
            "spxu_allocation_pct": round(spxu_alloc_pct, 1),
            "cash_allocation_pct": round(100 - spxu_alloc_pct, 1),
            "cumulative_return_pct": round((portfolio_value / initial_capital - 1) * 100, 2),
        })

    metrics = _compute_metrics(daily_log, trades, initial_capital)
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

    active_days = daily[daily["step3_active"] == "YES"]
    spxu_days = daily[daily["state"] == "CASH_SPXU"]

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
        "days_active_in_cash": len(active_days),
        "days_in_spxu": len(spxu_days),
        "pct_time_in_spxu": round(len(spxu_days) / len(daily) * 100, 1) if len(daily) > 0 else 0,
        "known_strategy_flaw": (
            "The exit rule (VIX back below threshold, or SPY reclaims its 50-day "
            "average) tends to sell this hedge once the market has already calmed "
            "down -- close to the worst time to sell insurance. This is a confirmed "
            "strategy design flaw, not a bug; it has not been fixed."
        ),
        "note_sgov_yield": (
            "SGOV yield on the un-invested share of tactical cash is not included "
            "in these numbers."
        ),
    }
