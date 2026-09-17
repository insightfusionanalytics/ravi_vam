"""
VAM Step 4 Combined (2011-2025 expansion) — Full Portfolio Integration.

Same capital-flow architecture as vam_step4_combined_databento.py:
    - Strategy 1 (UPRO/TQQQ) drives the portfolio during BULL/DEFENSIVE periods.
    - During CASH, Strategy 2 (SPXU) and Strategy 3 (SVIX) co-allocate.
    - SVIX layer remains inactive before 2022-03-30 (Option C: real data only).

Reads SPY/SPXU/VIX from data/merged/* (extends back to 2011) and SVIX from the
existing DataBento source (2022-03-30+). Step 2 daily portfolio values come from
results/2011_backtest/step2_2011_portfolio_values.csv.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

ENGINE_ROOT = Path(__file__).resolve().parent.parent  # repo root (was parents[3] in original monorepo layout)
MERGED_DIR = ENGINE_ROOT / "data" / "merged"
DB_EQ_DIR = ENGINE_ROOT / "data" / "databento" / "equities"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "2011_backtest"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# === CONFIGURABLE PARAMETERS (copied verbatim from vam_step4_combined_databento.py) ===
SPXU_EXIT_VIX = 30
SPXU_EXIT_SPY_50SMA = True
USE_SGOV = True
COMMISSION_MODEL = "IBKR"

SVIX_ENTRY_VIX_LOW: float = 30.0
SVIX_ENTRY_VIX_HIGH: float = 40.0
SVIX_PANIC_VIX: float = 50.0
SVIX_INITIAL_ALLOC: float = 0.10
SVIX_PANIC_ALLOC: float = 0.30
SVIX_SGOV_BUFFER: float = 0.70
SVIX_EXIT_VIX: float = 20.0

STRATEGY_2_CASH_SHARE: float = 0.50
STRATEGY_3_CASH_SHARE: float = 0.50
SPXU_ALLOC: float = STRATEGY_2_CASH_SHARE
VIX_SPXU_ENTRY: float = 30.0
SLIPPAGE_SPXU: float = 20.0
SVIX_LAUNCH_DATE: str = "2022-03-30"
SLIPPAGE_SVIX: float = 20.0
ALLOCATION_CAP: float = 0.99
INITIAL_CAPITAL: float = 100_000.0
RISK_FREE_RATE: float = 0.04


def load_csv(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def load_combined_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    spy = load_csv(MERGED_DIR / "SPY_daily_full.csv")
    spxu = load_csv(MERGED_DIR / "SPXU_daily_full.csv")
    svix = load_csv(DB_EQ_DIR / "SVIX_daily.csv")  # 2022-03-30+ only
    vix = load_csv(MERGED_DIR / "VIX_daily_full.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["SPXU_Close"] = spxu["close"].reindex(spy.index)
    df["SPXU_Open"] = spxu["open"].reindex(spy.index)
    df["SVIX_Close"] = svix["close"].reindex(spy.index)
    df["SVIX_Open"] = svix["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df["data_source"] = spy["source"].reindex(spy.index)

    # Forward-fill price columns (NaN before SVIX launch is expected)
    df = df.dropna(subset=["VIX", "SPY_Close"])
    for col in ["SPXU_Close", "SPXU_Open"]:
        df[col] = df[col].ffill()

    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()
    df["vix_fell"] = df["VIX"].diff() < 0
    df["vix_curve_down"] = df["vix_fell"] & df["vix_fell"].shift(1).fillna(False)

    step2_daily = pd.read_csv(
        RESULTS_DIR / "step2_2011_portfolio_values.csv",
        parse_dates=["date"],
        index_col="date",
    )
    return df, step2_daily


def get_step2_state(date: pd.Timestamp, step2_daily: pd.DataFrame) -> str:
    key = date.normalize()
    return str(step2_daily.loc[key, "state"]) if key in step2_daily.index else "UNKNOWN"


def get_step2_pv(date: pd.Timestamp, step2_daily: pd.DataFrame) -> float:
    key = date.normalize()
    return float(step2_daily.loc[key, "portfolio_value"]) if key in step2_daily.index else 0.0


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def _spxu_slippage(amount: float) -> float:
    return amount * (SLIPPAGE_SPXU / 10_000)


def _svix_slippage(amount: float) -> float:
    return amount * (SLIPPAGE_SVIX / 10_000)


def _build_trade(
    trade_num: int,
    sig_date: str,
    exec_date: pd.Timestamp,
    action: str,
    instrument: str,
    shares_before: float,
    shares_after: float,
    cash_before: float,
    cash_after: float,
    exec_price: float,
    close_price: float,
    pv_before: float,
    pv_after: float,
    trade_value: float,
    slip: float,
    target_alloc_pct: float,
    reason: str,
    comm: float,
) -> dict:
    return {
        "trade_number": trade_num,
        "signal_date": sig_date,
        "execution_date": exec_date.strftime("%Y-%m-%d"),
        "action": action,
        "instrument": instrument,
        "trigger_reason": reason,
        "exec_price": round(exec_price, 4),
        "shares_delta": round(shares_after - shares_before, 4),
        "cash_after": round(cash_after, 2),
        "trade_value_dollars": round(trade_value, 2),
        "slippage_dollars": round(slip, 2),
        "commission_dollars": round(comm, 2),
        "portfolio_value_at_close": round(pv_after, 2),
    }


def run_combined_backtest(
    df: pd.DataFrame, step2_daily: pd.DataFrame
) -> tuple[list[dict], list[dict], dict]:
    valid_df = df.dropna(subset=["SPY_SMA50", "SPY_SMA200"])
    if valid_df.empty:
        raise ValueError("No valid data after SMA warmup")

    combined_pv: float = INITIAL_CAPITAL
    cash: float = INITIAL_CAPITAL
    spxu_shares: float = 0.0
    svix_shares: float = 0.0
    svix_mode: str = "NONE"

    pending_spxu: tuple[str, str] | None = None
    pending_svix: tuple[str, str] | None = None

    trades: list[dict] = []
    daily_log: list[dict] = []
    prev_step2_state: str = "UNKNOWN"
    prev_step2_pv: float = INITIAL_CAPITAL
    prev_combined_pv: float = INITIAL_CAPITAL

    for date, row in valid_df.iterrows():
        vix = row["VIX"]
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]
        spxu_close = row["SPXU_Close"] if not pd.isna(row["SPXU_Close"]) else 0.0
        spxu_exec = (
            row["SPXU_Open"]
            if not pd.isna(row["SPXU_Open"]) and row["SPXU_Open"] > 0
            else spxu_close
        )
        svix_close_raw = row["SVIX_Close"]
        svix_exec_raw = row["SVIX_Open"]
        svix_avail = (
            date >= pd.Timestamp(SVIX_LAUNCH_DATE)
            and not pd.isna(svix_close_raw)
            and svix_close_raw > 0
        )
        svix_close = float(svix_close_raw) if svix_avail else 0.0
        svix_exec = (
            float(svix_exec_raw)
            if svix_avail and not pd.isna(svix_exec_raw) and svix_exec_raw > 0
            else svix_close
        )
        vix_curve_down = bool(row["vix_curve_down"])

        step2_state = get_step2_state(date, step2_daily)
        step2_pv_today = get_step2_pv(date, step2_daily)
        currently_cash = step2_state == "CASH"

        if not currently_cash and prev_step2_state == "CASH":
            if spxu_shares > 0 and pending_spxu is None:
                pending_spxu = ("SELL", "FORCED_EXIT: Step 2 exited CASH state")
            if svix_shares > 0 and pending_svix is None:
                pending_svix = ("SELL_ALL", "FORCED_EXIT: Step 2 exited CASH state")

        sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

        # Execute pending SPXU
        if pending_spxu is not None and spxu_close > 0:
            action, reason = pending_spxu
            pending_spxu = None
            if action == "BUY":
                pv = cash + spxu_shares * spxu_exec + svix_shares * svix_close
                invest = min(pv * SPXU_ALLOC, pv * ALLOCATION_CAP)
                slip = _spxu_slippage(invest)
                comm = ibkr_commission(invest, spxu_exec)
                old_sh = spxu_shares
                new_sh = spxu_shares + invest / spxu_exec
                new_cash = cash - invest - slip - comm
                pv_close = new_cash + new_sh * spxu_close + svix_shares * svix_close
                trades.append(
                    _build_trade(
                        len(trades) + 1,
                        sig_date,
                        date,
                        "BUY",
                        "SPXU",
                        old_sh,
                        new_sh,
                        cash,
                        new_cash,
                        spxu_exec,
                        spxu_close,
                        pv,
                        pv_close,
                        invest,
                        slip,
                        SPXU_ALLOC,
                        reason,
                        comm,
                    )
                )
                cash, spxu_shares = new_cash, new_sh
            elif action == "SELL" and spxu_shares > 0:
                sell_val = spxu_shares * spxu_exec
                slip = _spxu_slippage(sell_val)
                comm = ibkr_commission(sell_val, spxu_exec)
                pv = cash + spxu_shares * spxu_exec + svix_shares * svix_close
                old_sh = spxu_shares
                new_cash = cash + sell_val - slip - comm
                pv_close = new_cash + svix_shares * svix_close
                trades.append(
                    _build_trade(
                        len(trades) + 1,
                        sig_date,
                        date,
                        "SELL",
                        "SPXU",
                        old_sh,
                        0.0,
                        cash,
                        new_cash,
                        spxu_exec,
                        spxu_close,
                        pv,
                        pv_close,
                        sell_val,
                        slip,
                        0.0,
                        reason,
                        comm,
                    )
                )
                cash, spxu_shares = new_cash, 0.0
        elif pending_spxu is not None:
            pending_spxu = None

        # Execute pending SVIX
        if pending_svix is not None and svix_avail:
            action, reason = pending_svix
            pending_svix = None
            if action == "BUY_INIT":
                pv = cash + spxu_shares * spxu_close + svix_shares * svix_exec
                invest = pv * SVIX_INITIAL_ALLOC
                slip = _svix_slippage(invest)
                comm = ibkr_commission(invest, svix_exec)
                old_sh = svix_shares
                new_sh = svix_shares + invest / svix_exec
                new_cash = cash - invest - slip - comm
                pv_close = new_cash + spxu_shares * spxu_close + new_sh * svix_close
                trades.append(
                    _build_trade(
                        len(trades) + 1,
                        sig_date,
                        date,
                        "BUY_INIT",
                        "SVIX",
                        old_sh,
                        new_sh,
                        cash,
                        new_cash,
                        svix_exec,
                        svix_close,
                        pv,
                        pv_close,
                        invest,
                        slip,
                        SVIX_INITIAL_ALLOC,
                        reason,
                        comm,
                    )
                )
                cash, svix_shares, svix_mode = new_cash, new_sh, "INIT"
            elif action == "BUY_PANIC":
                pv = cash + spxu_shares * spxu_close + svix_shares * svix_exec
                current_alloc = (svix_shares * svix_exec) / pv if pv > 0 else 0.0
                add_alloc = max(0.0, SVIX_PANIC_ALLOC - current_alloc)
                invest = pv * add_alloc
                if invest > 1.0:
                    slip = _svix_slippage(invest)
                    comm = ibkr_commission(invest, svix_exec)
                    old_sh = svix_shares
                    new_sh = svix_shares + invest / svix_exec
                    new_cash = cash - invest - slip - comm
                    pv_close = new_cash + spxu_shares * spxu_close + new_sh * svix_close
                    trades.append(
                        _build_trade(
                            len(trades) + 1,
                            sig_date,
                            date,
                            "BUY_PANIC",
                            "SVIX",
                            old_sh,
                            new_sh,
                            cash,
                            new_cash,
                            svix_exec,
                            svix_close,
                            pv,
                            pv_close,
                            invest,
                            slip,
                            SVIX_PANIC_ALLOC,
                            reason,
                            comm,
                        )
                    )
                    cash, svix_shares, svix_mode = new_cash, new_sh, "PANIC"
            elif action == "SELL_ALL" and svix_shares > 0:
                sell_val = svix_shares * svix_exec
                slip = _svix_slippage(sell_val)
                comm = ibkr_commission(sell_val, svix_exec)
                pv = cash + spxu_shares * spxu_close + svix_shares * svix_exec
                old_sh = svix_shares
                new_cash = cash + sell_val - slip - comm
                pv_close = new_cash + spxu_shares * spxu_close
                trades.append(
                    _build_trade(
                        len(trades) + 1,
                        sig_date,
                        date,
                        "SELL_ALL",
                        "SVIX",
                        old_sh,
                        0.0,
                        cash,
                        new_cash,
                        svix_exec,
                        svix_close,
                        pv,
                        pv_close,
                        sell_val,
                        slip,
                        0.0,
                        reason,
                        comm,
                    )
                )
                cash, svix_shares, svix_mode = new_cash, 0.0, "NONE"
        elif pending_svix is not None and not svix_avail:
            pending_svix = None

        if (
            currently_cash
            and prev_step2_state != "CASH"
            and spxu_shares == 0.0
            and svix_shares == 0.0
        ):
            cash = combined_pv

        if currently_cash:
            spxu_val = spxu_shares * spxu_close
            svix_val = svix_shares * svix_close if svix_avail else 0.0
            combined_pv = cash + spxu_val + svix_val
        else:
            if prev_step2_pv > 0 and step2_pv_today > 0:
                daily_return = step2_pv_today / prev_step2_pv
                combined_pv = prev_combined_pv * daily_return
            else:
                combined_pv = prev_combined_pv
            spxu_val = 0.0
            svix_val = 0.0
            cash = combined_pv

        if currently_cash:
            in_spxu = spxu_shares > 0
            in_svix = svix_shares > 0
            if not in_spxu:
                if spy_close < spy_sma200 and vix > VIX_SPXU_ENTRY:
                    pending_spxu = (
                        "BUY",
                        f"PREDATORY_SHORT: SPY<200SMA({spy_sma200:.0f})+VIX={vix:.1f}>30",
                    )
            else:
                exit_parts = []
                if vix < SPXU_EXIT_VIX:
                    exit_parts.append(f"VIX={vix:.1f}<{SPXU_EXIT_VIX}")
                if SPXU_EXIT_SPY_50SMA and spy_close > spy_sma50:
                    exit_parts.append(f"SPY>{spy_sma50:.0f}(50SMA)")
                if exit_parts:
                    pending_spxu = ("SELL", "EXIT: " + " | ".join(exit_parts))

            if svix_avail:
                if not in_svix:
                    if SVIX_ENTRY_VIX_LOW <= vix <= SVIX_ENTRY_VIX_HIGH and vix_curve_down:
                        pending_svix = (
                            "BUY_INIT",
                            f"SVIX_INIT: VIX={vix:.1f} in [30,40], falling 2 days",
                        )
                else:
                    if vix < SVIX_EXIT_VIX:
                        pending_svix = ("SELL_ALL", f"EXIT: VIX={vix:.1f}<{SVIX_EXIT_VIX}")
                    elif svix_mode == "INIT" and vix >= SVIX_PANIC_VIX:
                        pending_svix = (
                            "BUY_PANIC",
                            f"SVIX_PANIC: VIX={vix:.1f}>={SVIX_PANIC_VIX} escalate to 30%",
                        )

        combined_state = step2_state
        if currently_cash:
            in_spxu_now = spxu_shares > 0
            in_svix_now = svix_shares > 0
            if in_spxu_now and in_svix_now:
                combined_state = "CASH_SPXU_SVIX"
            elif in_spxu_now:
                combined_state = "CASH_SPXU"
            elif in_svix_now:
                combined_state = "CASH_SVIX" if svix_mode == "INIT" else "CASH_SVIX_PANIC"
            else:
                combined_state = "CASH_IDLE"

        alloc_frac = (
            (spxu_shares * spxu_close + svix_shares * svix_close) / combined_pv
            if combined_pv > 0
            else 0.0
        )

        daily_log.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "step2_state": step2_state,
                "combined_state": combined_state,
                "vix": round(vix, 2),
                "spy_close": round(spy_close, 2),
                "spxu_close": round(spxu_close, 4) if spxu_close > 0 else 0.0,
                "svix_close": round(svix_close, 4) if svix_avail else 0.0,
                "svix_available": "YES" if svix_avail else "NO",
                "spxu_shares": round(spxu_shares, 4),
                "svix_shares": round(svix_shares, 4),
                "spxu_value": round(spxu_val, 2),
                "svix_value": round(svix_val, 2),
                "cash": round(cash if currently_cash else combined_pv, 2),
                "portfolio_value": round(combined_pv, 2),
                "allocation": round(alloc_frac, 4),
                "data_source": str(row.get("data_source", "")),
            }
        )

        prev_step2_state = step2_state
        prev_step2_pv = step2_pv_today
        prev_combined_pv = combined_pv

    metrics = calculate_combined_metrics(daily_log, trades)
    return trades, daily_log, metrics


def calculate_combined_metrics(daily_log: list[dict], trades: list[dict]) -> dict:
    daily = pd.DataFrame(daily_log)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.set_index("date")
    daily["daily_return"] = daily["portfolio_value"].pct_change()
    daily = daily.dropna(subset=["daily_return"])

    total_days = (daily.index[-1] - daily.index[0]).days
    years = total_days / 365.25
    start_val = INITIAL_CAPITAL
    end_val = float(daily["portfolio_value"].iloc[-1])
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

    spxu_trades = [t for t in trades if t["instrument"] == "SPXU"]
    svix_trades = [t for t in trades if t["instrument"] == "SVIX"]
    bull_days = daily[~daily["step2_state"].isin(["CASH", "UNKNOWN"])]
    cash_days = daily[daily["step2_state"] == "CASH"]
    coexist_days = daily[daily["combined_state"] == "CASH_SPXU_SVIX"]
    max_alloc = float(daily["allocation"].max())
    cap_violated = daily[daily["allocation"] > ALLOCATION_CAP]

    return {
        "step": "Step 4 Combined (2011-2025) — Full Portfolio (Polygon+DataBento)",
        "strategies": [
            "Strategy 1 (UPRO/TQQQ)",
            "Strategy 2 (SPXU)",
            "Strategy 3 (SVIX from 2022)",
        ],
        "start_date": daily.index[0].strftime("%Y-%m-%d"),
        "end_date": daily.index[-1].strftime("%Y-%m-%d"),
        "years": round(years, 2),
        "initial_capital": start_val,
        "final_value": round(end_val, 2),
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe_ratio": round(sharpe, 3),
        "sortino_ratio": round(sortino, 3),
        "calmar_ratio": round(calmar, 3),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "max_drawdown_date": max_dd_date,
        "max_allocation_reached": round(max_alloc, 4),
        "allocation_cap": ALLOCATION_CAP,
        "allocation_cap_violated_days": len(cap_violated),
        "total_trades": len(trades),
        "spxu_trades": len(spxu_trades),
        "svix_trades": len(svix_trades),
        "total_commissions": round(sum(t["commission_dollars"] for t in trades), 2),
        "total_slippage": round(sum(t["slippage_dollars"] for t in trades), 2),
        "days_in_bull_defensive": len(bull_days),
        "days_in_cash": len(cash_days),
        "days_spxu_and_svix_coexist": len(coexist_days),
        "svix_launch_date_used": SVIX_LAUNCH_DATE,
        "note": "SVIX layer active only from 2022-03-30 onwards (Option C: real data only).",
    }


def save_results(trades: list[dict], daily_log: list[dict], metrics: dict) -> None:
    pd.DataFrame(trades).to_csv(RESULTS_DIR / "step4_combined_2011_trade_log.csv", index=False)
    pd.DataFrame(daily_log).to_csv(
        RESULTS_DIR / "step4_combined_2011_portfolio_values.csv", index=False
    )
    with open(RESULTS_DIR / "step4_combined_2011_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"  saved step4_combined_2011_trade_log.csv ({len(trades)} trades)")
    print(f"  saved step4_combined_2011_portfolio_values.csv ({len(daily_log)} days)")
    print("  saved step4_combined_2011_metrics.json")


def print_summary(metrics: dict) -> None:
    print(f"\n{'=' * 65}")
    print(f"  {metrics['step']}")
    print(f"{'=' * 65}")
    print(
        f"  Period:        {metrics['start_date']} -> {metrics['end_date']} ({metrics['years']}y)"
    )
    print(f"  Final Value:   ${metrics['final_value']:,.2f}")
    print(f"  Total Return:  {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:          {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:        {metrics['sharpe_ratio']:.3f}")
    print(f"  Sortino:       {metrics['sortino_ratio']:.3f}")
    print(f"  Max Drawdown:  {metrics['max_drawdown_pct']:.2f}% ({metrics['max_drawdown_date']})")
    print(f"  Total Trades:  {metrics['total_trades']}")
    print(f"  SPXU/SVIX:     {metrics['spxu_trades']} / {metrics['svix_trades']}")
    print(f"  Days BULL/CASH:{metrics['days_in_bull_defensive']} / {metrics['days_in_cash']}")
    print(f"{'=' * 65}")


def main() -> dict:
    print("\n[Combined 2011] Loading merged + Step 2 results...")
    df, step2_daily = load_combined_data()
    print(f"  Range: {df.index[0].date()} -> {df.index[-1].date()} ({len(df)} bars)")

    print("[Combined 2011] Running combined portfolio backtest...")
    trades, daily_log, metrics = run_combined_backtest(df, step2_daily)

    print_summary(metrics)
    save_results(trades, daily_log, metrics)
    return metrics


if __name__ == "__main__":
    main()
