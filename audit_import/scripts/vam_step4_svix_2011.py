"""
VAM Step 4 SVIX (2011-2025 expansion) — Safety Valve standalone.

Per Option C: NO synthetic SVIX. The script only operates from the SVIX launch
date (2022-03-30) onwards. Pre-2022 days are simply inactive (cash sits idle in
this standalone view).

SPY/VIX load from data/merged/*_daily_full.csv (extends back to 2011). SVIX
loads from the existing DataBento source (2022-03-30+) since no Polygon SVIX
data was used. Step 2 CASH periods come from results/2011_backtest/.
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

SVIX_LAUNCH_DATE: str = "2022-03-30"

# === STRATEGY 3 PARAMETERS (copied verbatim from vam_step4_svix_databento.py) ===
SVIX_ENTRY_VIX_LOW: float = 30.0
SVIX_ENTRY_VIX_HIGH: float = 40.0
SVIX_PANIC_VIX: float = 50.0
SVIX_INITIAL_ALLOC: float = 0.10
SVIX_PANIC_ALLOC: float = 0.30
SVIX_SGOV_BUFFER: float = 0.70
SVIX_EXIT_VIX: float = 20.0
USE_SGOV = True
COMMISSION_MODEL = "IBKR"

VIX_CURVE_DAYS: int = 2
SVIX_ALLOC_CAP: float = 0.99
SLIPPAGE_BPS_SVIX: float = 20.0
INITIAL_CAPITAL: float = 100_000.0
RISK_FREE_RATE: float = 0.04


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def load_csv(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def load_step4_svix_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    spy = load_csv(MERGED_DIR / "SPY_daily_full.csv")
    svix = load_csv(DB_EQ_DIR / "SVIX_daily.csv")
    vix = load_csv(MERGED_DIR / "VIX_daily_full.csv")

    df = pd.DataFrame(index=spy.index)
    df["SVIX_Close"] = svix["close"].reindex(spy.index)
    df["SVIX_Open"] = svix["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df = df.dropna(subset=["VIX"])
    df["SVIX_Close"] = df["SVIX_Close"].ffill()
    df["SVIX_Open"] = df["SVIX_Open"].ffill()

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


def is_svix_available(date: pd.Timestamp) -> bool:
    return date >= pd.Timestamp(SVIX_LAUNCH_DATE)


def _build_trade(
    trade_num: int,
    sig_date: str,
    exec_date: pd.Timestamp,
    action: str,
    trade_value: float,
    shares_before: float,
    shares_after: float,
    cash_before: float,
    cash_after: float,
    exec_price: float,
    close_price: float,
    pv_before: float,
    pv_after: float,
    slip: float,
    target_alloc: float,
    reason: str,
    comm: float,
) -> dict:
    return {
        "trade_number": trade_num,
        "signal_date": sig_date,
        "execution_date": exec_date.strftime("%Y-%m-%d"),
        "action": action,
        "instrument": "SVIX",
        "trigger_reason": reason,
        "exec_price": round(exec_price, 4),
        "shares_before": round(shares_before, 4),
        "shares_after": round(shares_after, 4),
        "shares_delta": round(shares_after - shares_before, 4),
        "cash_after": round(cash_after, 2),
        "trade_value_dollars": round(trade_value, 2),
        "slippage_dollars": round(slip, 2),
        "commission_dollars": round(comm, 2),
        "portfolio_value_at_close": round(pv_after, 2),
    }


def run_step4_svix_backtest(
    df: pd.DataFrame, step2_daily: pd.DataFrame
) -> tuple[list[dict], list[dict], dict]:
    cash: float = INITIAL_CAPITAL
    svix_shares: float = 0.0
    svix_mode: str = "NONE"
    pending_trade: tuple[str, str] | None = None
    trades: list[dict] = []
    daily_log: list[dict] = []

    valid_rows = df.dropna(subset=["VIX"])

    for date, row in valid_rows.iterrows():
        vix = row["VIX"]
        svix_close_raw = row["SVIX_Close"]
        svix_open_raw = row["SVIX_Open"]
        vix_curve_down = bool(row["vix_curve_down"])
        svix_avail = is_svix_available(date) and not pd.isna(svix_close_raw) and svix_close_raw > 0
        svix_close = float(svix_close_raw) if svix_avail else 0.0
        svix_exec = (
            float(svix_open_raw)
            if svix_avail and not pd.isna(svix_open_raw) and svix_open_raw > 0
            else svix_close
        )
        step2_state = get_step2_state(date, step2_daily)
        currently_cash = step2_state == "CASH"
        in_svix = svix_shares > 0

        if not currently_cash and in_svix and pending_trade is None:
            pending_trade = ("SELL_ALL", "FORCED_EXIT: Step 2 exited CASH state")

        if pending_trade is not None and svix_avail:
            action, reason = pending_trade
            pending_trade = None
            sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

            if action == "BUY_INIT":
                pv = cash + svix_shares * svix_exec
                invest = min(pv * SVIX_INITIAL_ALLOC, pv * (1.0 - SVIX_SGOV_BUFFER))
                slip = invest * (SLIPPAGE_BPS_SVIX / 10_000)
                comm = ibkr_commission(invest, svix_exec)
                old_sh = svix_shares
                new_sh = svix_shares + invest / svix_exec
                new_cash = pv - new_sh * svix_exec - slip - comm
                trades.append(
                    _build_trade(
                        len(trades) + 1,
                        sig_date,
                        date,
                        "BUY_INIT",
                        invest,
                        old_sh,
                        new_sh,
                        cash,
                        new_cash,
                        svix_exec,
                        svix_close,
                        pv,
                        new_cash + new_sh * svix_close,
                        slip,
                        SVIX_INITIAL_ALLOC,
                        reason,
                        comm,
                    )
                )
                cash, svix_shares, svix_mode = new_cash, new_sh, "INIT"

            elif action == "BUY_PANIC":
                pv = cash + svix_shares * svix_exec
                current_alloc = (svix_shares * svix_exec) / pv if pv > 0 else 0.0
                target_alloc = min(SVIX_PANIC_ALLOC, 1.0 - SVIX_SGOV_BUFFER)
                add_alloc = max(0.0, target_alloc - current_alloc)
                invest = pv * add_alloc
                if invest > 0:
                    slip = invest * (SLIPPAGE_BPS_SVIX / 10_000)
                    comm = ibkr_commission(invest, svix_exec)
                    old_sh = svix_shares
                    new_sh = svix_shares + invest / svix_exec
                    new_cash = cash - invest - slip - comm
                    trades.append(
                        _build_trade(
                            len(trades) + 1,
                            sig_date,
                            date,
                            "BUY_PANIC",
                            invest,
                            old_sh,
                            new_sh,
                            cash,
                            new_cash,
                            svix_exec,
                            svix_close,
                            pv,
                            new_cash + new_sh * svix_close,
                            slip,
                            target_alloc,
                            reason,
                            comm,
                        )
                    )
                    cash, svix_shares, svix_mode = new_cash, new_sh, "PANIC"

            elif action == "SELL_ALL" and svix_shares > 0:
                sell_val = svix_shares * svix_exec
                slip = sell_val * (SLIPPAGE_BPS_SVIX / 10_000)
                comm = ibkr_commission(sell_val, svix_exec)
                pv = cash + svix_shares * svix_exec
                old_sh = svix_shares
                new_cash = cash + sell_val - slip - comm
                trades.append(
                    _build_trade(
                        len(trades) + 1,
                        sig_date,
                        date,
                        "SELL_ALL",
                        sell_val,
                        old_sh,
                        0.0,
                        cash,
                        new_cash,
                        svix_exec,
                        svix_close,
                        pv,
                        new_cash,
                        slip,
                        0.0,
                        reason,
                        comm,
                    )
                )
                cash, svix_shares, svix_mode = new_cash, 0.0, "NONE"
        elif pending_trade is not None and not svix_avail:
            pending_trade = None

        in_svix = svix_shares > 0

        if currently_cash and svix_avail:
            if not in_svix:
                if SVIX_ENTRY_VIX_LOW <= vix <= SVIX_ENTRY_VIX_HIGH and vix_curve_down:
                    pending_trade = (
                        "BUY_INIT",
                        f"SVIX_INIT: VIX={vix:.1f} in [30,40], falling 2 days",
                    )
            else:
                if vix < SVIX_EXIT_VIX:
                    pending_trade = ("SELL_ALL", f"EXIT: VIX={vix:.1f}<{SVIX_EXIT_VIX}")
                elif svix_mode == "INIT" and vix >= SVIX_PANIC_VIX:
                    pending_trade = (
                        "BUY_PANIC",
                        f"SVIX_PANIC: VIX={vix:.1f}>={SVIX_PANIC_VIX} escalating to 30%",
                    )

        svix_value = svix_shares * svix_close if svix_avail else 0.0
        portfolio_value = cash + svix_value

        if in_svix and svix_mode == "PANIC":
            step4_state = "CASH_SVIX_PANIC"
        elif in_svix:
            step4_state = "CASH_SVIX_INIT"
        elif currently_cash:
            step4_state = "CASH_IDLE"
        else:
            step4_state = "INACTIVE"

        daily_log.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "step2_state": step2_state,
                "svix_available": "YES" if svix_avail else "NO",
                "step4_state": step4_state,
                "vix": round(vix, 2),
                "svix_close": round(svix_close, 4) if svix_avail else 0.0,
                "svix_shares": round(svix_shares, 4),
                "svix_value": round(svix_value, 2),
                "cash": round(cash, 2),
                "portfolio_value": round(portfolio_value, 2),
            }
        )

    metrics = calculate_step4_svix_metrics(daily_log, trades)
    return trades, daily_log, metrics


def calculate_step4_svix_metrics(daily_log: list[dict], trades: list[dict]) -> dict:
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

    return {
        "step": "Step 4 SVIX (2011-2025) — Safety Valve standalone",
        "approach": "Activates only during Step 2 CASH and only after SVIX launch (2022-03-30)",
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
        "total_commissions": round(sum(t["commission_dollars"] for t in trades), 2),
        "total_slippage": round(sum(t["slippage_dollars"] for t in trades), 2),
        "note_pre_svix_period": (
            "Strategy inactive before 2022-03-30 — SVIX did not exist (Option C)."
        ),
    }


def save_results(trades: list[dict], daily_log: list[dict], metrics: dict) -> None:
    pd.DataFrame(trades).to_csv(RESULTS_DIR / "step4_svix_2011_trade_log.csv", index=False)
    pd.DataFrame(daily_log).to_csv(
        RESULTS_DIR / "step4_svix_2011_portfolio_values.csv", index=False
    )
    with open(RESULTS_DIR / "step4_svix_2011_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"  saved step4_svix_2011_trade_log.csv ({len(trades)} trades)")
    print(f"  saved step4_svix_2011_portfolio_values.csv ({len(daily_log)} days)")
    print("  saved step4_svix_2011_metrics.json")


def print_summary(metrics: dict) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"{'=' * 60}")
    print(f"  Period:      {metrics['start_date']} -> {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value: ${metrics['final_value']:,.2f}")
    print(f"  CAGR:        {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:      {metrics['sharpe']:.3f}")
    print(f"  Max DD:      {metrics['max_drawdown_pct']:.2f}%")
    print(f"  Trades:      {metrics['total_trades']}")
    print(f"{'=' * 60}")


def main() -> dict:
    print("\n[Step 4 SVIX 2011] Loading merged + SVIX (DataBento, 2022+) + Step 2 results...")
    df, step2_daily = load_step4_svix_data()
    print(f"  Range: {df.index[0].date()} -> {df.index[-1].date()} ({len(df)} bars)")

    print("[Step 4 SVIX 2011] Running Safety Valve backtest...")
    trades, daily_log, metrics = run_step4_svix_backtest(df, step2_daily)

    print_summary(metrics)
    save_results(trades, daily_log, metrics)
    return metrics


if __name__ == "__main__":
    main()
