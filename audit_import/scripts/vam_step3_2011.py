"""
VAM Step 3 (2011-2025 expansion) — Predatory Short (SPXU) on merged Polygon+DataBento.

Identical entry/exit logic to vam_step3_databento.py. Reads SPXU from
data/merged/SPXU_daily_full.csv (already split-adjusted on a single basis), and
reads Step 2 CASH periods from results/2011_backtest/step2_2011_portfolio_values.csv.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

ENGINE_ROOT = Path(__file__).resolve().parent.parent  # repo root (was parents[3] in original monorepo layout)
MERGED_DIR = ENGINE_ROOT / "data" / "merged"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "2011_backtest"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# === CONFIGURABLE PARAMETERS (copied verbatim from vam_step3_databento.py) ===
SPXU_EXIT_VIX = 30
SPXU_EXIT_SPY_50SMA = True
USE_SGOV = True
COMMISSION_MODEL = "IBKR"

SLIPPAGE_BPS_SPXU = 20.0
INITIAL_CAPITAL = 100_000.0
RISK_FREE_RATE = 0.04
VIX_ENTRY = 30.0
SPXU_ALLOCATION = 0.50
SPXU_ALLOC_CAP = 0.99


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def load_merged_csv(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def load_step3_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    spy = load_merged_csv(MERGED_DIR / "SPY_daily_full.csv")
    spxu = load_merged_csv(MERGED_DIR / "SPXU_daily_full.csv")
    vix = load_merged_csv(MERGED_DIR / "VIX_daily_full.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["SPXU_Close"] = spxu["close"].reindex(spy.index)
    df["SPXU_Open"] = spxu["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df["data_source"] = spy["source"].reindex(spy.index)

    df = df.ffill().dropna(subset=["SPY_Close", "SPXU_Close", "VIX"])

    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()

    step2_daily = pd.read_csv(
        RESULTS_DIR / "step2_2011_portfolio_values.csv",
        parse_dates=["date"],
        index_col="date",
    )
    return df, step2_daily


def get_step2_state(date: pd.Timestamp, step2_daily: pd.DataFrame) -> str:
    key = date.normalize()
    if key in step2_daily.index:
        return str(step2_daily.loc[key, "state"])
    return "UNKNOWN"


def run_step3_backtest(
    df: pd.DataFrame, step2_daily: pd.DataFrame
) -> tuple[list[dict], list[dict], dict]:
    trading_df = df.dropna(subset=["SPY_SMA50", "SPY_SMA200"])
    if trading_df.empty:
        raise ValueError("No valid data after SMA warmup period")

    cash = INITIAL_CAPITAL
    spxu_shares = 0.0
    in_spxu = False
    pending_trade: tuple[str, str] | None = None
    trades: list[dict] = []
    daily_log: list[dict] = []

    for date, row in trading_df.iterrows():
        spxu_price = row["SPXU_Close"]
        spxu_exec = row["SPXU_Open"] if row["SPXU_Open"] > 0 else spxu_price
        vix = row["VIX"]
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]

        step2_state = get_step2_state(date, step2_daily)
        currently_cash = step2_state == "CASH"

        if not currently_cash and in_spxu and pending_trade is None:
            pending_trade = ("SELL", "FORCED_EXIT: Step 2 exited CASH state")

        if pending_trade is not None:
            action, reason = pending_trade
            pending_trade = None
            pv_before = cash + spxu_shares * spxu_exec

            if action == "BUY" and spxu_exec > 0:
                invest = min(pv_before * SPXU_ALLOCATION, pv_before * SPXU_ALLOC_CAP)
                slip = invest * (SLIPPAGE_BPS_SPXU / 10_000)
                comm = ibkr_commission(invest, spxu_exec)
                old_shares = spxu_shares
                old_cash = cash
                new_shares = invest / spxu_exec
                spxu_shares = new_shares
                cash = pv_before - invest - slip - comm
                in_spxu = True
                trades.append(
                    {
                        "trade_number": len(trades) + 1,
                        "execution_date": date.strftime("%Y-%m-%d"),
                        "action": "BUY",
                        "instrument": "SPXU",
                        "trigger_reason": reason,
                        "exec_price": round(spxu_exec, 4),
                        "shares_before": round(old_shares, 4),
                        "shares_after": round(spxu_shares, 4),
                        "trade_value_dollars": round(invest, 2),
                        "commission_dollars": round(comm, 2),
                        "slippage_dollars": round(slip, 2),
                        "cash_after": round(cash, 2),
                        "portfolio_value_at_close": round(cash + spxu_shares * spxu_price, 2),
                    }
                )
            elif action == "SELL" and spxu_shares > 0:
                sell_val = spxu_shares * spxu_exec
                slip = sell_val * (SLIPPAGE_BPS_SPXU / 10_000)
                comm = ibkr_commission(sell_val, spxu_exec)
                old_shares = spxu_shares
                cash = cash + sell_val - slip - comm
                spxu_shares = 0.0
                in_spxu = False
                trades.append(
                    {
                        "trade_number": len(trades) + 1,
                        "execution_date": date.strftime("%Y-%m-%d"),
                        "action": "SELL",
                        "instrument": "SPXU",
                        "trigger_reason": reason,
                        "exec_price": round(spxu_exec, 4),
                        "shares_before": round(old_shares, 4),
                        "shares_after": 0.0,
                        "trade_value_dollars": round(sell_val, 2),
                        "commission_dollars": round(comm, 2),
                        "slippage_dollars": round(slip, 2),
                        "cash_after": round(cash, 2),
                        "portfolio_value_at_close": round(cash, 2),
                    }
                )

        if currently_cash:
            spy_below_200 = spy_close < spy_sma200
            spy_above_50 = spy_close > spy_sma50
            vix_above_entry = vix > VIX_ENTRY
            vix_below_exit = vix < SPXU_EXIT_VIX

            if not in_spxu:
                if spy_below_200 and vix_above_entry:
                    pending_trade = (
                        "BUY",
                        f"PREDATORY_SHORT: SPY<200SMA({spy_sma200:.0f})+VIX={vix:.1f}>{VIX_ENTRY}",
                    )
            else:
                exit_parts = []
                if vix_below_exit:
                    exit_parts.append(f"VIX={vix:.1f}<{SPXU_EXIT_VIX}")
                if SPXU_EXIT_SPY_50SMA and spy_above_50:
                    exit_parts.append(f"SPY>{spy_sma50:.0f}(50SMA)")
                if exit_parts:
                    pending_trade = ("SELL", "EXIT: " + " | ".join(exit_parts))

        portfolio_value = cash + spxu_shares * spxu_price
        spxu_value = spxu_shares * spxu_price

        if in_spxu:
            step3_state = "CASH_SPXU"
        elif currently_cash:
            step3_state = "CASH_IDLE"
        else:
            step3_state = "INACTIVE"

        daily_log.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "step2_state": step2_state,
                "step3_state": step3_state,
                "spy_close": round(spy_close, 2),
                "vix": round(vix, 2),
                "spxu_close": round(spxu_price, 4),
                "spxu_shares": round(spxu_shares, 4),
                "spxu_value": round(spxu_value, 2),
                "cash": round(cash, 2),
                "portfolio_value": round(portfolio_value, 2),
                "data_source": str(row.get("data_source", "")),
            }
        )

    metrics = calculate_metrics(daily_log, trades)
    return trades, daily_log, metrics


def calculate_metrics(daily_log: list[dict], trades: list[dict]) -> dict:
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

    daily_rf = RISK_FREE_RATE / 252
    excess = daily["daily_return"] - daily_rf
    sharpe = (excess.mean() / excess.std()) * np.sqrt(252) if excess.std() > 0 else 0
    downside = excess[excess < 0].std()
    sortino = (excess.mean() / downside) * np.sqrt(252) if downside > 0 else 0

    cummax = daily["portfolio_value"].cummax()
    drawdown = (daily["portfolio_value"] - cummax) / cummax
    max_dd = drawdown.min()
    max_dd_date = drawdown.idxmin().strftime("%Y-%m-%d")
    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    buy_trades = [t for t in trades if t["action"] == "BUY"]
    sell_trades = [t for t in trades if t["action"] == "SELL"]

    return {
        "step": "Step 3 (2011-2025) — Predatory Short / SPXU (Polygon+DataBento)",
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
        "total_commissions": round(sum(t["commission_dollars"] for t in trades), 2),
        "total_slippage": round(sum(t["slippage_dollars"] for t in trades), 2),
    }


def save_results(trades: list[dict], daily_log: list[dict], metrics: dict) -> None:
    pd.DataFrame(trades).to_csv(RESULTS_DIR / "step3_2011_trade_log.csv", index=False)
    pd.DataFrame(daily_log).to_csv(RESULTS_DIR / "step3_2011_portfolio_values.csv", index=False)
    with open(RESULTS_DIR / "step3_2011_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"  saved step3_2011_trade_log.csv ({len(trades)} trades)")
    print(f"  saved step3_2011_portfolio_values.csv ({len(daily_log)} days)")
    print("  saved step3_2011_metrics.json")


def print_summary(metrics: dict) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"{'=' * 60}")
    print(f"  Period:      {metrics['start_date']} -> {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value: ${metrics['final_value']:,.2f}")
    print(f"  Total Return:{metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:        {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:      {metrics['sharpe']:.3f}")
    print(f"  Sortino:     {metrics['sortino']:.3f}")
    print(f"  Max Drawdown:{metrics['max_drawdown_pct']:.2f}%")
    print(f"  Trades:      {metrics['total_trades']}")
    print(f"{'=' * 60}")


def main() -> dict:
    print("\n[Step 3 2011] Loading merged data + Step 2 results...")
    df, step2_daily = load_step3_data()
    print(f"  Range: {df.index[0].date()} -> {df.index[-1].date()} ({len(df)} bars)")

    print("[Step 3 2011] Running Predatory Short backtest...")
    trades, daily_log, metrics = run_step3_backtest(df, step2_daily)

    print_summary(metrics)
    save_results(trades, daily_log, metrics)
    return metrics


if __name__ == "__main__":
    main()
