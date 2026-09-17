"""
VAM Step 2 (2011-2025 expansion) — UPRO + TQQQ Backtest on merged Polygon+DataBento.

Identical 7-state machine, parameters, and execution logic to vam_step2_databento.py.
Loads from data/merged/*_daily_full.csv (already split-adjusted), so no in-memory
split work is performed here. Outputs land in results/2011_backtest/.
"""

import json
from enum import Enum
from pathlib import Path

import numpy as np
import pandas as pd

ENGINE_ROOT = Path(__file__).resolve().parent.parent  # repo root (was parents[3] in original monorepo layout)
MERGED_DIR = ENGINE_ROOT / "data" / "merged"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "2011_backtest"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def load_merged_csv(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def load_step2_data() -> pd.DataFrame:
    spy = load_merged_csv(MERGED_DIR / "SPY_daily_full.csv")
    qqq = load_merged_csv(MERGED_DIR / "QQQ_daily_full.csv")
    upro = load_merged_csv(MERGED_DIR / "UPRO_daily_full.csv")
    tqqq = load_merged_csv(MERGED_DIR / "TQQQ_daily_full.csv")
    vix = load_merged_csv(MERGED_DIR / "VIX_daily_full.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["QQQ_Close"] = qqq["close"].reindex(spy.index)
    df["UPRO_Close"] = upro["close"].reindex(spy.index)
    df["UPRO_Open"] = upro["open"].reindex(spy.index)
    df["TQQQ_Close"] = tqqq["close"].reindex(spy.index)
    df["TQQQ_Open"] = tqqq["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df["data_source"] = spy["source"].reindex(spy.index)

    df = df.ffill().dropna(subset=["SPY_Close", "QQQ_Close", "UPRO_Close", "TQQQ_Close", "VIX"])
    return df


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def consecutive_streak(condition: pd.Series) -> pd.Series:
    streak = condition.astype(int).copy()
    for i in range(1, len(streak)):
        if streak.iloc[i] == 1:
            streak.iloc[i] = streak.iloc[i - 1] + 1
        else:
            streak.iloc[i] = 0
    return streak


def add_step2_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()
    df["SPY_RSI"] = calculate_rsi(df["SPY_Close"], RSI_PERIOD)
    df["SPY_below_50_streak"] = consecutive_streak(df["SPY_Close"] < df["SPY_SMA50"])
    df["SPY_above_50_streak"] = consecutive_streak(df["SPY_Close"] > df["SPY_SMA50"])
    df["QQQ_SMA50"] = df["QQQ_Close"].rolling(50).mean()
    df["QQQ_below_50_streak"] = consecutive_streak(df["QQQ_Close"] < df["QQQ_SMA50"])
    df["QQQ_above_50_streak"] = consecutive_streak(df["QQQ_Close"] > df["QQQ_SMA50"])
    return df


# === STATE MACHINE (7-state, copied verbatim from vam_step2_databento.py) ===


class State(Enum):
    BULL_100 = "BULL_100"
    BULL_TRIMMED = "BULL_TRIMMED"
    DEFENSIVE_SPY = "DEFENSIVE_SPY"
    DEFENSIVE_QQQ = "DEFENSIVE_QQQ"
    DEFENSIVE_BOTH = "DEFENSIVE_BOTH"
    CASH = "CASH"
    SMA_RECOVERY = "SMA_RECOVERY"


STATE_ALLOCATION: dict[State, tuple[float, float]] = {
    State.BULL_100: (0.7425, 0.2475),
    State.BULL_TRIMMED: (0.5625, 0.1875),
    State.DEFENSIVE_SPY: (0.375, 0.25),
    State.DEFENSIVE_QQQ: (0.75, 0.125),
    State.DEFENSIVE_BOTH: (0.375, 0.125),
    State.CASH: (0.0, 0.0),
    State.SMA_RECOVERY: (0.5625, 0.1875),
}

# === CONFIGURABLE PARAMETERS (copied verbatim) ===
RSI_PERIOD = 14
RSI_REBUY = 60
DEFENSIVE_CONFIRM_DAYS = 2
KILL_SWITCH_LOGIC = "OR"
REENTRY_VIX = 30
REENTRY_REQUIRE_50SMA = False
USE_SGOV = True
COMMISSION_MODEL = "IBKR"

VIX_KILL = 30.0
RSI_SELL = 75.0
SLIPPAGE_BPS_NORMAL = 5.0
SLIPPAGE_BPS_STRESS = 20.0
INITIAL_CAPITAL = 100_000.0
RISK_FREE_RATE = 0.04


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
    if current not in (State.CASH,):
        if KILL_SWITCH_LOGIC == "OR":
            kill_triggered = vix > VIX_KILL or spy_close < spy_sma200
        else:
            kill_triggered = vix > VIX_KILL and spy_close < spy_sma200
        if kill_triggered:
            return State.CASH, f"KILL: VIX={vix:.1f}, SPY vs 200SMA"

    spy_below = spy_below_streak >= DEFENSIVE_CONFIRM_DAYS
    qqq_below = qqq_below_streak >= DEFENSIVE_CONFIRM_DAYS
    spy_above = spy_above_streak >= DEFENSIVE_CONFIRM_DAYS
    qqq_above = qqq_above_streak >= DEFENSIVE_CONFIRM_DAYS

    if current in (State.BULL_100, State.BULL_TRIMMED):
        if spy_below and qqq_below:
            return State.DEFENSIVE_BOTH, "DEFENSIVE BOTH: SPY & QQQ below 50-SMA"
        if spy_below:
            return State.DEFENSIVE_SPY, "DEFENSIVE SPY: SPY below 50-SMA"
        if qqq_below:
            return State.DEFENSIVE_QQQ, "DEFENSIVE QQQ: QQQ below 50-SMA"
        if current == State.BULL_100 and spy_rsi > RSI_SELL:
            return State.BULL_TRIMMED, f"RSI TRIM: RSI={spy_rsi:.1f}"
        if current == State.BULL_TRIMMED and spy_rsi < RSI_REBUY:
            return State.BULL_100, f"RSI RECOVERY: RSI={spy_rsi:.1f}"
        return current, "HOLD"

    if current == State.DEFENSIVE_SPY:
        if qqq_below:
            return State.DEFENSIVE_BOTH, "WORSENING: QQQ also below 50-SMA"
        if spy_above and vix < VIX_KILL:
            return State.BULL_100, "RECOVERY: SPY reclaimed 50-SMA"
        return State.DEFENSIVE_SPY, "HOLD"

    if current == State.DEFENSIVE_QQQ:
        if spy_below:
            return State.DEFENSIVE_BOTH, "WORSENING: SPY also below 50-SMA"
        if qqq_above and vix < VIX_KILL:
            return State.BULL_100, "RECOVERY: QQQ reclaimed 50-SMA"
        return State.DEFENSIVE_QQQ, "HOLD"

    if current == State.DEFENSIVE_BOTH:
        if spy_above and qqq_above and vix < VIX_KILL:
            return State.BULL_100, "FULL RECOVERY: both above 50-SMA"
        if spy_above and not qqq_above:
            return State.DEFENSIVE_QQQ, "PARTIAL: SPY recovered, QQQ still below"
        if qqq_above and not spy_above:
            return State.DEFENSIVE_SPY, "PARTIAL: QQQ recovered, SPY still below"
        return State.DEFENSIVE_BOTH, "HOLD"

    if current == State.CASH:
        if spy_close > spy_sma200 and vix < REENTRY_VIX:
            return (
                State.SMA_RECOVERY,
                f"SMA_RECOVERY: SPY>{spy_sma200:.0f}(200SMA), VIX={vix:.1f}<{REENTRY_VIX}",
            )
        return State.CASH, "HOLD"

    if current == State.SMA_RECOVERY:
        if REENTRY_REQUIRE_50SMA:
            reentry_ok = spy_close > spy_sma50 and qqq_close > qqq_sma50 and vix < REENTRY_VIX
            reentry_reason = f"RE-ENTRY COMPLETE: SPY & QQQ above 50-SMA, VIX<{REENTRY_VIX}"
        else:
            reentry_ok = vix < REENTRY_VIX
            reentry_reason = (
                f"RE-ENTRY COMPLETE: VIX={vix:.1f}<{REENTRY_VIX} (50-SMA check skipped)"
            )
        if reentry_ok:
            return State.BULL_100, reentry_reason
        return State.SMA_RECOVERY, "HOLD: awaiting re-entry conditions"

    return current, "NO_TRANSITION"


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def get_slippage_bps(vix: float, is_kill: bool) -> float:
    if is_kill or vix >= 25:
        return SLIPPAGE_BPS_STRESS
    return SLIPPAGE_BPS_NORMAL


def run_step2_backtest(df: pd.DataFrame) -> tuple[list[dict], list[dict], dict]:
    trading_df = df.dropna(subset=["SPY_SMA200", "SPY_RSI", "QQQ_SMA50", "UPRO_Open", "TQQQ_Open"])
    if trading_df.empty:
        raise ValueError("No valid data after warmup")

    cash = INITIAL_CAPITAL
    upro_shares = 0.0
    tqqq_shares = 0.0
    state = State.CASH
    pending_trade: tuple[State, str] | None = None
    trades: list[dict] = []
    daily_log: list[dict] = []
    runtime_spy_below_streak = 0
    runtime_qqq_below_streak = 0

    for date, row in trading_df.iterrows():
        upro_price = row["UPRO_Close"]
        tqqq_price = row["TQQQ_Close"]
        upro_exec = row["UPRO_Open"]
        tqqq_exec = row["TQQQ_Open"]
        vix = row["VIX"]

        if pending_trade is not None:
            new_state, reason = pending_trade
            pending_trade = None

            pv_before = cash + upro_shares * upro_exec + tqqq_shares * tqqq_exec
            upro_alloc, tqqq_alloc = STATE_ALLOCATION[new_state]
            is_kill = "KILL" in reason
            slip_bps = get_slippage_bps(vix, is_kill)

            target_upro = (pv_before * upro_alloc) / upro_exec if upro_exec > 0 else 0
            target_tqqq = (pv_before * tqqq_alloc) / tqqq_exec if tqqq_exec > 0 else 0

            upro_delta = target_upro - upro_shares
            upro_trade_val = abs(upro_delta * upro_exec)
            upro_comm = ibkr_commission(upro_trade_val, upro_exec)
            upro_slip_dollars = upro_trade_val * (slip_bps / 10000)

            tqqq_delta = target_tqqq - tqqq_shares
            tqqq_trade_val = abs(tqqq_delta * tqqq_exec)
            tqqq_comm = ibkr_commission(tqqq_trade_val, tqqq_exec)
            tqqq_slip_dollars = tqqq_trade_val * (slip_bps / 10000)

            total_cost = upro_comm + upro_slip_dollars + tqqq_comm + tqqq_slip_dollars

            old_state_val = state.value
            upro_shares = target_upro
            tqqq_shares = target_tqqq
            cash = pv_before - (target_upro * upro_exec) - (target_tqqq * tqqq_exec) - total_cost
            state = new_state

            pv_at_close = cash + upro_shares * upro_price + tqqq_shares * tqqq_price
            exec_date_str = date.strftime("%Y-%m-%d")
            signal_date_str = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

            if upro_trade_val > 0:
                trades.append(
                    {
                        "trade_number": len(trades) + 1,
                        "signal_date": signal_date_str,
                        "execution_date": exec_date_str,
                        "action": "BUY" if upro_delta > 0 else "SELL",
                        "instrument": "UPRO",
                        "state_from": old_state_val,
                        "state_to": new_state.value,
                        "trigger_reason": reason,
                        "exec_price": round(upro_exec, 4),
                        "shares_delta": round(upro_delta, 4),
                        "trade_value_dollars": round(upro_trade_val, 2),
                        "commission_dollars": round(upro_comm, 2),
                        "slippage_dollars": round(upro_slip_dollars, 2),
                        "portfolio_value_at_close": round(pv_at_close, 2),
                    }
                )

            if tqqq_trade_val > 0:
                trades.append(
                    {
                        "trade_number": len(trades) + 1,
                        "signal_date": signal_date_str,
                        "execution_date": exec_date_str,
                        "action": "BUY" if tqqq_delta > 0 else "SELL",
                        "instrument": "TQQQ",
                        "state_from": old_state_val,
                        "state_to": new_state.value,
                        "trigger_reason": reason,
                        "exec_price": round(tqqq_exec, 4),
                        "shares_delta": round(tqqq_delta, 4),
                        "trade_value_dollars": round(tqqq_trade_val, 2),
                        "commission_dollars": round(tqqq_comm, 2),
                        "slippage_dollars": round(tqqq_slip_dollars, 2),
                        "portfolio_value_at_close": round(pv_at_close, 2),
                    }
                )

        old_state = state
        spy_close = row["SPY_Close"]
        qqq_close = row["QQQ_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]
        spy_rsi = row["SPY_RSI"]
        qqq_sma50 = row["QQQ_SMA50"]
        spy_above_streak = int(row["SPY_above_50_streak"])
        qqq_above_streak = int(row["QQQ_above_50_streak"])

        if state in (State.CASH, State.SMA_RECOVERY):
            runtime_spy_below_streak = 0
            runtime_qqq_below_streak = 0
        else:
            runtime_spy_below_streak = runtime_spy_below_streak + 1 if spy_close < spy_sma50 else 0
            runtime_qqq_below_streak = runtime_qqq_below_streak + 1 if qqq_close < qqq_sma50 else 0

        new_state, reason = next_state(
            current=old_state,
            spy_close=spy_close,
            spy_sma50=spy_sma50,
            spy_sma200=spy_sma200,
            spy_rsi=spy_rsi,
            vix=vix,
            spy_below_streak=runtime_spy_below_streak,
            spy_above_streak=spy_above_streak,
            qqq_close=qqq_close,
            qqq_sma50=qqq_sma50,
            qqq_below_streak=runtime_qqq_below_streak,
            qqq_above_streak=qqq_above_streak,
        )

        if new_state != old_state:
            pending_trade = (new_state, reason)

        portfolio_value = cash + upro_shares * upro_price + tqqq_shares * tqqq_price
        upro_value = upro_shares * upro_price
        tqqq_value = tqqq_shares * tqqq_price

        daily_log.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "state": state.value,
                "spy_close": round(spy_close, 2),
                "qqq_close": round(qqq_close, 2),
                "vix": round(vix, 2),
                "upro_shares": round(upro_shares, 4),
                "tqqq_shares": round(tqqq_shares, 4),
                "cash": round(cash, 2),
                "upro_value": round(upro_value, 2),
                "tqqq_value": round(tqqq_value, 2),
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

    return {
        "step": "Step 2 (2011-2025) — UPRO + TQQQ (Polygon+DataBento)",
        "approach": "Normal (7-state machine)",
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
        "state_distribution_pct": state_pct,
        "annual_returns": {str(y): round(r * 100, 2) for y, r in annual["return"].items()},
        "benchmark_spy_cagr_pct": round(spy_cagr * 100, 2),
        "benchmark_spy_total_return_pct": round(spy_total * 100, 2),
        "alpha_vs_spy_pct": round((cagr - spy_cagr) * 100, 2),
    }


def save_results(trades: list[dict], daily_log: list[dict], metrics: dict) -> None:
    pd.DataFrame(trades).to_csv(RESULTS_DIR / "step2_2011_trade_log.csv", index=False)
    pd.DataFrame(daily_log).to_csv(RESULTS_DIR / "step2_2011_portfolio_values.csv", index=False)
    with open(RESULTS_DIR / "step2_2011_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"  saved step2_2011_trade_log.csv ({len(trades)} trades)")
    print(f"  saved step2_2011_portfolio_values.csv ({len(daily_log)} days)")
    print("  saved step2_2011_metrics.json")


def print_summary(metrics: dict) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"{'=' * 60}")
    print(f"  Period:       {metrics['start_date']} -> {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value:  ${metrics['final_value']:,.2f}")
    print(f"  Total Return: {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:         {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:       {metrics['sharpe']:.3f}")
    print(f"  Sortino:      {metrics['sortino']:.3f}")
    print(f"  Calmar:       {metrics['calmar']:.3f}")
    print(f"  Max Drawdown: {metrics['max_drawdown_pct']:.2f}% ({metrics['max_drawdown_date']})")
    print(f"  Trades:       {metrics['total_trades']}")
    print(f"  SPY B&H CAGR: {metrics['benchmark_spy_cagr_pct']:+.2f}%")
    print(f"{'=' * 60}")


def main() -> dict:
    print("\n[Step 2 2011] Loading merged data...")
    df = load_step2_data()
    print(f"  Range: {df.index[0].date()} -> {df.index[-1].date()} ({len(df)} bars)")

    print("[Step 2 2011] Calculating indicators...")
    df = add_step2_indicators(df)

    print("[Step 2 2011] Running 7-state backtest...")
    trades, daily_log, metrics = run_step2_backtest(df)

    print_summary(metrics)
    save_results(trades, daily_log, metrics)
    return metrics


if __name__ == "__main__":
    main()
