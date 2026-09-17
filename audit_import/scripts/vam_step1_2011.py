"""
VAM Step 1 (2011-2025 expansion) — UPRO-Only Backtest on Polygon+DataBento Merge

Identical strategy, parameters, and state machine to vam_step1_databento.py.
Difference: data loads from data/merged/*_daily_full.csv (Polygon 2011-2019 +
DataBento 2020-2025, already aligned to a single split-adjusted basis), so no
in-memory split adjustment is applied here.

Outputs land in results/2011_backtest/ with prefix step1_2011.
"""

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import numpy as np
import pandas as pd

ENGINE_ROOT = Path(__file__).resolve().parent.parent  # repo root (was parents[3] in original monorepo layout)
MERGED_DIR = ENGINE_ROOT / "data" / "merged"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results" / "2011_backtest"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# DATA LOADING — merged Polygon + DataBento (already split-adjusted)
# =============================================================================


def load_merged_csv(filepath: Path) -> pd.DataFrame:
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def load_step1_data() -> pd.DataFrame:
    """Load merged SPY, UPRO, VIX into one aligned DataFrame (no split work needed)."""
    spy = load_merged_csv(MERGED_DIR / "SPY_daily_full.csv")
    upro = load_merged_csv(MERGED_DIR / "UPRO_daily_full.csv")
    vix = load_merged_csv(MERGED_DIR / "VIX_daily_full.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["UPRO_Close"] = upro["close"].reindex(spy.index)
    df["UPRO_Open"] = upro["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df["data_source"] = spy["source"].reindex(spy.index)

    df = df.ffill().dropna()
    return df


# =============================================================================
# INDICATORS
# =============================================================================


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


def add_step1_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()
    df["SPY_RSI"] = calculate_rsi(df["SPY_Close"], RSI_PERIOD)
    df["SPY_below_50_streak"] = consecutive_streak(df["SPY_Close"] < df["SPY_SMA50"])
    df["SPY_above_50_streak"] = consecutive_streak(df["SPY_Close"] > df["SPY_SMA50"])
    return df


# =============================================================================
# STATE MACHINE (4-state + SMA_RECOVERY) — identical to vam_step1_databento.py
# =============================================================================


class State(Enum):
    BULL_100 = "BULL_100"
    BULL_TRIMMED = "BULL_TRIMMED"
    DEFENSIVE = "DEFENSIVE"
    CASH = "CASH"
    SMA_RECOVERY = "SMA_RECOVERY"


STATE_ALLOCATION = {
    State.BULL_100: 0.99,
    State.BULL_TRIMMED: 0.75,
    State.DEFENSIVE: 0.50,
    State.CASH: 0.00,
    State.SMA_RECOVERY: 0.75,
}

# === CONFIGURABLE PARAMETERS (copied verbatim from vam_step1_databento.py) ===
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
    below_50_streak: int,
    above_50_streak: int,
) -> tuple[State, str]:
    if current != State.CASH:
        if KILL_SWITCH_LOGIC == "OR":
            kill_triggered = vix > VIX_KILL or spy_close < spy_sma200
        else:
            kill_triggered = vix > VIX_KILL and spy_close < spy_sma200
        if kill_triggered:
            return (
                State.CASH,
                f"KILL: VIX={vix:.1f}, SPY={spy_close:.2f} vs 200SMA={spy_sma200:.2f}",
            )

    if current == State.BULL_100:
        if below_50_streak >= DEFENSIVE_CONFIRM_DAYS:
            return State.DEFENSIVE, f"DEFENSIVE: SPY below 50-SMA for {DEFENSIVE_CONFIRM_DAYS}d"
        if spy_rsi > RSI_SELL:
            return State.BULL_TRIMMED, f"RSI TRIM: RSI={spy_rsi:.1f}>{RSI_SELL}"
        return State.BULL_100, "HOLD"

    if current == State.BULL_TRIMMED:
        if below_50_streak >= DEFENSIVE_CONFIRM_DAYS:
            return State.DEFENSIVE, "DEFENSIVE from TRIMMED: SPY below 50-SMA"
        if spy_rsi < RSI_REBUY:
            return State.BULL_100, f"RSI RECOVERY: RSI={spy_rsi:.1f}<{RSI_REBUY}"
        return State.BULL_TRIMMED, "HOLD"

    if current == State.DEFENSIVE:
        if above_50_streak >= DEFENSIVE_CONFIRM_DAYS and vix < VIX_KILL:
            return State.BULL_100, f"DEF EXIT: SPY above 50-SMA for {DEFENSIVE_CONFIRM_DAYS}d"
        return State.DEFENSIVE, "HOLD"

    if current == State.CASH:
        if spy_close > spy_sma200 and vix < REENTRY_VIX:
            return (
                State.SMA_RECOVERY,
                f"SMA_RECOVERY: SPY>{spy_sma200:.0f}(200SMA), VIX={vix:.1f}<{REENTRY_VIX}",
            )
        return State.CASH, "HOLD"

    if current == State.SMA_RECOVERY:
        if REENTRY_REQUIRE_50SMA:
            reentry_ok = spy_close > spy_sma50 and vix < REENTRY_VIX
            reentry_reason = f"RE-ENTRY COMPLETE: SPY above 50-SMA, VIX<{REENTRY_VIX}"
        else:
            reentry_ok = vix < REENTRY_VIX
            reentry_reason = (
                f"RE-ENTRY COMPLETE: VIX={vix:.1f}<{REENTRY_VIX} (50-SMA check skipped)"
            )
        if reentry_ok:
            return State.BULL_100, reentry_reason
        return State.SMA_RECOVERY, "HOLD: awaiting re-entry conditions"

    return current, "NO_TRANSITION"


# =============================================================================
# BACKTEST ENGINE
# =============================================================================


@dataclass
class Trade:
    date: str
    action: str
    state_from: str
    state_to: str
    reason: str
    shares_before: float
    shares_after: float
    upro_price: float
    trade_value: float
    commission: float
    slippage: float
    portfolio_value: float


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def get_slippage_bps(vix: float, is_kill: bool) -> float:
    if is_kill or vix >= 25:
        return SLIPPAGE_BPS_STRESS
    return SLIPPAGE_BPS_NORMAL


def run_step1_backtest(df: pd.DataFrame) -> tuple[list[dict], list[dict], dict]:
    trading_df = df.dropna(subset=["SPY_SMA200", "SPY_RSI", "UPRO_Open"])
    if trading_df.empty:
        raise ValueError("No valid trading data after indicator warmup")

    cash = INITIAL_CAPITAL
    shares = 0.0
    state = State.CASH
    pending_trade: tuple[State, str] | None = None
    trades: list[dict] = []
    daily_log: list[dict] = []
    runtime_below_streak = 0

    for date, row in trading_df.iterrows():
        upro_price = row["UPRO_Close"]
        upro_open = row["UPRO_Open"]
        vix = row["VIX"]

        if pending_trade is not None:
            new_state, reason = pending_trade
            pending_trade = None

            old_state_val = state.value
            target_alloc = STATE_ALLOCATION[new_state]
            pv_before = cash + shares * upro_open
            target_shares = (pv_before * target_alloc) / upro_open if upro_open > 0 else 0

            shares_delta = target_shares - shares
            trade_value = abs(shares_delta * upro_open)
            comm = ibkr_commission(trade_value, upro_open)
            is_kill = "KILL" in reason
            slip_bps = get_slippage_bps(vix, is_kill)
            slip = trade_value * (slip_bps / 10000)

            old_shares = shares
            old_cash = cash
            shares = target_shares
            cash = pv_before - (target_shares * upro_open) - comm - slip
            state = new_state

            action = "BUY" if shares_delta > 0 else "SELL"
            pv_after_close = cash + shares * upro_price
            trades.append(
                {
                    "trade_number": len(trades) + 1,
                    "signal_date": (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d"),
                    "execution_date": date.strftime("%Y-%m-%d"),
                    "action": action,
                    "state_from": old_state_val,
                    "state_to": new_state.value,
                    "trigger_reason": reason,
                    "exec_price_upro_open": round(upro_open, 4),
                    "shares_before": round(old_shares, 4),
                    "shares_delta": round(shares_delta, 4),
                    "shares_after": round(target_shares, 4),
                    "trade_value_dollars": round(trade_value, 2),
                    "commission_dollars": round(comm, 2),
                    "slippage_dollars": round(slip, 2),
                    "portfolio_value_at_close": round(pv_after_close, 2),
                    "data_source": str(row.get("data_source", "")),
                }
            )

        old_state = state
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]
        spy_rsi = row["SPY_RSI"]
        above_streak = int(row["SPY_above_50_streak"])

        if state in (State.CASH, State.SMA_RECOVERY):
            runtime_below_streak = 0
        elif spy_close < spy_sma50:
            runtime_below_streak += 1
        else:
            runtime_below_streak = 0

        new_state, reason = next_state(
            current=old_state,
            spy_close=spy_close,
            spy_sma50=spy_sma50,
            spy_sma200=spy_sma200,
            spy_rsi=spy_rsi,
            vix=vix,
            below_50_streak=runtime_below_streak,
            above_50_streak=above_streak,
        )

        if new_state != old_state:
            pending_trade = (new_state, reason)

        portfolio_value = cash + shares * upro_price
        upro_alloc_pct = (shares * upro_price / portfolio_value * 100) if portfolio_value > 0 else 0

        daily_log.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "state": state.value,
                "spy_close": round(spy_close, 2),
                "upro_close": round(upro_price, 4),
                "vix": round(vix, 2),
                "shares_held": round(shares, 4),
                "cash_held": round(cash, 2),
                "portfolio_value": round(portfolio_value, 2),
                "upro_allocation_pct": round(upro_alloc_pct, 1),
                "data_source": str(row.get("data_source", "")),
            }
        )

    metrics = calculate_metrics(daily_log, trades)
    return trades, daily_log, metrics


# =============================================================================
# METRICS
# =============================================================================


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
    excess_returns = daily["daily_return"] - daily_rf
    sharpe = (
        (excess_returns.mean() / excess_returns.std()) * np.sqrt(252)
        if excess_returns.std() > 0
        else 0
    )
    downside = excess_returns[excess_returns < 0].std()
    sortino = (excess_returns.mean() / downside) * np.sqrt(252) if downside > 0 else 0

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
        "step": "Step 1 (2011-2025) — UPRO Only (Polygon+DataBento)",
        "approach": "Normal (4-state machine)",
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


# =============================================================================
# OUTPUT
# =============================================================================


def save_results(trades: list[dict], daily_log: list[dict], metrics: dict) -> None:
    pd.DataFrame(trades).to_csv(RESULTS_DIR / "step1_2011_trade_log.csv", index=False)
    pd.DataFrame(daily_log).to_csv(RESULTS_DIR / "step1_2011_portfolio_values.csv", index=False)
    with open(RESULTS_DIR / "step1_2011_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"  saved step1_2011_trade_log.csv ({len(trades)} trades)")
    print(f"  saved step1_2011_portfolio_values.csv ({len(daily_log)} days)")
    print("  saved step1_2011_metrics.json")


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
    print(f"  Alpha vs SPY: {metrics['alpha_vs_spy_pct']:+.2f}%")
    print(f"{'=' * 60}")


def main() -> dict:
    print("\n[Step 1 2011] Loading merged Polygon+DataBento data...")
    df = load_step1_data()
    print(f"  Range: {df.index[0].date()} -> {df.index[-1].date()} ({len(df)} bars)")

    print("[Step 1 2011] Calculating indicators...")
    df = add_step1_indicators(df)

    print("[Step 1 2011] Running 4-state backtest...")
    trades, daily_log, metrics = run_step1_backtest(df)

    print_summary(metrics)
    save_results(trades, daily_log, metrics)
    return metrics


if __name__ == "__main__":
    main()
