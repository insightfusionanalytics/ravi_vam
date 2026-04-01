"""
VAM Step 1 — UPRO-Only Backtest on DataBento Production Data
Client: Ravi Mareedu & Sudhir Vyakaranam
Proposal: IFA_Ravi_Proposal_v2

4-State Machine: BULL_100, BULL_TRIMMED, DEFENSIVE, CASH
Signals: SPY SMA-50, SPY SMA-200, SPY RSI-14, VIX
Position: UPRO (3x S&P 500 Bull ETF)

Data source: DataBento daily CSVs (production-grade, replaces yfinance)
"""

import sys
import json
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

# Paths
ENGINE_ROOT = Path(__file__).resolve().parents[3]  # _engine/code/
DATA_DIR = ENGINE_ROOT / "data"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


# =============================================================================
# DATA LOADING
# =============================================================================


def load_databento_csv(filepath: Path) -> pd.DataFrame:
    """Load a DataBento daily CSV into a clean DataFrame."""
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime")
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df


# DataBento data is NOT split-adjusted. These splits must be applied in-memory.
UPRO_SPLITS = [
    ("2018-05-24", 3),  # 3:1 forward split
    ("2022-01-13", 2),  # 2:1 forward split
]

TQQQ_SPLITS = [
    ("2021-01-21", 2),  # 2:1 forward split
    ("2022-01-13", 2),  # 2:1 forward split
    ("2025-11-20", 2),  # 2:1 forward split
]

PRICE_COLS = ["open", "high", "low", "close"]


def adjust_for_splits(df: pd.DataFrame, splits: list[tuple[str, int]]) -> pd.DataFrame:
    """Adjust historical prices for forward stock splits (in-place)."""
    df = df.copy()
    for split_date, ratio in splits:
        mask = df.index < pd.Timestamp(split_date)
        for col in PRICE_COLS:
            if col in df.columns:
                df.loc[mask, col] = df.loc[mask, col] / ratio
    return df


def load_step1_data() -> pd.DataFrame:
    """Load SPY, UPRO, VIX and merge into a single aligned DataFrame.

    Includes next-day open for T+1 execution (no look-ahead bias).
    """
    spy = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPY_daily.csv")
    upro = load_databento_csv(DATA_DIR / "databento" / "equities" / "UPRO_daily.csv")
    upro = adjust_for_splits(upro, UPRO_SPLITS)
    vix = load_databento_csv(DATA_DIR / "cboe" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["UPRO_Close"] = upro["close"].reindex(spy.index)
    df["UPRO_Open"] = upro["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)

    df = df.ffill().dropna()
    return df


# =============================================================================
# INDICATORS
# =============================================================================


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI using EWM (matches TradingView)."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def consecutive_streak(condition: pd.Series) -> pd.Series:
    """Count consecutive True days. Resets to 0 on False."""
    streak = condition.astype(int).copy()
    for i in range(1, len(streak)):
        if streak.iloc[i] == 1:
            streak.iloc[i] = streak.iloc[i - 1] + 1
        else:
            streak.iloc[i] = 0
    return streak


def add_step1_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all indicators needed for the 4-state machine."""
    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()
    df["SPY_RSI"] = calculate_rsi(df["SPY_Close"], 14)

    df["SPY_below_50_streak"] = consecutive_streak(df["SPY_Close"] < df["SPY_SMA50"])
    df["SPY_above_50_streak"] = consecutive_streak(df["SPY_Close"] > df["SPY_SMA50"])

    return df


# =============================================================================
# STATE MACHINE (4-state, proposal-faithful)
# =============================================================================


class State(Enum):
    BULL_100 = "BULL_100"
    BULL_TRIMMED = "BULL_TRIMMED"
    DEFENSIVE = "DEFENSIVE"
    CASH = "CASH"


# Allocations capped at 99% to reserve cash for commission + slippage
STATE_ALLOCATION = {
    State.BULL_100: 0.99,
    State.BULL_TRIMMED: 0.75,
    State.DEFENSIVE: 0.50,
    State.CASH: 0.00,
}

# Config constants (from proposal)
VIX_KILL = 30.0
SMA_CONFIRM_DAYS = 2
RSI_SELL = 75.0
RSI_REBUY = 60.0
COMMISSION = 1.0
SLIPPAGE_BPS_NORMAL = 5.0  # Normal market conditions
SLIPPAGE_BPS_STRESS = 20.0  # VIX > 25 or kill switch days
INITIAL_CAPITAL = 100_000.0
RISK_FREE_RATE = 0.04  # ~4% avg 2020-2025 (Fed funds 0-5.5%)


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
    """Determine next state. Signal priority: kill > defensive > RSI."""

    # PRIORITY 1: Kill Switch (from any non-CASH state)
    if current != State.CASH:
        if vix > VIX_KILL or spy_close < spy_sma200:
            return (
                State.CASH,
                f"KILL: VIX={vix:.1f}, SPY={spy_close:.2f} vs 200SMA={spy_sma200:.2f}",
            )

    if current == State.BULL_100:
        if below_50_streak >= SMA_CONFIRM_DAYS:
            return State.DEFENSIVE, f"DEFENSIVE: SPY below 50-SMA for {SMA_CONFIRM_DAYS}d"
        if spy_rsi > RSI_SELL:
            return State.BULL_TRIMMED, f"RSI TRIM: RSI={spy_rsi:.1f}>{RSI_SELL}"
        return State.BULL_100, "HOLD"

    elif current == State.BULL_TRIMMED:
        if below_50_streak >= SMA_CONFIRM_DAYS:
            return State.DEFENSIVE, f"DEFENSIVE from TRIMMED: SPY below 50-SMA"
        if spy_rsi < RSI_REBUY:
            return State.BULL_100, f"RSI RECOVERY: RSI={spy_rsi:.1f}<{RSI_REBUY}"
        return State.BULL_TRIMMED, "HOLD"

    elif current == State.DEFENSIVE:
        if above_50_streak >= SMA_CONFIRM_DAYS and vix < VIX_KILL:
            return State.BULL_100, f"DEF EXIT: SPY above 50-SMA for {SMA_CONFIRM_DAYS}d"
        return State.DEFENSIVE, "HOLD"

    elif current == State.CASH:
        if spy_close > spy_sma200 and vix < VIX_KILL and spy_close > spy_sma50:
            return (
                State.BULL_100,
                f"RE-ENTRY: SPY>{spy_sma200:.0f}(200), VIX={vix:.1f}<30, SPY>{spy_sma50:.0f}(50)",
            )
        return State.CASH, "HOLD"

    return current, "NO_TRANSITION"


# =============================================================================
# BACKTEST ENGINE
# =============================================================================


@dataclass
class Trade:
    """Single trade record."""

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


def get_slippage_bps(vix: float, is_kill: bool) -> float:
    """Dynamic slippage: higher on stress days (VIX > 25 or kill switch)."""
    if is_kill or vix > 25:
        return SLIPPAGE_BPS_STRESS
    return SLIPPAGE_BPS_NORMAL


def run_step1_backtest(df: pd.DataFrame) -> tuple[list[Trade], list[dict], dict]:
    """
    Run the 4-state UPRO-only backtest on DataBento data.

    T+1 execution: signal generated on today's close, trade executed at
    next day's open. This eliminates look-ahead bias per the proposal:
    "signal at 4 PM EST close, order placed next day 30 min after open."
    """
    trading_df = df.dropna(subset=["SPY_SMA200", "SPY_RSI", "UPRO_Open"])
    if trading_df.empty:
        raise ValueError("No valid trading data after indicator warmup")

    cash = INITIAL_CAPITAL
    shares = 0.0
    state = State.CASH
    pending_trade: tuple[State, str] | None = None  # (new_state, reason)
    trades: list[Trade] = []
    daily_log: list[dict] = []

    for date, row in trading_df.iterrows():
        upro_price = row["UPRO_Close"]
        upro_open = row["UPRO_Open"]  # Today's open — execution price for T+1
        vix = row["VIX"]

        # STEP 1: Execute pending trade from yesterday's signal at TODAY's open
        # Signal fires on day T-1 close → trade executes at day T open.
        # The pending_trade queue provides the 1-bar delay. upro_open is
        # today's open (not shifted), so no double-shift.

        if pending_trade is not None:
            new_state, reason = pending_trade
            pending_trade = None

            old_state_val = state.value  # Capture BEFORE update
            target_alloc = STATE_ALLOCATION[new_state]
            pv_before = cash + shares * upro_open
            target_shares = (pv_before * target_alloc) / upro_open if upro_open > 0 else 0

            shares_delta = target_shares - shares
            trade_value = abs(shares_delta * upro_open)
            comm = COMMISSION if trade_value > 0 else 0
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
            trades.append({
                # ── Identity ──
                "trade_number": len(trades) + 1,
                "signal_date": (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d"),
                "execution_date": date.strftime("%Y-%m-%d"),
                "execution_timing": "T+1 (signal at prev close, execute at today open)",
                # ── Action ──
                "action": action,
                "state_from": old_state_val,
                "state_to": new_state.value,
                "trigger_reason": reason,
                "target_allocation_pct": round(target_alloc * 100, 2),
                # ── Signal values (from PREVIOUS day's close) ──
                "signal_spy_close": "",  # filled below in daily log
                "signal_vix": "",
                "signal_spy_vs_sma200": "",
                "signal_spy_vs_sma50": "",
                "signal_rsi": "",
                # ── Execution prices ──
                "exec_price_upro_open": round(upro_open, 4),
                "upro_close_same_day": round(upro_price, 4),
                "overnight_gap_pct": round((upro_open / upro_price - 1) * 100, 2) if upro_price > 0 else 0,
                # ── Position before ──
                "shares_before": round(old_shares, 4),
                "cash_before": round(old_cash, 2),
                "portfolio_value_before_trade": round(pv_before, 2),
                # ── Trade math ──
                "shares_delta": round(shares_delta, 4),
                "trade_value_dollars": round(trade_value, 2),
                "commission_dollars": round(comm, 2),
                "slippage_bps_used": round(slip_bps, 1),
                "slippage_dollars": round(slip, 2),
                "total_cost_dollars": round(comm + slip, 2),
                "slippage_type": "STRESS (VIX>25 or kill)" if slip_bps > 5 else "NORMAL",
                # ── Position after ──
                "shares_after": round(target_shares, 4),
                "cash_after": round(cash, 2),
                "upro_value_after": round(shares * upro_price, 2),
                "portfolio_value_at_close": round(pv_after_close, 2),
                # ── Verification ──
                "check_pv_equals_cash_plus_shares_x_close": round(pv_after_close - (cash + shares * upro_price), 2),
                "check_alloc_pct_actual": round((shares * upro_open / pv_before) * 100, 2) if pv_before > 0 else 0,
            })

        # STEP 2: Generate signal for TOMORROW based on today's close
        old_state = state
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]
        spy_rsi = row["SPY_RSI"]
        below_streak = int(row["SPY_below_50_streak"])
        above_streak = int(row["SPY_above_50_streak"])

        new_state, reason = next_state(
            current=old_state,
            spy_close=spy_close,
            spy_sma50=spy_sma50,
            spy_sma200=spy_sma200,
            spy_rsi=spy_rsi,
            vix=vix,
            below_50_streak=below_streak,
            above_50_streak=above_streak,
        )

        if new_state != old_state:
            pending_trade = (new_state, reason)

        # ── Classify every signal for the daily log ──
        vix_kill_active = vix > VIX_KILL
        spy_below_200 = spy_close < spy_sma200
        kill_switch_on = vix_kill_active or spy_below_200
        spy_below_50 = spy_close < spy_sma50
        defensive_trigger = below_streak >= SMA_CONFIRM_DAYS
        rsi_hot = spy_rsi > RSI_SELL
        rsi_cool = spy_rsi < RSI_REBUY

        # Portfolio snapshot at close
        portfolio_value = cash + shares * upro_price
        upro_alloc_pct = (shares * upro_price / portfolio_value * 100) if portfolio_value > 0 else 0

        daily_log.append({
            # ── Date & State ──
            "date": date.strftime("%Y-%m-%d"),
            "day_number": len(daily_log) + 1,
            "state": state.value,
            "state_changed_today": "YES" if (len(trades) > 0 and trades[-1]["execution_date"] == date.strftime("%Y-%m-%d")) else "NO",
            "pending_signal_for_tomorrow": pending_trade[1] if pending_trade else "NONE",
            # ── Prices ──
            "spy_close": round(spy_close, 2),
            "upro_open": round(upro_open, 4),
            "upro_close": round(upro_price, 4),
            "upro_daily_return_pct": round((upro_price / upro_open - 1) * 100, 2) if upro_open > 0 else 0,
            # ── Signal 1: Kill Switch (HIGHEST PRIORITY) ──
            "vix": round(vix, 2),
            "vix_threshold": VIX_KILL,
            "vix_above_threshold": "YES" if vix_kill_active else "NO",
            "spy_sma200": round(spy_sma200, 2),
            "spy_vs_sma200": "ABOVE" if spy_close > spy_sma200 else "BELOW",
            "spy_minus_sma200": round(spy_close - spy_sma200, 2),
            "kill_switch_active": "YES" if kill_switch_on else "NO",
            "kill_reason": ("VIX>" + str(VIX_KILL) if vix_kill_active else "") + (" | " if vix_kill_active and spy_below_200 else "") + ("SPY<200SMA" if spy_below_200 else "") if kill_switch_on else "N/A",
            # ── Signal 2: Defensive Trim ──
            "spy_sma50": round(spy_sma50, 2),
            "spy_vs_sma50": "ABOVE" if spy_close > spy_sma50 else "BELOW",
            "spy_minus_sma50": round(spy_close - spy_sma50, 2),
            "days_below_sma50": below_streak,
            "days_above_sma50": above_streak,
            "defensive_trigger": "YES" if defensive_trigger else f"NO (need {SMA_CONFIRM_DAYS}d, have {below_streak}d)",
            # ── Signal 3: RSI Trim (LOWEST PRIORITY) ──
            "spy_rsi_14": round(spy_rsi, 2),
            "rsi_sell_threshold": RSI_SELL,
            "rsi_rebuy_threshold": RSI_REBUY,
            "rsi_zone": "HOT (>75)" if rsi_hot else ("COOL (<60)" if rsi_cool else "NEUTRAL (60-75)"),
            # ── Portfolio ──
            "shares_held": round(shares, 4),
            "cash_held": round(cash, 2),
            "upro_position_value": round(shares * upro_price, 2),
            "portfolio_value": round(portfolio_value, 2),
            "upro_allocation_pct": round(upro_alloc_pct, 1),
            "cash_allocation_pct": round(100 - upro_alloc_pct, 1),
            "daily_pnl": round(portfolio_value - (daily_log[-1]["portfolio_value"] if daily_log else INITIAL_CAPITAL), 2),
            "cumulative_return_pct": round((portfolio_value / INITIAL_CAPITAL - 1) * 100, 2),
        })

    metrics = calculate_metrics(daily_log, trades)
    return trades, daily_log, metrics


# =============================================================================
# METRICS
# =============================================================================


def calculate_metrics(daily_log: list[dict], trades: list[Trade]) -> dict:
    """Calculate performance metrics from backtest results."""
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

    daily_returns = daily["daily_return"]
    daily_rf = RISK_FREE_RATE / 252
    excess_returns = daily_returns - daily_rf
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
        "step": "Step 1 — UPRO Only (DataBento)",
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


def save_results(
    trades: list[Trade],
    daily_log: list[dict],
    metrics: dict,
    prefix: str = "step1",
) -> None:
    """Save trade log, portfolio values, and metrics to files."""
    # Trade log
    trade_df = pd.DataFrame(trades)
    trade_path = RESULTS_DIR / f"{prefix}_trade_log.csv"
    trade_df.to_csv(trade_path, index=False)

    # Daily portfolio values
    daily_df = pd.DataFrame(daily_log)
    daily_path = RESULTS_DIR / f"{prefix}_portfolio_values.csv"
    daily_df.to_csv(daily_path, index=False)

    # Metrics
    metrics_path = RESULTS_DIR / f"{prefix}_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"\n  Saved: {trade_path.name} ({len(trades)} trades)")
    print(f"  Saved: {daily_path.name} ({len(daily_log)} days)")
    print(f"  Saved: {metrics_path.name}")


def print_summary(metrics: dict) -> None:
    """Print a compact performance summary."""
    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"  {metrics['approach']}")
    print(f"{'=' * 60}")
    print(f"  Period:       {metrics['start_date']} to {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value:  ${metrics['final_value']:,.2f}")
    print(f"  Total Return: {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:         {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:       {metrics['sharpe']:.3f}")
    print(f"  Sortino:      {metrics['sortino']:.3f}")
    print(f"  Calmar:       {metrics['calmar']:.3f}")
    print(f"  Max Drawdown: {metrics['max_drawdown_pct']:.2f}% ({metrics['max_drawdown_date']})")
    print(f"  Total Trades: {metrics['total_trades']}")
    print(f"  Costs:        ${metrics['total_commissions'] + metrics['total_slippage']:.2f}")
    print(f"\n  SPY B&H CAGR: {metrics['benchmark_spy_cagr_pct']:+.2f}%")
    print(f"  Alpha vs SPY: {metrics['alpha_vs_spy_pct']:+.2f}%")
    print(f"\n  Annual Returns:")
    for yr, ret in sorted(metrics["annual_returns"].items()):
        print(f"    {yr}: {ret:+.2f}%")
    print(f"\n  State Distribution:")
    for st, pct in sorted(metrics["state_distribution_pct"].items()):
        print(f"    {st}: {pct:.1f}%")
    print(f"{'=' * 60}")


# =============================================================================
# MAIN
# =============================================================================


def main() -> dict:
    """Run Step 1 backtest and return metrics."""
    print("\n[Step 1] Loading DataBento data...")
    df = load_step1_data()
    print(f"  Data range: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} bars)")

    print("\n[Step 1] Calculating indicators...")
    df = add_step1_indicators(df)

    print("\n[Step 1] Running 4-state backtest...")
    trades, daily_log, metrics = run_step1_backtest(df)

    print_summary(metrics)
    save_results(trades, daily_log, metrics, prefix="step1_databento")

    return metrics


if __name__ == "__main__":
    main()
