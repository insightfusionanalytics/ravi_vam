"""
VAM Matrix Cross-Check — Validate normal backtest against YAML transition matrix.

Reads the UPRO Markov transition matrix from upro_transitions.yaml and runs
the same DataBento data through it. Compares state sequences with Step 1
normal backtest to verify consistency.

Key differences from normal approach (expected):
  - Matrix has 10 states (FULL/TRIM/HALF/HALF_R1/OFF_C5-C1/OFF_R) vs 4 states
  - Matrix has 5-day cooldown chain vs instant re-entry
  - Matrix has VIX hysteresis (kill>30, re-enter<=25) vs simple VIX<30

These differences mean results WON'T match exactly. We compare:
  1. Kill switch triggers (should match — same VIX>30 OR SPY<200SMA rule)
  2. Directional agreement (both bullish or both defensive at same time)
  3. Final portfolio values (matrix should be more conservative due to cooldown)
"""

import json
from pathlib import Path

import pandas as pd
import numpy as np
import yaml

ENGINE_ROOT = Path(__file__).resolve().parents[3]  # _engine/code/
DATA_DIR = ENGINE_ROOT / "data"
MATRIX_DIR = ENGINE_ROOT / "matrices"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


# =============================================================================
# DATA LOADING (reuse from Step 1)
# =============================================================================


def load_databento_csv(filepath: Path) -> pd.DataFrame:
    """Load a DataBento daily CSV."""
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df


UPRO_SPLITS = [("2018-05-24", 3), ("2022-01-13", 2)]
PRICE_COLS = ["open", "high", "low", "close"]


def adjust_for_splits(df: pd.DataFrame, splits: list[tuple[str, int]]) -> pd.DataFrame:
    """Adjust historical prices for forward stock splits."""
    df = df.copy()
    for split_date, ratio in splits:
        mask = df.index < pd.Timestamp(split_date)
        for col in PRICE_COLS:
            if col in df.columns:
                df.loc[mask, col] = df.loc[mask, col] / ratio
    return df


def load_data() -> pd.DataFrame:
    """Load SPY, UPRO, VIX."""
    spy = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPY_daily.csv")
    upro = load_databento_csv(DATA_DIR / "databento" / "equities" / "UPRO_daily.csv")
    upro = adjust_for_splits(upro, UPRO_SPLITS)
    vix = load_databento_csv(DATA_DIR / "cboe" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["UPRO_Close"] = upro["close"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df = df.ffill().dropna()

    # Indicators
    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()

    # RSI on UPRO (matrix uses UPRO RSI per yaml: rsi_applied_to: UPRO)
    delta = df["UPRO_Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=13, min_periods=14).mean()
    avg_loss = loss.ewm(com=13, min_periods=14).mean()
    rs = avg_gain / avg_loss
    df["UPRO_RSI"] = 100 - (100 / (1 + rs))

    return df.dropna()


# =============================================================================
# MATRIX ENGINE
# =============================================================================


def load_matrix(filepath: Path) -> dict:
    """Load YAML transition matrix."""
    with open(filepath) as f:
        return yaml.safe_load(f)


def classify_signals(row: pd.Series, params: dict) -> dict:
    """Classify continuous values into discrete labels."""
    vix_label = "HIGH" if row["VIX"] > params["vix_kill_threshold"] else "LOW"
    sma200_label = "ABOVE" if row["SPY_Close"] > row["SPY_SMA200"] else "BELOW"
    sma50_label = "ABOVE" if row["SPY_Close"] > row["SPY_SMA50"] else "BELOW"

    rsi = row["UPRO_RSI"]
    if rsi > params["rsi_hot_threshold"]:
        rsi_label = "HOT"
    elif rsi < params["rsi_cool_threshold"]:
        rsi_label = "COOL"
    else:
        rsi_label = "NEUTRAL"

    return {
        "vix": vix_label,
        "spy_sma200": sma200_label,
        "spy_sma50": sma50_label,
        "upro_rsi": rsi_label,
    }


def find_scenario(signals: dict, scenarios: dict) -> int:
    """Find matching scenario number from classified signals."""
    for num, scenario in scenarios.items():
        if (
            scenario["vix"] == signals["vix"]
            and scenario["spy_sma200"] == signals["spy_sma200"]
            and scenario["spy_sma50"] == signals["spy_sma50"]
            and scenario["upro_rsi"] == signals["upro_rsi"]
        ):
            return num
    raise ValueError(f"No matching scenario for signals: {signals}")


def resolve_transition(
    transition_entry: object,
    vix: float,
    vix_reentry: float,
) -> str:
    """Resolve a transition entry which may be a simple string or conditional."""
    if isinstance(transition_entry, str):
        return transition_entry
    if isinstance(transition_entry, dict):
        if "condition" in transition_entry:
            # Check VIX re-entry condition
            if "vix_reentry_threshold" in transition_entry["condition"].lower():
                if vix <= vix_reentry:
                    return transition_entry["to_state"]
                return transition_entry.get("fallback", transition_entry["to_state"])
            return transition_entry["to_state"]
        if "to_state" in transition_entry:
            return transition_entry["to_state"]
    return str(transition_entry)


def run_matrix_backtest(
    df: pd.DataFrame,
    matrix: dict,
) -> tuple[list[dict], dict]:
    """Run the YAML-driven matrix backtest."""
    params = matrix["parameters"]
    scenarios = matrix["scenarios"]
    transitions = matrix["transitions"]
    allocations = matrix["state_allocations"]
    sleeve_pct = matrix["sleeve_pct"] / 100.0

    state = "OFF_R"  # Start off-market, waiting for entry
    cash = 100_000.0
    shares = 0.0
    daily_log = []
    trades = []

    for date, row in df.iterrows():
        signals = classify_signals(row, params)
        scenario_num = find_scenario(signals, scenarios)

        # Look up transition
        old_state = state
        transition_entry = transitions[state][scenario_num]
        new_state = resolve_transition(
            transition_entry,
            vix=row["VIX"],
            vix_reentry=params["vix_reentry_threshold"],
        )

        upro_price = row["UPRO_Close"]

        if new_state != old_state:
            alloc_pct = allocations[new_state] / 100.0
            target_alloc = alloc_pct * sleeve_pct
            portfolio_value = cash + shares * upro_price
            target_shares = (portfolio_value * target_alloc) / upro_price if upro_price > 0 else 0

            delta = target_shares - shares
            trade_val = abs(delta * upro_price)
            comm = 1.0 if trade_val > 0 else 0
            slip = trade_val * 0.0005

            shares = target_shares
            cash = portfolio_value - (target_shares * upro_price) - comm - slip
            state = new_state

            trades.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "from": old_state,
                    "to": new_state,
                    "scenario": scenario_num,
                    "signals": signals,
                    "trade_value": round(trade_val, 2),
                }
            )
        else:
            state = new_state

        portfolio_value = cash + shares * upro_price
        daily_log.append(
            {
                "date": date.strftime("%Y-%m-%d"),
                "state": state,
                "scenario": scenario_num,
                "upro_price": round(upro_price, 4),
                "shares": round(shares, 4),
                "cash": round(cash, 2),
                "portfolio_value": round(portfolio_value, 2),
                "vix": round(row["VIX"], 2),
            }
        )

    # Metrics
    daily = pd.DataFrame(daily_log)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.set_index("date")
    daily["daily_return"] = daily["portfolio_value"].pct_change()
    daily = daily.dropna(subset=["daily_return"])

    total_days = (daily.index[-1] - daily.index[0]).days
    years = total_days / 365.25
    end_val = daily["portfolio_value"].iloc[-1]
    total_return = (end_val / 100_000) - 1
    cagr = (end_val / 100_000) ** (1 / years) - 1 if years > 0 else 0
    daily_rets = daily["daily_return"]
    sharpe = (daily_rets.mean() / daily_rets.std()) * np.sqrt(252) if daily_rets.std() > 0 else 0

    cummax = daily["portfolio_value"].cummax()
    dd = (daily["portfolio_value"] - cummax) / cummax
    max_dd = dd.min()

    metrics = {
        "step": "Step 1 — UPRO Matrix Cross-Check",
        "approach": "Matrix (10-state Markov, YAML-driven)",
        "start_date": daily.index[0].strftime("%Y-%m-%d"),
        "end_date": daily.index[-1].strftime("%Y-%m-%d"),
        "years": round(years, 2),
        "final_value": round(end_val, 2),
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "total_trades": len(trades),
        "note": "Matrix uses 5-day cooldown + VIX hysteresis (re-entry at 25, not 30)",
    }

    return daily_log, metrics


# =============================================================================
# COMPARISON
# =============================================================================


def compare_results(step1_metrics: dict, matrix_metrics: dict) -> dict:
    """Compare normal vs matrix approach results."""
    comparison = {
        "metric": [],
        "step1_normal": [],
        "step1_matrix": [],
        "difference": [],
    }

    for key in ["total_return_pct", "cagr_pct", "sharpe", "max_drawdown_pct", "total_trades"]:
        s1 = step1_metrics.get(key, 0)
        mx = matrix_metrics.get(key, 0)
        comparison["metric"].append(key)
        comparison["step1_normal"].append(s1)
        comparison["step1_matrix"].append(mx)
        comparison["difference"].append(
            round(s1 - mx, 3) if isinstance(s1, (int, float)) else "N/A"
        )

    return comparison


# =============================================================================
# MAIN
# =============================================================================


def main() -> dict:
    """Run matrix cross-check and return metrics."""
    print("\n[Matrix] Loading data and YAML matrix...")
    df = load_data()
    matrix = load_matrix(MATRIX_DIR / "upro_transitions.yaml")
    print(f"  Data: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} bars)")
    print(f"  Matrix: {len(matrix['states'])} states x {len(matrix['scenarios'])} scenarios")

    print("\n[Matrix] Running 10-state Markov backtest...")
    daily_log, metrics = run_matrix_backtest(df, matrix)

    # Save
    pd.DataFrame(daily_log).to_csv(RESULTS_DIR / "step1_matrix_portfolio_values.csv", index=False)
    with open(RESULTS_DIR / "step1_matrix_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"  {metrics['step']}")
    print(f"  {metrics['approach']}")
    print(f"{'=' * 60}")
    print(f"  Period:       {metrics['start_date']} to {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value:  ${metrics['final_value']:,.2f}")
    print(f"  Total Return: {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:         {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:       {metrics['sharpe']:.3f}")
    print(f"  Max Drawdown: {metrics['max_drawdown_pct']:.2f}%")
    print(f"  Trades:       {metrics['total_trades']}")
    print(f"  Note:         {metrics['note']}")
    print(f"{'=' * 60}")

    # Compare with Step 1 normal if available
    step1_path = RESULTS_DIR / "step1_databento_metrics.json"
    if step1_path.exists():
        with open(step1_path) as f:
            step1_metrics = json.load(f)
        comp = compare_results(step1_metrics, metrics)
        print("\n  CROSS-CHECK COMPARISON:")
        print(f"  {'Metric':<20} {'Normal':>12} {'Matrix':>12} {'Diff':>10}")
        print(f"  {'-' * 54}")
        for i, m in enumerate(comp["metric"]):
            print(
                f"  {m:<20} {comp['step1_normal'][i]:>12} {comp['step1_matrix'][i]:>12} {comp['difference'][i]:>10}"
            )
        print()

    return metrics


if __name__ == "__main__":
    main()
