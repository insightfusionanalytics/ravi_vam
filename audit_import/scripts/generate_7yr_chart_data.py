#!/usr/bin/env python3
"""
Generate 7.5-Year Chart Data for Ravi VAM Interactive Verifier.

Loads SPY daily OHLC from DataBento, computes SMA(50), SMA(100), SMA(200),
RSI(14), loads VIX from CBOE, loads pre-computed trade data for 7 strategies
from the databento_7yr_chart_data.json, and outputs a single JSON file
with all data needed by the chart verifier dashboard.

Output: clients/ravi_vam/delivery/ravi_vam_7yr_chart_data.json
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent  # clients/ravi_vam/
ENGINE_DIR = PROJECT_DIR.parent.parent  # _engine/code/
SPY_PATH = ENGINE_DIR / "data" / "databento" / "equities" / "SPY_daily.csv"
VIX_PATH = ENGINE_DIR / "data" / "cboe" / "VIX_daily.csv"
TRADE_DATA_PATH = PROJECT_DIR / "results" / "databento_7yr_chart_data.json"
OUTPUT_DIR = PROJECT_DIR / "delivery"
OUTPUT_PATH = OUTPUT_DIR / "ravi_vam_7yr_chart_data.json"


def load_spy() -> pd.DataFrame:
    """Load SPY daily OHLCV data from DataBento CSV."""
    if not SPY_PATH.exists():
        print(f"ERROR: Missing {SPY_PATH}")
        sys.exit(1)
    df = pd.read_csv(SPY_PATH, parse_dates=["datetime"])
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    print(f"  SPY: {len(df)} bars, {df.index[0].date()} to {df.index[-1].date()}")
    return df


def load_vix() -> pd.DataFrame:
    """Load VIX daily data from CBOE CSV."""
    if not VIX_PATH.exists():
        print(f"ERROR: Missing {VIX_PATH}")
        sys.exit(1)
    df = pd.read_csv(VIX_PATH, parse_dates=["datetime"])
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    print(f"  VIX: {len(df)} bars, {df.index[0].date()} to {df.index[-1].date()}")
    return df


def load_trade_data() -> dict[str, Any]:
    """Load pre-computed trade data for 7 strategies."""
    if not TRADE_DATA_PATH.exists():
        print(f"ERROR: Missing {TRADE_DATA_PATH}")
        sys.exit(1)
    with open(TRADE_DATA_PATH) as f:
        data = json.load(f)
    print(f"  Trade data: {len(data)} strategies loaded")
    for name, strat in data.items():
        print(
            f"    {name}: {len(strat['trades'])} trades, {len(strat['equity_curve'])} equity points"
        )
    return data


def compute_sma(series: pd.Series, window: int) -> pd.Series:
    """Compute simple moving average."""
    return series.rolling(window).mean()


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI using Wilder smoothing (EWM with alpha=1/period)."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def build_candles(spy: pd.DataFrame) -> list[dict]:
    """Build OHLC candle data for the chart."""
    candles = []
    for date, row in spy.iterrows():
        candles.append(
            {
                "time": date.strftime("%Y-%m-%d"),
                "open": round(float(row["open"]), 2),
                "high": round(float(row["high"]), 2),
                "low": round(float(row["low"]), 2),
                "close": round(float(row["close"]), 2),
            }
        )
    return candles


def build_indicator_series(
    spy: pd.DataFrame,
    sma50: pd.Series,
    sma100: pd.Series,
    sma200: pd.Series,
    rsi: pd.Series,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Build time-value arrays for SMA50, SMA100, SMA200, and RSI."""
    sma50_data: list[dict] = []
    sma100_data: list[dict] = []
    sma200_data: list[dict] = []
    rsi_data: list[dict] = []

    for date in spy.index:
        date_str = date.strftime("%Y-%m-%d")
        if not np.isnan(sma50[date]):
            sma50_data.append({"time": date_str, "value": round(float(sma50[date]), 2)})
        if not np.isnan(sma100[date]):
            sma100_data.append({"time": date_str, "value": round(float(sma100[date]), 2)})
        if not np.isnan(sma200[date]):
            sma200_data.append({"time": date_str, "value": round(float(sma200[date]), 2)})
        if not np.isnan(rsi[date]):
            rsi_data.append({"time": date_str, "value": round(float(rsi[date]), 2)})

    return sma50_data, sma100_data, sma200_data, rsi_data


def build_vix_series(vix: pd.DataFrame, spy_dates: pd.DatetimeIndex) -> list[dict]:
    """Build VIX time-value array, aligned to SPY trading dates."""
    vix_data: list[dict] = []
    for date in spy_dates:
        if date in vix.index:
            val = float(vix.loc[date, "close"])
            if not np.isnan(val):
                vix_data.append(
                    {
                        "time": date.strftime("%Y-%m-%d"),
                        "value": round(val, 2),
                    }
                )
    return vix_data


def parse_strategy_params(name: str) -> dict[str, Any]:
    """Extract strategy parameters from the strategy name string."""
    parts = name.split("_")
    params: dict[str, Any] = {}

    # Parse SMA fast/slow (e.g., SMA50_200 or SMA100_200)
    if parts[0].startswith("SMA"):
        params["sma_fast"] = int(parts[0].replace("SMA", ""))
        params["sma_slow"] = int(parts[1])

    # Parse RSI threshold (e.g., RSI50 or RSI45)
    for p in parts:
        if p.startswith("RSI"):
            params["rsi_threshold"] = int(p.replace("RSI", ""))

    # Parse UPRO percentage (e.g., UPRO100 or UPRO75)
    for p in parts:
        if p.startswith("UPRO"):
            params["upro_pct"] = int(p.replace("UPRO", ""))

    # Parse defensive instrument (SHY, cash, TLT)
    for defensive in ["SHY", "cash", "TLT"]:
        if defensive in parts:
            params["defensive"] = defensive

    # Parse delay
    for p in parts:
        if p.startswith("delay"):
            params["sma_delay"] = int(p.replace("delay", ""))

    # Parse VIX kill level
    for p in parts:
        if p.startswith("vix"):
            val = p.replace("vix", "")
            params["vix_kill"] = float(val) if val != "None" else None

    return params


def generate_explanation(
    trade: dict[str, Any],
    params: dict[str, Any],
    prev_equity: float | None,
) -> str:
    """Generate a human-readable explanation for a trade.

    Args:
        trade: Trade dict with date, from, to, equity, spy_close, sma_fast, sma_slow, rsi, vix.
        params: Strategy parameters extracted from strategy name.
        prev_equity: Equity at the previous trade (for PnL calculation).

    Returns:
        Human-readable explanation string.
    """
    from_state = trade["from"]
    to_state = trade["to"]
    sma_f = trade["sma_fast"]
    sma_s = trade["sma_slow"]
    rsi_val = trade["rsi"]
    vix_val = trade["vix"]
    equity = trade["equity"]
    sma_fast_period = params.get("sma_fast", 50)
    sma_slow_period = params.get("sma_slow", 200)
    rsi_threshold = params.get("rsi_threshold", 50)
    vix_kill = params.get("vix_kill")

    # Determine action direction
    bullish_states = {"BULL_FULL", "MOMENTUM", "BULL_CAUTION", "RECOVERY"}
    defensive_states = {"BEAR_HEDGE", "CRASH", "NEUTRAL", "WARMUP"}

    if to_state in bullish_states and from_state in defensive_states:
        action = "BOUGHT"
    elif to_state in defensive_states and from_state in bullish_states:
        action = "SOLD"
    else:
        action = "ROTATED"

    # Build indicator context
    parts: list[str] = []

    if sma_f is not None and sma_s is not None:
        direction = "ABOVE" if sma_f > sma_s else "BELOW"
        parts.append(
            f"SMA({sma_fast_period}) = {sma_f:.2f} crossed {direction} "
            f"SMA({sma_slow_period}) = {sma_s:.2f}"
        )

    if rsi_val is not None:
        rel = ">" if rsi_val > rsi_threshold else "<"
        parts.append(f"RSI(14) = {rsi_val:.1f} {rel} {rsi_threshold} threshold")

    if vix_val is not None and vix_kill is not None:
        vix_status = "below" if vix_val < vix_kill else "ABOVE"
        parts.append(f"VIX = {vix_val:.1f} ({vix_status} kill level {vix_kill:.0f})")
    elif vix_val is not None:
        parts.append(f"VIX = {vix_val:.1f}")

    indicator_text = " AND ".join(parts) if parts else "regime change"

    # PnL since last trade
    pnl_text = ""
    if prev_equity is not None and prev_equity > 0:
        pnl_dollar = equity - prev_equity
        sign = "+" if pnl_dollar >= 0 else ""
        pnl_text = f" PnL since last trade: {sign}${pnl_dollar:,.0f}"

    # State transition
    state_text = f"State: {from_state} -> {to_state}"

    return f"{action}: {indicator_text}. {state_text}.{pnl_text}"


def build_strategies(
    trade_data: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Build the strategies dict with enriched trade data and explanations."""
    strategies: dict[str, dict[str, Any]] = {}

    for strat_name, strat_data in trade_data.items():
        params = parse_strategy_params(strat_name)
        trades_raw = strat_data["trades"]
        enriched_trades: list[dict[str, Any]] = []
        prev_equity: float | None = None

        for trade in trades_raw:
            explanation = generate_explanation(trade, params, prev_equity)

            # Determine BUY/SELL/ROTATE type
            bullish_states = {"BULL_FULL", "MOMENTUM", "BULL_CAUTION", "RECOVERY"}
            defensive_states = {"BEAR_HEDGE", "CRASH", "NEUTRAL", "WARMUP"}
            from_state = trade["from"]
            to_state = trade["to"]

            if to_state in bullish_states and from_state in defensive_states:
                trade_type = "BUY"
            elif to_state in defensive_states and from_state in bullish_states:
                trade_type = "SELL"
            else:
                trade_type = "ROTATE"

            enriched_trades.append(
                {
                    "date": trade["date"],
                    "type": trade_type,
                    "from_state": from_state,
                    "to_state": to_state,
                    "equity": trade["equity"],
                    "spy_close": trade["spy_close"],
                    "sma_fast": trade["sma_fast"],
                    "sma_slow": trade["sma_slow"],
                    "rsi": trade["rsi"],
                    "vix": trade["vix"],
                    "explanation": explanation,
                }
            )

            prev_equity = trade["equity"]

        strategies[strat_name] = {
            "params": params,
            "trades": enriched_trades,
            "equity_curve": strat_data["equity_curve"],
        }

    return strategies


def main() -> None:
    """Generate the 7.5-year chart data JSON for the Ravi VAM dashboard."""
    print("=" * 60)
    print("GENERATING 7.5-YEAR CHART DATA FOR RAVI VAM VERIFIER")
    print("=" * 60)

    # Load data
    print("\nLoading data...")
    spy = load_spy()
    vix = load_vix()
    trade_data = load_trade_data()

    # Compute indicators on SPY
    print("\nComputing indicators...")
    spy_close = spy["close"]
    sma50 = compute_sma(spy_close, 50)
    sma100 = compute_sma(spy_close, 100)
    sma200 = compute_sma(spy_close, 200)
    rsi = compute_rsi(spy_close, 14)

    print(f"  SMA50 valid from: {sma50.dropna().index[0].date()}")
    print(f"  SMA100 valid from: {sma100.dropna().index[0].date()}")
    print(f"  SMA200 valid from: {sma200.dropna().index[0].date()}")
    print(f"  RSI valid from: {rsi.dropna().index[0].date()}")

    # Build output arrays
    print("\nBuilding output arrays...")
    candles = build_candles(spy)
    sma50_data, sma100_data, sma200_data, rsi_data = build_indicator_series(
        spy, sma50, sma100, sma200, rsi
    )
    vix_data = build_vix_series(vix, spy.index)
    strategies = build_strategies(trade_data)

    print(f"  Candles: {len(candles)}")
    print(f"  SMA50: {len(sma50_data)} points")
    print(f"  SMA100: {len(sma100_data)} points")
    print(f"  SMA200: {len(sma200_data)} points")
    print(f"  RSI: {len(rsi_data)} points")
    print(f"  VIX: {len(vix_data)} points")
    print(f"  Strategies: {len(strategies)}")
    for name, strat in strategies.items():
        print(f"    {name}: {len(strat['trades'])} trades")

    # Assemble output
    output = {
        "metadata": {
            "title": "Ravi VAM 7.5-Year Strategy Verification Dashboard",
            "data_source": "DataBento XNAS.ITCH + CBOE VIX",
            "data_range": f"{spy.index[0].date()} to {spy.index[-1].date()}",
            "total_bars": len(candles),
            "num_strategies": len(strategies),
            "generated": datetime.now().isoformat(),
        },
        "candles": candles,
        "sma50": sma50_data,
        "sma100": sma100_data,
        "sma200": sma200_data,
        "rsi": rsi_data,
        "vix": vix_data,
        "strategies": {
            name: {
                "params": strat["params"],
                "trades": strat["trades"],
                "equity_curve": strat["equity_curve"],
            }
            for name, strat in strategies.items()
        },
    }

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=None, separators=(",", ":"))

    file_size_mb = OUTPUT_PATH.stat().st_size / (1024 * 1024)
    print(f"\nOutput: {OUTPUT_PATH}")
    print(f"Size: {file_size_mb:.2f} MB")

    # Sample explanation from first strategy
    first_strat = next(iter(strategies.values()))
    if first_strat["trades"]:
        print(f"\nSample explanations:")
        for trade in first_strat["trades"][:3]:
            print(f"  {trade['date']}: {trade['explanation']}")

    print("\nDone.")


if __name__ == "__main__":
    main()
