#!/usr/bin/env python3
"""
Generate Chart Data for Ravi VAM Interactive Verifier
=====================================================
Loads SPY daily data from DataBento, computes SMA(50), SMA(200), RSI(14),
runs the 7-state regime classification, computes the equity curve with
UPRO/TQQQ/SHY returns, and records every state transition with full context.

Output: clients/ravi_vam/delivery/ravi_vam_chart_data.json

Uses the BEST parameter set from the sweep:
  SMA(50,200), RSI entry=40, 75% UPRO / 25% TQQQ, SHY defensive,
  0 confirmation days, bull_caution=True, no VIX kill
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent  # clients/ravi_vam/
ENGINE_DIR = PROJECT_DIR.parent.parent  # _engine/code/
DATA_DIR = ENGINE_DIR / "data" / "databento" / "equities"
OUTPUT_DIR = PROJECT_DIR / "delivery"

# ---------------------------------------------------------------------------
# Strategy Parameters (best variant from sweep)
# ---------------------------------------------------------------------------
SMA_FAST = 50
SMA_SLOW = 200
RSI_ENTRY = 40
RSI_CAUTION_LO = 30  # rsi_entry - 10
UPRO_PCT = 75
TQQQ_PCT = 25
DEFENSIVE = "SHY"
BULL_CAUTION_ENABLED = True
CONFIRM_DAYS = 0
INITIAL_CAPITAL = 100_000.0
COMMISSION_PCT = 0.0010
SLIPPAGE_PCT = 0.0005

# State colors (for the HTML to use)
STATE_COLORS = {
    "WARMUP": "#333333",
    "BULL_FULL": "#16a34a",
    "BULL_CAUTION": "#eab308",
    "MOMENTUM": "#22c55e",
    "NEUTRAL": "#6b7280",
    "RECOVERY": "#3b82f6",
    "BEAR_HEDGE": "#ef4444",
    "CRASH": "#991b1b",
}

# State allocations (human-readable)
STATE_ALLOCATIONS = {
    "WARMUP": "100% Cash",
    "CRASH": "100% Cash",
    "NEUTRAL": "100% SHY",
    "BULL_FULL": "75% UPRO + 25% TQQQ",
    "MOMENTUM": "100% UPRO",
    "RECOVERY": "25% UPRO + 75% SHY",
    "BEAR_HEDGE": "100% SHY",
    "BULL_CAUTION": "50% UPRO + 50% SHY",
}


def load_data() -> dict[str, pd.DataFrame]:
    """Load SPY, UPRO, TQQQ, SHY daily CSVs."""
    dfs = {}
    for sym in ["SPY", "UPRO", "TQQQ", "SHY"]:
        path = DATA_DIR / f"{sym}_daily.csv"
        if not path.exists():
            print(f"ERROR: Missing {path}")
            sys.exit(1)
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.sort_index(inplace=True)
        dfs[sym] = df
    return dfs


def align_dates(dfs: dict[str, pd.DataFrame]) -> pd.DatetimeIndex:
    """Find common trading dates."""
    common = dfs["SPY"].index
    for sym in ["UPRO", "TQQQ", "SHY"]:
        common = common.intersection(dfs[sym].index)
    return common.sort_values()


def compute_indicators(spy_close: pd.Series) -> pd.DataFrame:
    """Compute SMA(50), SMA(200), RSI(14)."""
    df = pd.DataFrame({"close": spy_close})
    df["sma_fast"] = spy_close.rolling(SMA_FAST).mean()
    df["sma_slow"] = spy_close.rolling(SMA_SLOW).mean()

    # RSI with Wilder smoothing
    delta = spy_close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - (100 / (1 + rs))

    return df


def classify_regimes(indicators: pd.DataFrame) -> pd.Series:
    """Classify each bar into one of 7 regime states."""
    n = len(indicators)
    sma_f = indicators["sma_fast"].values
    sma_s = indicators["sma_slow"].values
    rsi = indicators["rsi_14"].values

    regimes = np.full(n, "WARMUP", dtype=object)
    bull_raw = sma_f > sma_s

    if CONFIRM_DAYS > 0:
        bull_confirmed = np.zeros(n, dtype=bool)
        consec = 0
        for i in range(n):
            if np.isnan(sma_f[i]) or np.isnan(sma_s[i]):
                consec = 0
                continue
            if bull_raw[i]:
                consec += 1
            else:
                consec = 0
            if consec >= CONFIRM_DAYS:
                bull_confirmed[i] = True

        bear_raw = sma_f < sma_s
        bear_confirmed = np.zeros(n, dtype=bool)
        consec = 0
        for i in range(n):
            if np.isnan(sma_f[i]) or np.isnan(sma_s[i]):
                consec = 0
                continue
            if bear_raw[i]:
                consec += 1
            else:
                consec = 0
            if consec >= CONFIRM_DAYS:
                bear_confirmed[i] = True
    else:
        bull_confirmed = bull_raw
        bear_confirmed = ~bull_raw

    for i in range(n):
        if np.isnan(sma_f[i]) or np.isnan(sma_s[i]) or np.isnan(rsi[i]):
            regimes[i] = "WARMUP"
            continue

        if rsi[i] < 20:
            regimes[i] = "CRASH"
            continue

        if abs(sma_f[i] - sma_s[i]) / sma_s[i] < 0.005:
            regimes[i] = "NEUTRAL"
            continue

        if bull_confirmed[i]:
            if rsi[i] > 65:
                regimes[i] = "MOMENTUM"
            elif BULL_CAUTION_ENABLED and RSI_CAUTION_LO <= rsi[i] <= RSI_ENTRY:
                regimes[i] = "BULL_CAUTION"
            elif rsi[i] > RSI_ENTRY:
                regimes[i] = "BULL_FULL"
            else:
                regimes[i] = "BULL_CAUTION" if BULL_CAUTION_ENABLED else "BEAR_HEDGE"
            continue

        if bear_confirmed[i]:
            if rsi[i] > 50:
                lookback_start = max(0, i - 5)
                if np.any(rsi[lookback_start:i] < 30):
                    regimes[i] = "RECOVERY"
                    continue
            regimes[i] = "BEAR_HEDGE"
            continue

        regimes[i] = "BEAR_HEDGE"

    return pd.Series(regimes, index=indicators.index)


def run_backtest_with_equity(
    regimes: pd.Series,
    returns: dict[str, pd.Series],
) -> tuple[np.ndarray, list[dict], dict]:
    """Run backtest, return equity curve, transitions, and summary stats."""

    upro_w = UPRO_PCT / 100.0
    tqqq_w = TQQQ_PCT / 100.0

    allocations = {
        "WARMUP": {"CASH": 1.0},
        "CRASH": {"CASH": 1.0},
        "NEUTRAL": {"SHY": 1.0},
        "BULL_FULL": {"UPRO": upro_w, "TQQQ": tqqq_w},
        "MOMENTUM": {"UPRO": 1.0},
        "RECOVERY": {"UPRO": 0.25, "SHY": 0.75},
        "BEAR_HEDGE": {"SHY": 1.0},
        "BULL_CAUTION": {"UPRO": 0.50, "SHY": 0.50},
    }

    n = len(regimes)
    equity = INITIAL_CAPITAL
    equity_values = np.empty(n)
    prev_alloc: dict = {}
    prev_regime = None
    transitions_list: list[dict] = []
    tc_rate = COMMISSION_PCT + SLIPPAGE_PCT
    regime_arr = regimes.values
    dates = regimes.index
    total_transitions = 0
    wins = 0
    losses = 0
    best_trade_pct = -999.0
    worst_trade_pct = 999.0
    entry_equity = INITIAL_CAPITAL

    for i in range(n):
        regime = regime_arr[i]
        alloc = allocations.get(regime, {"CASH": 1.0})

        # Daily return from previous allocation
        daily_ret = 0.0
        for sym, weight in prev_alloc.items():
            if sym == "CASH":
                continue
            ret_val = returns[sym].iloc[i]
            if not np.isnan(ret_val):
                daily_ret += weight * ret_val

        # Transaction costs on allocation change
        tc = 0.0
        if prev_regime is not None and regime != prev_regime:
            all_syms = set(list(alloc.keys()) + list(prev_alloc.keys()))
            turnover = sum(abs(alloc.get(s, 0.0) - prev_alloc.get(s, 0.0)) for s in all_syms)
            tc = turnover * tc_rate
            total_transitions += 1

            # Compute trade return for the segment that just ended
            trade_pct = (equity / entry_equity - 1) * 100 if entry_equity > 0 else 0.0
            if trade_pct > 0:
                wins += 1
            elif trade_pct < 0:
                losses += 1

            if trade_pct > best_trade_pct:
                best_trade_pct = trade_pct
            if trade_pct < worst_trade_pct:
                worst_trade_pct = trade_pct

            entry_equity = equity

        equity *= 1 + daily_ret - tc
        equity_values[i] = equity

        # Record transition
        if prev_regime is not None and regime != prev_regime:
            transitions_list.append(
                {
                    "date": dates[i].strftime("%Y-%m-%d"),
                    "from_state": prev_regime,
                    "to_state": regime,
                    "portfolio_value": round(equity, 2),
                }
            )

        prev_alloc = alloc
        prev_regime = regime

    # Compute summary metrics
    total_return = (equity / INITIAL_CAPITAL - 1) * 100
    n_years = (dates[-1] - dates[0]).days / 365.25
    if n_years <= 0:
        n_years = 1.0
    cagr = ((equity / INITIAL_CAPITAL) ** (1 / n_years) - 1) * 100

    daily_rets = np.diff(equity_values) / equity_values[:-1]
    daily_rets = daily_rets[~np.isnan(daily_rets)]
    sharpe = (
        (np.mean(daily_rets) / np.std(daily_rets)) * np.sqrt(252) if np.std(daily_rets) > 0 else 0.0
    )

    cummax = np.maximum.accumulate(equity_values)
    drawdowns = (equity_values - cummax) / cummax
    max_dd = np.min(drawdowns) * 100

    summary = {
        "total_trades": total_transitions,
        "winning_trades": wins,
        "losing_trades": losses,
        "win_rate_pct": round(wins / max(total_transitions, 1) * 100, 1),
        "total_return_pct": round(total_return, 2),
        "cagr_pct": round(cagr, 2),
        "sharpe": round(sharpe, 4),
        "max_drawdown_pct": round(max_dd, 2),
        "final_equity": round(equity, 2),
        "best_trade_pct": round(best_trade_pct, 2),
        "worst_trade_pct": round(worst_trade_pct, 2),
        "current_state": str(regime_arr[-1]),
    }

    return equity_values, transitions_list, summary


def generate_transition_reasons(
    transitions: list[dict],
    indicators: pd.DataFrame,
    regimes: pd.Series,
) -> list[dict]:
    """Enrich each transition with indicator values and human-readable reasons."""
    enriched = []
    dates = regimes.index

    for t in transitions:
        date_str = t["date"]
        date_ts = pd.Timestamp(date_str)

        # Get indicator values at transition
        if date_ts in indicators.index:
            sma_f = indicators.loc[date_ts, "sma_fast"]
            sma_s = indicators.loc[date_ts, "sma_slow"]
            rsi_val = indicators.loc[date_ts, "rsi_14"]
            close_val = indicators.loc[date_ts, "close"]
        else:
            sma_f = sma_s = rsi_val = close_val = None

        # Compute duration in previous state
        idx = dates.get_loc(date_ts)
        if isinstance(idx, slice):
            idx = idx.start
        prev_state = t["from_state"]
        duration = 0
        for j in range(idx - 1, -1, -1):
            if regimes.iloc[j] == prev_state:
                duration += 1
            else:
                break

        # Build reason text
        reasons = []
        to_state = t["to_state"]
        from_state = t["from_state"]

        if sma_f is not None and sma_s is not None:
            if sma_f > sma_s:
                reasons.append(f"SMA(50) = {sma_f:.2f} is ABOVE SMA(200) = {sma_s:.2f} (bullish)")
            else:
                reasons.append(f"SMA(50) = {sma_f:.2f} is BELOW SMA(200) = {sma_s:.2f} (bearish)")

            sma_gap_pct = abs(sma_f - sma_s) / sma_s * 100
            if sma_gap_pct < 0.5:
                reasons.append(f"SMA gap = {sma_gap_pct:.2f}% (< 0.5% = convergence zone)")

        if rsi_val is not None:
            if rsi_val < 20:
                reasons.append(f"RSI(14) = {rsi_val:.1f} < 20 (CRASH level)")
            elif rsi_val < RSI_ENTRY:
                reasons.append(f"RSI(14) = {rsi_val:.1f} < {RSI_ENTRY} threshold (cautious)")
            elif rsi_val > 65:
                reasons.append(f"RSI(14) = {rsi_val:.1f} > 65 (strong momentum)")
            else:
                reasons.append(f"RSI(14) = {rsi_val:.1f} > {RSI_ENTRY} threshold (entry confirmed)")

        # Determine if this is a buy or sell/defensive transition
        bullish_states = {"BULL_FULL", "MOMENTUM", "BULL_CAUTION", "RECOVERY"}
        defensive_states = {"BEAR_HEDGE", "CRASH", "NEUTRAL", "WARMUP"}

        if to_state in bullish_states and from_state in defensive_states:
            action_type = "buy"
        elif to_state in defensive_states and from_state in bullish_states:
            action_type = "sell"
        else:
            action_type = "rotate"

        enriched.append(
            {
                "date": date_str,
                "from_state": from_state,
                "to_state": to_state,
                "portfolio_value": t["portfolio_value"],
                "sma_fast": round(sma_f, 2) if sma_f is not None else None,
                "sma_slow": round(sma_s, 2) if sma_s is not None else None,
                "rsi": round(rsi_val, 1) if rsi_val is not None else None,
                "spy_close": round(close_val, 2) if close_val is not None else None,
                "reasons": reasons,
                "allocation": STATE_ALLOCATIONS.get(to_state, "Unknown"),
                "prev_allocation": STATE_ALLOCATIONS.get(from_state, "Unknown"),
                "duration_in_prev_days": duration,
                "action_type": action_type,
            }
        )

    return enriched


def build_sma_crossover_points(indicators: pd.DataFrame) -> list[dict]:
    """Find all SMA crossover points for highlighting on chart."""
    sma_f = indicators["sma_fast"].values
    sma_s = indicators["sma_slow"].values
    dates = indicators.index
    crossovers = []

    for i in range(1, len(sma_f)):
        if (
            np.isnan(sma_f[i])
            or np.isnan(sma_s[i])
            or np.isnan(sma_f[i - 1])
            or np.isnan(sma_s[i - 1])
        ):
            continue

        prev_diff = sma_f[i - 1] - sma_s[i - 1]
        curr_diff = sma_f[i] - sma_s[i]

        if prev_diff <= 0 < curr_diff:
            crossovers.append(
                {
                    "date": dates[i].strftime("%Y-%m-%d"),
                    "price": round((sma_f[i] + sma_s[i]) / 2, 2),
                    "type": "golden_cross",
                    "label": "Golden Cross",
                }
            )
        elif prev_diff >= 0 > curr_diff:
            crossovers.append(
                {
                    "date": dates[i].strftime("%Y-%m-%d"),
                    "price": round((sma_f[i] + sma_s[i]) / 2, 2),
                    "type": "death_cross",
                    "label": "Death Cross",
                }
            )

    return crossovers


def main() -> None:
    print("=" * 60)
    print("GENERATING CHART DATA FOR RAVI VAM VERIFIER")
    print("=" * 60)

    # Load data
    print("\nLoading data...")
    dfs = load_data()
    common_dates = align_dates(dfs)
    print(
        f"  Common dates: {common_dates[0].date()} to {common_dates[-1].date()} ({len(common_dates)} bars)"
    )

    # Compute indicators
    print("Computing indicators...")
    spy_close = dfs["SPY"].loc[common_dates, "close"]
    indicators = compute_indicators(spy_close)

    # Classify regimes
    print("Classifying regimes...")
    regimes = classify_regimes(indicators.loc[common_dates])

    # Compute returns
    print("Computing returns...")
    returns = {}
    for sym in ["UPRO", "TQQQ", "SHY"]:
        returns[sym] = dfs[sym].loc[common_dates, "close"].pct_change()

    # Run backtest
    print("Running backtest...")
    equity_values, transitions, summary = run_backtest_with_equity(regimes, returns)

    # Enrich transitions
    print("Enriching transitions...")
    enriched_transitions = generate_transition_reasons(
        transitions, indicators.loc[common_dates], regimes
    )

    # SMA crossovers
    print("Finding SMA crossovers...")
    crossovers = build_sma_crossover_points(indicators.loc[common_dates])

    # Build state timeline (compressed: only store changes)
    print("Building state timeline...")
    state_timeline = []
    prev_state = None
    for i, (date, state) in enumerate(regimes.items()):
        if state != prev_state:
            state_timeline.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "state": state,
                }
            )
            prev_state = state

    # Build OHLC data for candlestick chart
    print("Building OHLC data...")
    spy_df = dfs["SPY"].loc[common_dates]
    ohlc_data = []
    for date, row in spy_df.iterrows():
        ohlc_data.append(
            {
                "time": date.strftime("%Y-%m-%d"),
                "open": round(row["open"], 2),
                "high": round(row["high"], 2),
                "low": round(row["low"], 2),
                "close": round(row["close"], 2),
            }
        )

    # Build SMA series
    sma_fast_data = []
    sma_slow_data = []
    ind = indicators.loc[common_dates]
    for date in common_dates:
        if not np.isnan(ind.loc[date, "sma_fast"]):
            sma_fast_data.append(
                {
                    "time": date.strftime("%Y-%m-%d"),
                    "value": round(ind.loc[date, "sma_fast"], 2),
                }
            )
        if not np.isnan(ind.loc[date, "sma_slow"]):
            sma_slow_data.append(
                {
                    "time": date.strftime("%Y-%m-%d"),
                    "value": round(ind.loc[date, "sma_slow"], 2),
                }
            )

    # Build RSI series
    rsi_data = []
    for date in common_dates:
        if not np.isnan(ind.loc[date, "rsi_14"]):
            rsi_data.append(
                {
                    "time": date.strftime("%Y-%m-%d"),
                    "value": round(ind.loc[date, "rsi_14"], 2),
                }
            )

    # Build equity series
    equity_data = []
    for i, date in enumerate(common_dates):
        equity_data.append(
            {
                "time": date.strftime("%Y-%m-%d"),
                "value": round(equity_values[i], 2),
            }
        )

    # Build regime series (state for each date)
    regime_data = []
    for date in common_dates:
        regime_data.append(
            {
                "time": date.strftime("%Y-%m-%d"),
                "state": regimes[date],
            }
        )

    # Assemble output
    output = {
        "metadata": {
            "strategy": "Ravi VAM 7-State Regime Rotation",
            "variant": f"SMA({SMA_FAST},{SMA_SLOW}) RSI_entry={RSI_ENTRY} "
            f"UPRO={UPRO_PCT}%/TQQQ={TQQQ_PCT}% Def={DEFENSIVE} "
            f"BC={BULL_CAUTION_ENABLED} Conf={CONFIRM_DAYS}d",
            "data_source": "DataBento XNAS.ITCH",
            "data_range": f"{common_dates[0].date()} to {common_dates[-1].date()}",
            "total_bars": len(common_dates),
            "initial_capital": INITIAL_CAPITAL,
            "generated": datetime.now().isoformat(),
        },
        "parameters": {
            "sma_fast": SMA_FAST,
            "sma_slow": SMA_SLOW,
            "rsi_entry": RSI_ENTRY,
            "rsi_caution_lo": RSI_CAUTION_LO,
            "upro_pct": UPRO_PCT,
            "tqqq_pct": TQQQ_PCT,
            "defensive": DEFENSIVE,
            "bull_caution": BULL_CAUTION_ENABLED,
            "confirm_days": CONFIRM_DAYS,
        },
        "state_colors": STATE_COLORS,
        "state_allocations": STATE_ALLOCATIONS,
        "summary": summary,
        "ohlc": ohlc_data,
        "sma_fast": sma_fast_data,
        "sma_slow": sma_slow_data,
        "rsi": rsi_data,
        "equity": equity_data,
        "regimes": regime_data,
        "transitions": enriched_transitions,
        "crossovers": crossovers,
        "state_timeline": state_timeline,
    }

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "ravi_vam_chart_data.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=None, separators=(",", ":"))

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\nOutput: {output_path}")
    print(f"Size: {file_size_mb:.2f} MB")
    print(f"OHLC bars: {len(ohlc_data)}")
    print(f"Transitions: {len(enriched_transitions)}")
    print(f"Crossovers: {len(crossovers)}")
    print(f"State changes: {len(state_timeline)}")
    print(f"\nSummary: {json.dumps(summary, indent=2)}")
    print("\nDone.")


if __name__ == "__main__":
    main()
