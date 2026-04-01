"""
v3 / v5 / v5b engine wrapper — thin wrapper over scripts/backtest_ravi_v5.py.

That script already accepts a `params` dict in run_backtest(), so the wrapper
is minimal. We just patch the DATA_DIR, merge caller params with defaults,
and serialize the equity curve to the format the frontend expects.
"""

import sys
from pathlib import Path

import pandas as pd

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

import backtest_ravi_v5 as _v5


def _normalize_trades(trade_log: list, alloc: dict, dfs: dict) -> list:
    """
    Convert v5 regime-transition trades into the standard frontend trade format.

    v5 raw:  { date, from_regime, to_regime, turnover, tc_pct }
    Target:  { date, action, instrument, exec_price, state_from, state_to, state_changed_today }

    Rules:
      - Going TO an invested regime (not WARMUP/DEFENSIVE/CASH/CRASH_PROTECT) = BUY
      - Going FROM an invested regime                                          = SELL
      - instrument = primary non-cash instrument in the destination allocation
      - exec_price = closing price of that instrument on that date (T+0 for display)
    """
    CASH_REGIMES = {"WARMUP", "DEFENSIVE", "CASH", "CRASH_PROTECT"}

    # Build a quick lookup: date-string → close price per symbol
    price_lookup: dict[str, dict[str, float]] = {}
    for sym, df in dfs.items():
        for idx_date, row in df.iterrows():
            d = str(idx_date.date()) if hasattr(idx_date, "date") else str(idx_date)[:10]
            if d not in price_lookup:
                price_lookup[d] = {}
            price_lookup[d][sym] = float(row["close"])

    def primary_instrument(regime: str) -> tuple[str, float]:
        """Return (symbol, price) for the dominant invested instrument in regime."""
        weights = alloc.get(regime, {})
        # Filter out cash-like instruments
        invested = {s: w for s, w in weights.items() if s not in ("SHY", "GLD", "TLT", "CASH")}
        if not invested:
            # Fallback to the highest-weight instrument
            invested = weights
        sym = max(invested, key=invested.get) if invested else "SPY"
        return sym

    normalized = []
    for t in trade_log:
        date_str = str(t["date"])[:10]
        to_regime = t.get("to_regime", "")
        from_regime = t.get("from_regime", "")

        action = "BUY" if to_regime not in CASH_REGIMES else "SELL"

        # Use destination regime for BUY, source regime for SELL price reference
        ref_regime = to_regime if action == "BUY" else from_regime
        instrument = primary_instrument(ref_regime)
        exec_price = price_lookup.get(date_str, {}).get(instrument, 0.0)

        normalized.append({
            "date": date_str,
            "action": action,
            "instrument": instrument,
            "exec_price": round(exec_price, 2),
            "state_from": from_regime,
            "state_to": to_regime,
            "state_changed_today": "YES",
            "turnover": t.get("turnover", 0),
            "tc_pct": t.get("tc_pct", 0),
        })

    return normalized


def _build_indicator_lookups(dfs: dict, params: dict) -> dict:
    """Pre-compute SPY SMA-50/200, RSI-14, VIX, and price lookups from raw dfs."""
    import numpy as np

    lookups: dict[str, dict[str, float]] = {
        "spy": {}, "qqq": {}, "vix": {},
        "upro_open": {}, "upro_close": {},
        "tqqq_open": {}, "tqqq_close": {},
        "spy_sma50": {}, "spy_sma200": {}, "spy_rsi_14": {},
    }

    # Price lookups
    for sym, key_open, key_close in [
        ("SPY", "spy", "spy"), ("QQQ", "qqq", "qqq"),
        ("UPRO", "upro_open", "upro_close"), ("TQQQ", "tqqq_open", "tqqq_close"),
    ]:
        if sym not in dfs:
            continue
        for idx_date, row in dfs[sym].iterrows():
            d = str(idx_date.date()) if hasattr(idx_date, "date") else str(idx_date)[:10]
            if sym in ("SPY", "QQQ"):
                lookups[key_open][d] = float(row["close"])
            else:
                lookups[key_open][d] = float(row["open"])
                lookups[key_close][d] = float(row["close"])

    # VIX — stored in dfs as a separate file, or use ^VIX
    vix_path = None
    if "SPY" in dfs:
        # Try loading VIX from cboe directory
        from app.config import find_data_dir
        data_dir = find_data_dir()
        vix_csv = data_dir / "cboe" / "VIX_daily.csv"
        if vix_csv.exists():
            try:
                vix_df = pd.read_csv(vix_csv, index_col=0, parse_dates=True)
                vix_df.columns = [c.lower() for c in vix_df.columns]
                for idx_date, row in vix_df.iterrows():
                    d = str(idx_date.date()) if hasattr(idx_date, "date") else str(idx_date)[:10]
                    lookups["vix"][d] = float(row["close"])
            except Exception:
                pass

    # Compute SMA-50, SMA-200, RSI-14 from SPY close
    if "SPY" in dfs:
        spy_df = dfs["SPY"].copy()
        spy_df["sma50"] = spy_df["close"].rolling(50).mean()
        spy_df["sma200"] = spy_df["close"].rolling(200).mean()

        # RSI-14
        delta = spy_df["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.ewm(span=14, adjust=False).mean()
        avg_loss = loss.ewm(span=14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        spy_df["rsi"] = 100 - (100 / (1 + rs))

        for idx_date, row in spy_df.iterrows():
            d = str(idx_date.date()) if hasattr(idx_date, "date") else str(idx_date)[:10]
            if not np.isnan(row["sma50"]):
                lookups["spy_sma50"][d] = round(float(row["sma50"]), 2)
            if not np.isnan(row["sma200"]):
                lookups["spy_sma200"][d] = round(float(row["sma200"]), 2)
            if not np.isnan(row.get("rsi", float("nan"))):
                lookups["spy_rsi_14"][d] = round(float(row["rsi"]), 2)

    return lookups


def _serialize_result(equity_curve: pd.DataFrame, trade_log: list, metrics: dict,
                      alloc: dict, dfs: dict) -> dict:
    """Convert backtest_ravi_v5 output to the standard API response format."""
    initial_equity = float(equity_curve["equity"].iloc[0])

    # Build all indicator lookups from raw data
    lookups = _build_indicator_lookups(dfs, {})

    daily_log = []
    for date, row in equity_curve.iterrows():
        date_str = str(date.date())
        daily_log.append({
            "date": date_str,
            "state": row.get("regime", "UNKNOWN"),
            "state_changed_today": "NO",
            "portfolio_value": round(float(row["equity"]), 2),
            "spy_close": lookups["spy"].get(date_str, 0.0),
            "qqq_close": lookups["qqq"].get(date_str, 0.0),
            "upro_open": lookups["upro_open"].get(date_str, 0.0),
            "upro_close": lookups["upro_close"].get(date_str, 0.0),
            "tqqq_open": lookups["tqqq_open"].get(date_str, 0.0),
            "tqqq_close": lookups["tqqq_close"].get(date_str, 0.0),
            "vix": lookups["vix"].get(date_str, 0.0),
            "spy_sma50": lookups["spy_sma50"].get(date_str, None),
            "spy_sma200": lookups["spy_sma200"].get(date_str, None),
            "spy_rsi_14": lookups["spy_rsi_14"].get(date_str, None),
            "vix_threshold": 30,
            "upro_allocation_pct": 0.0,
            "tqqq_allocation_pct": 0.0,
            "cash_allocation_pct": 0.0,
            "cumulative_return_pct": round(
                (float(row["equity"]) / initial_equity - 1) * 100, 2
            ),
        })

    # Mark state transitions in daily log
    prev_regime = None
    for entry in daily_log:
        regime = entry["state"]
        if regime != prev_regime and prev_regime is not None:
            entry["state_changed_today"] = "YES"
        prev_regime = regime

    # Normalize metrics keys to match frontend expectations
    normalized_metrics = {
        "cagr_pct": metrics.get("cagr_pct"),
        "sharpe": metrics.get("sharpe_ratio", metrics.get("sharpe")),
        "sortino": metrics.get("sortino_ratio", metrics.get("sortino")),
        "calmar": metrics.get("calmar_ratio", metrics.get("calmar")),
        "max_drawdown_pct": metrics.get("max_drawdown_pct"),
        "total_return_pct": metrics.get("total_return_pct"),
        "final_value": metrics.get("final_equity", metrics.get("final_value")),
        "total_trades": metrics.get("trade_count", metrics.get("total_trades")),
        "win_rate_pct": metrics.get("win_rate_pct"),
        "profit_factor": metrics.get("profit_factor"),
        "annual_volatility_pct": metrics.get("annual_volatility_pct"),
        "yearly_returns": metrics.get("yearly_returns", {}),
        "years": metrics.get("n_years"),
        # Computed from daily log for frontend buy-hold calc
        "alpha_vs_spy_pct": None,
        "benchmark_spy_cagr_pct": None,
    }

    # Normalize trade log so frontend renders correctly
    normalized_trades = _normalize_trades(trade_log, alloc, dfs)

    return {
        "daily_log": daily_log,
        "trades": normalized_trades,
        "metrics": normalized_metrics,
    }


def _prep_data_dir():
    """Ensure data is available and patch module DATA_DIR before any backtest."""
    from app.config import ensure_data_available
    data_dir = ensure_data_available()
    _v5.DATA_DIR = data_dir / "databento" / "equities"


def run_v5(params: dict, initial_capital: float = 100_000.0) -> dict:
    """Run v5 (leveraged: UPRO/TQQQ in AGGRESSIVE) backtest."""
    _prep_data_dir()
    merged = {**_v5.PARAMS, **params}
    dfs = _v5.load_data()
    ec, trade_log, trade_count = _v5.run_backtest(
        dfs, _v5.ALLOC_V5, initial_capital=initial_capital, params=merged
    )
    metrics = _v5.compute_metrics(ec, trade_count, initial_capital)
    return _serialize_result(ec, trade_log, metrics, _v5.ALLOC_V5, dfs)


def run_v5b(params: dict, initial_capital: float = 100_000.0) -> dict:
    """Run v5b (non-leveraged: SPY/QQQ in AGGRESSIVE) backtest."""
    _prep_data_dir()
    merged = {**_v5.PARAMS, **params}
    dfs = _v5.load_data()
    ec, trade_log, trade_count = _v5.run_backtest(
        dfs, _v5.ALLOC_V5B, initial_capital=initial_capital, params=merged
    )
    metrics = _v5.compute_metrics(ec, trade_count, initial_capital)
    return _serialize_result(ec, trade_log, metrics, _v5.ALLOC_V5B, dfs)


def run_v3(params: dict, initial_capital: float = 100_000.0) -> dict:
    """
    Run v3 (7-state leveraged rotation) backtest.
    AGGRESSIVE = UPRO 100%, all other regimes = SHY 100%.
    """
    _prep_data_dir()
    v3_alloc = {
        "WARMUP": {"SHY": 1.0},
        "AGGRESSIVE": {"UPRO": 1.0},
        "DEFENSIVE": {"SHY": 1.0},
        "CRASH_PROTECT": {"SHY": 1.0},
    }
    merged = {**_v5.PARAMS, **params}
    dfs = _v5.load_data()
    ec, trade_log, trade_count = _v5.run_backtest(
        dfs, v3_alloc, initial_capital=initial_capital, params=merged
    )
    metrics = _v5.compute_metrics(ec, trade_count, initial_capital)
    return _serialize_result(ec, trade_log, metrics, v3_alloc, dfs)
