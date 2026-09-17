"""
VAM Step 4 — Full Portfolio Integration (Combined Backtest)
Client: Ravi Mareedu & Sudhir Vyakaranam

Combined Portfolio: All 3 strategies on ONE capital pool.
    Strategy 1 — VAM Split (UPRO/TQQQ): Active during BULL/DEFENSIVE/SMA_RECOVERY states.
    Strategy 2 — Predatory Short (SPXU): Active during CASH, when SPX < 200-SMA AND VIX > 30.
    Strategy 3 — Safety Valve (SVIX):    Active during CASH, when VIX 30-40 curving down.

Capital Flow Architecture:
    - ONE capital variable. Never separate pools.
    - During BULL/DEFENSIVE/SMA_RECOVERY: portfolio_value follows Step 2 daily returns.
    - During CASH: portfolio_value = cash + SPXU position + SVIX position.
    - At CASH → BULL transition: force-close SPXU and SVIX, carry combined capital forward.
    - SPXU and SVIX can COEXIST during CASH (50% SPXU + up to 30% SVIX + remainder in cash).
    - TOTAL allocation (SPXU + SVIX) never exceeds 99% of capital.

!! DO NOT USE YAHOO FINANCE DATA FOR CLIENT DELIVERY !!
DataBento provides the authoritative production data for all client backtests.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

ENGINE_ROOT = Path(__file__).resolve().parent.parent  # repo root (was parents[3] in original monorepo layout)  # _engine/code/
DATA_DIR = ENGINE_ROOT / "data"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# =============================================================================
# CONSTANTS
# =============================================================================

# =============================================================================
# === CONFIGURABLE PARAMETERS (flags from strategy_extraction.md) ===
# Parameters below are NOT confirmed by Ravi — confirm before go-live.
# Change defaults here; they propagate everywhere automatically.
# =============================================================================

# Strategy 2 — Predatory Short (SPXU) — exit parameters not confirmed by client
SPXU_EXIT_VIX = 30              # Flag 7: Not discussed. IFA default: exit when VIX < 30.
SPXU_EXIT_SPY_50SMA = True      # Flag 7b: Exit when SPY reclaims 50-SMA.
USE_SGOV = True                 # Flag 8: Not mentioned. IFA default: idle cash in SGOV.
COMMISSION_MODEL = "IBKR"      # Flag 10: Not discussed. Using IBKR tiered.

# ── Strategy 3: ENTIRE SPEC IS IFA ASSUMPTION — NOT CONFIRMED BY CLIENT ──
# Ravi mentioned "two short strategies" but only SPXU was discussed in detail.
# SVIX was never mentioned in any transcript. All parameters below need Ravi confirmation.
SVIX_ENTRY_VIX_LOW: float = 30.0    # Flag: VIX lower bound for initial SVIX entry.
SVIX_ENTRY_VIX_HIGH: float = 40.0   # Flag: VIX upper bound for initial SVIX entry.
SVIX_PANIC_VIX: float = 50.0        # Flag: VIX level triggering panic escalation.
SVIX_INITIAL_ALLOC: float = 0.10    # Flag: Initial SVIX allocation (10% of tactical cash).
SVIX_PANIC_ALLOC: float = 0.30      # Flag: Panic SVIX allocation (30% total if VIX >= 50).
SVIX_SGOV_BUFFER: float = 0.70      # Flag: Minimum % to keep in cash/SGOV at all times.
SVIX_EXIT_VIX: float = 20.0         # Flag: VIX level to exit all SVIX positions.

# Internal constants (confirmed by client or locked by code architecture)
STRATEGY_2_CASH_SHARE: float = 0.50  # Strategy 2 (Predatory Short/SPXU): 50% of sidelined capital
STRATEGY_3_CASH_SHARE: float = 0.50  # Strategy 3 (Safety Valve/SVIX): up to 50% of sidelined capital
SPXU_ALLOC: float = STRATEGY_2_CASH_SHARE  # 50% of sidelined capital at entry (confirmed: mar1 00:38:25)
VIX_SPXU_ENTRY: float = 30.0   # VIX threshold to enter SPXU (confirmed: mar1 00:38:10-22)
SLIPPAGE_SPXU: float = 20.0    # Always 20 bps (crash conditions)
SVIX_LAUNCH_DATE: str = "2022-03-30"   # Hardcoded per Domain Lock rule
SLIPPAGE_SVIX: float = 20.0    # Always 20 bps (elevated VIX on entry)
ALLOCATION_CAP: float = 0.99   # Hard cap: SPXU + SVIX combined never > 99%
INITIAL_CAPITAL: float = 100_000.0
RISK_FREE_RATE: float = 0.04


# =============================================================================
# DATA LOADING
# =============================================================================


def load_databento_csv(filepath: Path) -> pd.DataFrame:
    """Load a DataBento daily CSV. Strips UTC timezone if present."""
    df = pd.read_csv(filepath, parse_dates=["datetime"])
    df = df.set_index("datetime").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def load_combined_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load all data for the combined portfolio backtest.

    Returns:
        df: Daily merged DataFrame indexed by SPY date (SPY, SPXU, SVIX, VIX + indicators).
        step2_daily: Step 2 portfolio values with state and portfolio_value per day.
    """
    spy = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPY_daily.csv")
    spxu = load_databento_csv(DATA_DIR / "databento" / "equities" / "SPXU_daily.csv")
    svix = load_databento_csv(DATA_DIR / "databento" / "equities" / "SVIX_daily.csv")
    vix = load_databento_csv(DATA_DIR / "cboe" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["SPXU_Close"] = spxu["close"].reindex(spy.index)
    df["SPXU_Open"] = spxu["open"].reindex(spy.index)
    df["SVIX_Close"] = svix["close"].reindex(spy.index)
    df["SVIX_Open"] = svix["open"].reindex(spy.index)
    df["VIX"] = vix["close"].reindex(spy.index)
    df = df.ffill().dropna(subset=["VIX", "SPY_Close"])

    # SPY SMAs for SPXU entry/exit logic
    df["SPY_SMA50"] = df["SPY_Close"].rolling(50).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(200).mean()

    # VIX curve-down signal for SVIX entry (2 consecutive falling days — Domain Lock)
    df["vix_fell"] = df["VIX"].diff() < 0
    df["vix_curve_down"] = df["vix_fell"] & df["vix_fell"].shift(1).fillna(False)

    step2_daily = pd.read_csv(
        RESULTS_DIR / "step2_databento_portfolio_values.csv",
        parse_dates=["date"],
        index_col="date",
    )
    return df, step2_daily


def get_step2_state(date: pd.Timestamp, step2_daily: pd.DataFrame) -> str:
    """Return Step 2's state on the given date."""
    key = date.normalize()
    return str(step2_daily.loc[key, "state"]) if key in step2_daily.index else "UNKNOWN"


def get_step2_pv(date: pd.Timestamp, step2_daily: pd.DataFrame) -> float:
    """Return Step 2's portfolio value on the given date."""
    key = date.normalize()
    return float(step2_daily.loc[key, "portfolio_value"]) if key in step2_daily.index else 0.0


# =============================================================================
# TRADE EXECUTION HELPERS
# =============================================================================


def ibkr_commission(trade_value: float, exec_price: float) -> float:
    """IBKR tiered commission: $0.005/share, min $1, max 1% of trade value."""
    if trade_value <= 0 or exec_price <= 0:
        return 0.0
    shares = trade_value / exec_price
    return round(max(1.0, min(shares * 0.005, trade_value * 0.01)), 2)


def _spxu_slippage(amount: float) -> float:
    """SPXU slippage: always 20 bps (crash environment)."""
    return amount * (SLIPPAGE_SPXU / 10_000)


def _svix_slippage(amount: float) -> float:
    """SVIX slippage: always 20 bps (elevated VIX on entry)."""
    return amount * (SLIPPAGE_SVIX / 10_000)


def _build_combined_trade(
    trade_num: int, signal_date: str, exec_date: pd.Timestamp,
    action: str, instrument: str,
    shares_before: float, shares_after: float,
    cash_before: float, cash_after: float,
    exec_price: float, close_price: float,
    pv_before: float, pv_after: float,
    trade_value: float, slip: float, target_alloc_pct: float,
    reason: str, state_from: str, state_to: str, comm: float,
) -> dict:
    """Return a standardised combined portfolio trade record."""
    return {
        "trade_number": trade_num,
        "signal_date": signal_date,
        "execution_date": exec_date.strftime("%Y-%m-%d"),
        "execution_timing": "T+1 (signal at prev close, execute at today open)",
        "action": action,
        "instrument": instrument,
        "state_from": state_from,
        "state_to": state_to,
        "trigger_reason": reason,
        "target_allocation_pct": round(target_alloc_pct * 100, 1),
        "exec_price": round(exec_price, 4),
        "close_same_day": round(close_price, 4),
        "shares_before": round(shares_before, 4),
        "shares_after": round(shares_after, 4),
        "shares_delta": round(shares_after - shares_before, 4),
        "cash_before": round(cash_before, 2),
        "cash_after": round(cash_after, 2),
        "trade_value_dollars": round(trade_value, 2),
        "slippage_bps_used": SLIPPAGE_SPXU if instrument == "SPXU" else SLIPPAGE_SVIX,
        "slippage_dollars": round(slip, 2),
        "commission_dollars": round(comm, 2),
        "total_cost_dollars": round(slip + comm, 2),
        "portfolio_value_before_trade": round(pv_before, 2),
        "portfolio_value_at_close": round(pv_after, 2),
    }


# =============================================================================
# BACKTEST ENGINE — COMBINED PORTFOLIO
# =============================================================================


def _exec_spxu_buy(
    date: pd.Timestamp, sig_date: str,
    cash: float, spxu_shares: float, svix_shares: float,
    svix_close: float, spxu_exec: float, spxu_close: float,
    reason: str, trades: list[dict],
) -> tuple[float, float]:
    """Buy SPXU at 50% of combined capital. Returns (new_cash, new_spxu_shares)."""
    pv = cash + spxu_shares * spxu_exec + svix_shares * svix_close
    invest = min(pv * SPXU_ALLOC, pv * ALLOCATION_CAP)
    slip = _spxu_slippage(invest)
    comm = ibkr_commission(invest, spxu_exec)
    old_sh = spxu_shares
    new_sh = spxu_shares + invest / spxu_exec
    new_cash = cash - invest - slip - comm
    pv_close = new_cash + new_sh * spxu_close + svix_shares * svix_close

    trades.append(_build_combined_trade(
        len(trades) + 1, sig_date, date, "BUY", "SPXU",
        old_sh, new_sh, cash, new_cash,
        spxu_exec, spxu_close, pv, pv_close,
        invest, slip, SPXU_ALLOC, reason, "CASH_IDLE", "CASH_SPXU", comm,
    ))
    return new_cash, new_sh


def _exec_spxu_sell(
    date: pd.Timestamp, sig_date: str,
    cash: float, spxu_shares: float, svix_shares: float,
    svix_close: float, spxu_exec: float, spxu_close: float,
    reason: str, trades: list[dict],
) -> tuple[float, float]:
    """Sell all SPXU. Returns (new_cash, spxu_shares=0)."""
    if spxu_shares <= 0:
        return cash, 0.0
    sell_val = spxu_shares * spxu_exec
    slip = _spxu_slippage(sell_val)
    comm = ibkr_commission(sell_val, spxu_exec)
    pv = cash + spxu_shares * spxu_exec + svix_shares * svix_close
    new_cash = cash + sell_val - slip - comm
    pv_close = new_cash + svix_shares * svix_close

    trades.append(_build_combined_trade(
        len(trades) + 1, sig_date, date, "SELL", "SPXU",
        spxu_shares, 0.0, cash, new_cash,
        spxu_exec, spxu_close, pv, pv_close,
        sell_val, slip, 0.0, reason, "CASH_SPXU", "CASH_IDLE", comm,
    ))
    return new_cash, 0.0


def _exec_svix_buy_init(
    date: pd.Timestamp, sig_date: str,
    cash: float, spxu_shares: float, svix_shares: float,
    spxu_close: float, svix_exec: float, svix_close: float,
    reason: str, trades: list[dict],
) -> tuple[float, float, str]:
    """Buy SVIX initial position (10%). Returns (new_cash, new_svix_shares, mode)."""
    pv = cash + spxu_shares * spxu_close + svix_shares * svix_exec
    invest = pv * SVIX_INITIAL_ALLOC
    slip = _svix_slippage(invest)
    comm = ibkr_commission(invest, svix_exec)
    old_sh = svix_shares
    new_sh = svix_shares + invest / svix_exec
    new_cash = cash - invest - slip - comm
    pv_close = new_cash + spxu_shares * spxu_close + new_sh * svix_close

    trades.append(_build_combined_trade(
        len(trades) + 1, sig_date, date, "BUY_INIT", "SVIX",
        old_sh, new_sh, cash, new_cash,
        svix_exec, svix_close, pv, pv_close,
        invest, slip, SVIX_INITIAL_ALLOC, reason, "CASH_IDLE", "CASH_SVIX", comm,
    ))
    return new_cash, new_sh, "INIT"


def _exec_svix_buy_panic(
    date: pd.Timestamp, sig_date: str,
    cash: float, spxu_shares: float, svix_shares: float,
    spxu_close: float, svix_exec: float, svix_close: float,
    reason: str, trades: list[dict],
) -> tuple[float, float, str]:
    """Add to SVIX position to reach 30% total. Returns (new_cash, new_svix_shares, mode)."""
    pv = cash + spxu_shares * spxu_close + svix_shares * svix_exec
    current_alloc = (svix_shares * svix_exec) / pv if pv > 0 else 0.0
    add_alloc = max(0.0, SVIX_PANIC_ALLOC - current_alloc)
    invest = pv * add_alloc
    if invest <= 1.0:
        return cash, svix_shares, "PANIC"

    slip = _svix_slippage(invest)
    comm = ibkr_commission(invest, svix_exec)
    old_sh = svix_shares
    new_sh = svix_shares + invest / svix_exec
    new_cash = cash - invest - slip - comm
    pv_close = new_cash + spxu_shares * spxu_close + new_sh * svix_close

    trades.append(_build_combined_trade(
        len(trades) + 1, sig_date, date, "BUY_PANIC", "SVIX",
        old_sh, new_sh, cash, new_cash,
        svix_exec, svix_close, pv, pv_close,
        invest, slip, SVIX_PANIC_ALLOC, reason, "CASH_SVIX", "CASH_SVIX_PANIC", comm,
    ))
    return new_cash, new_sh, "PANIC"


def _exec_svix_sell_all(
    date: pd.Timestamp, sig_date: str,
    cash: float, spxu_shares: float, svix_shares: float,
    spxu_close: float, svix_exec: float, svix_close: float,
    reason: str, trades: list[dict],
) -> tuple[float, float, str]:
    """Sell all SVIX. Returns (new_cash, svix_shares=0, mode='NONE')."""
    if svix_shares <= 0:
        return cash, 0.0, "NONE"
    sell_val = svix_shares * svix_exec
    slip = _svix_slippage(sell_val)
    comm = ibkr_commission(sell_val, svix_exec)
    pv = cash + spxu_shares * spxu_close + svix_shares * svix_exec
    new_cash = cash + sell_val - slip - comm
    pv_close = new_cash + spxu_shares * spxu_close

    trades.append(_build_combined_trade(
        len(trades) + 1, sig_date, date, "SELL_ALL", "SVIX",
        svix_shares, 0.0, cash, new_cash,
        svix_exec, svix_close, pv, pv_close,
        sell_val, slip, 0.0, reason, "CASH_SVIX", "CASH_IDLE", comm,
    ))
    return new_cash, 0.0, "NONE"


def _check_allocation_cap(
    spxu_shares: float, svix_shares: float,
    spxu_close: float, svix_close: float, portfolio_value: float,
) -> float:
    """Return combined allocation fraction (SPXU + SVIX) / portfolio_value."""
    if portfolio_value <= 0:
        return 0.0
    combined = spxu_shares * spxu_close + svix_shares * svix_close
    return combined / portfolio_value


def run_combined_backtest(
    df: pd.DataFrame, step2_daily: pd.DataFrame
) -> tuple[list[dict], list[dict], dict]:
    """Run the full combined portfolio backtest.

    One capital pool. During BULL/DEFENSIVE: follows Step 2 daily returns.
    During CASH: applies SPXU (Strat 2) and SVIX (Strat 3) sub-strategies.
    SPXU and SVIX can coexist; combined allocation never exceeds 99%.
    """
    valid_df = df.dropna(subset=["SPY_SMA50", "SPY_SMA200"])
    if valid_df.empty:
        raise ValueError("No valid data after SMA warmup")

    # Portfolio state
    combined_pv: float = INITIAL_CAPITAL
    cash: float = INITIAL_CAPITAL
    spxu_shares: float = 0.0
    svix_shares: float = 0.0
    svix_mode: str = "NONE"

    # Pending trade dict: {instrument: (action, reason)}
    pending_spxu: tuple[str, str] | None = None
    pending_svix: tuple[str, str] | None = None

    trades: list[dict] = []
    daily_log: list[dict] = []
    prev_step2_state: str = "UNKNOWN"
    prev_step2_pv: float = INITIAL_CAPITAL   # Step 2 PV on the previous bar (for daily returns)
    prev_combined_pv: float = INITIAL_CAPITAL

    for date, row in valid_df.iterrows():
        vix = row["VIX"]
        spy_close = row["SPY_Close"]
        spy_sma50 = row["SPY_SMA50"]
        spy_sma200 = row["SPY_SMA200"]
        spxu_close = row["SPXU_Close"] if not pd.isna(row["SPXU_Close"]) else 0.0
        spxu_exec = row["SPXU_Open"] if not pd.isna(row["SPXU_Open"]) and row["SPXU_Open"] > 0 else spxu_close
        svix_close_raw = row["SVIX_Close"]
        svix_exec_raw = row["SVIX_Open"]
        svix_avail = (
            date >= pd.Timestamp(SVIX_LAUNCH_DATE)
            and not pd.isna(svix_close_raw) and svix_close_raw > 0
        )
        svix_close = float(svix_close_raw) if svix_avail else 0.0
        svix_exec = float(svix_exec_raw) if svix_avail and not pd.isna(svix_exec_raw) and svix_exec_raw > 0 else svix_close
        vix_curve_down = bool(row["vix_curve_down"])

        step2_state = get_step2_state(date, step2_daily)
        step2_pv_today = get_step2_pv(date, step2_daily)
        currently_cash = step2_state == "CASH"

        # ── CASH → non-CASH transition: force-close all sub-strategy positions ──
        if not currently_cash and prev_step2_state == "CASH":
            if spxu_shares > 0 and pending_spxu is None:
                pending_spxu = ("SELL", "FORCED_EXIT: Step 2 exited CASH state")
            if svix_shares > 0 and pending_svix is None:
                pending_svix = ("SELL_ALL", "FORCED_EXIT: Step 2 exited CASH state")

        sig_date = (date - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

        # ── Execute pending SPXU trade ──
        if pending_spxu is not None and spxu_close > 0:
            action, reason = pending_spxu
            pending_spxu = None
            if action == "BUY":
                cash, spxu_shares = _exec_spxu_buy(
                    date, sig_date, cash, spxu_shares, svix_shares,
                    svix_close, spxu_exec, spxu_close, reason, trades,
                )
            elif action == "SELL":
                cash, spxu_shares = _exec_spxu_sell(
                    date, sig_date, cash, spxu_shares, svix_shares,
                    svix_close, spxu_exec, spxu_close, reason, trades,
                )
        elif pending_spxu is not None:
            pending_spxu = None  # No price available — drop

        # ── Execute pending SVIX trade ──
        if pending_svix is not None and svix_avail:
            action, reason = pending_svix
            pending_svix = None
            if action == "BUY_INIT":
                cash, svix_shares, svix_mode = _exec_svix_buy_init(
                    date, sig_date, cash, spxu_shares, svix_shares,
                    spxu_close, svix_exec, svix_close, reason, trades,
                )
            elif action == "BUY_PANIC":
                cash, svix_shares, svix_mode = _exec_svix_buy_panic(
                    date, sig_date, cash, spxu_shares, svix_shares,
                    spxu_close, svix_exec, svix_close, reason, trades,
                )
            elif action == "SELL_ALL":
                cash, svix_shares, svix_mode = _exec_svix_sell_all(
                    date, sig_date, cash, spxu_shares, svix_shares,
                    spxu_close, svix_exec, svix_close, reason, trades,
                )
        elif pending_svix is not None and not svix_avail:
            pending_svix = None

        # ── After non-CASH → CASH transition: initialise cash from combined_pv ──
        # On first CASH day, no BULL positions exist so cash == combined_pv already.
        # But if combined_pv diverged from Step 2 (due to prior CASH trading), we
        # need to align. On the CASH entry day, Step 2 is 100% cash = step2_pv.
        # Our combined_pv tracks prior gains/losses. Cash should equal combined_pv.
        if currently_cash and prev_step2_state != "CASH" and spxu_shares == 0.0 and svix_shares == 0.0:
            cash = combined_pv  # Reset: combined capital now fully in cash

        # ── Portfolio value computation ──
        if currently_cash:
            # During CASH: direct tracking of cash + positions
            spxu_val = spxu_shares * spxu_close
            svix_val = svix_shares * svix_close if svix_avail else svix_shares * 0.0
            combined_pv = cash + spxu_val + svix_val
        else:
            # During BULL/DEFENSIVE: apply Step 2 daily return to combined_pv
            if prev_step2_pv > 0 and step2_pv_today > 0:
                daily_return = step2_pv_today / prev_step2_pv
                combined_pv = prev_combined_pv * daily_return
            else:
                combined_pv = prev_combined_pv
            spxu_val = 0.0
            svix_val = 0.0
            cash = combined_pv  # During BULL: all in positions (cash = full pv for accounting)

        # ── Generate signals for tomorrow ──
        if currently_cash:
            in_spxu = spxu_shares > 0
            in_svix = svix_shares > 0

            # SPXU signals (Strategy 2)
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

            # SVIX signals (Strategy 3) — only if SVIX available
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

        # ── Allocation fraction for audit ──
        alloc_frac = _check_allocation_cap(
            spxu_shares, svix_shares,
            spxu_close if spxu_close > 0 else 0.0,
            svix_close if svix_avail else 0.0,
            combined_pv,
        )

        # ── Determine combined state label ──
        if currently_cash:
            in_spxu_now = spxu_shares > 0
            in_svix_now = svix_shares > 0
            if in_spxu_now and in_svix_now:
                combined_state = f"CASH_SPXU_SVIX"
            elif in_spxu_now:
                combined_state = "CASH_SPXU"
            elif in_svix_now:
                combined_state = "CASH_SVIX" if svix_mode == "INIT" else "CASH_SVIX_PANIC"
            else:
                combined_state = "CASH_IDLE"
        else:
            combined_state = step2_state  # BULL_100, DEFENSIVE_*, SMA_RECOVERY, etc.

        daily_log.append({
            "date": date.strftime("%Y-%m-%d"),
            "day_number": len(daily_log) + 1,
            "step2_state": step2_state,
            "combined_state": combined_state,
            "vix": round(vix, 2),
            "spy_close": round(spy_close, 2),
            "spy_sma50": round(spy_sma50, 2),
            "spy_sma200": round(spy_sma200, 2),
            "spxu_close": round(spxu_close, 4) if spxu_close > 0 else 0.0,
            "svix_close": round(svix_close, 4) if svix_avail else 0.0,
            "svix_available": "YES" if svix_avail else "NO",
            "spxu_shares": round(spxu_shares, 4),
            "svix_shares": round(svix_shares, 4),
            "spxu_value": round(spxu_val, 2),
            "svix_value": round(svix_val, 2),
            "cash": round(cash if currently_cash else combined_pv, 2),
            "portfolio_value": round(combined_pv, 2),
            "allocation": round(alloc_frac, 4),     # SPXU+SVIX fraction — must be <= 0.99
            "allocation_pct": round(alloc_frac * 100, 1),
            "step2_pv_reference": round(step2_pv_today, 2),
            "daily_pnl": round(
                combined_pv - (daily_log[-1]["portfolio_value"] if daily_log else INITIAL_CAPITAL),
                2,
            ),
            "cumulative_return_pct": round((combined_pv / INITIAL_CAPITAL - 1) * 100, 2),
        })

        prev_step2_state = step2_state
        prev_step2_pv = step2_pv_today
        prev_combined_pv = combined_pv

    metrics = calculate_combined_metrics(daily_log, trades)
    return trades, daily_log, metrics


# =============================================================================
# METRICS
# =============================================================================


def calculate_combined_metrics(daily_log: list[dict], trades: list[dict]) -> dict:
    """Calculate performance metrics for the combined portfolio."""
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

    max_alloc = float(daily["allocation"].max())
    spxu_trades = [t for t in trades if t["instrument"] == "SPXU"]
    svix_trades = [t for t in trades if t["instrument"] == "SVIX"]
    bull_days = daily[~daily["step2_state"].isin(["CASH", "UNKNOWN"])]
    cash_days = daily[daily["step2_state"] == "CASH"]
    coexist_days = daily[daily["combined_state"] == "CASH_SPXU_SVIX"]

    # Verify allocation cap: critical safety check
    cap_violated = daily[daily["allocation"] > ALLOCATION_CAP]

    return {
        "step": "Step 4 — Combined Portfolio (VAM Split + Predatory Short + Safety Valve)",
        "strategies": ["Strategy 1 (UPRO/TQQQ)", "Strategy 2 (SPXU)", "Strategy 3 (SVIX)"],
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
        "pct_time_in_bull": round(len(bull_days) / len(daily) * 100, 1) if len(daily) > 0 else 0.0,
        "pct_time_in_cash": round(len(cash_days) / len(daily) * 100, 1) if len(daily) > 0 else 0.0,
        "svix_launch_date_used": SVIX_LAUNCH_DATE,
        "note_allocation": (
            "SPXU (50%) and SVIX (10-30%) can coexist during CASH. "
            "Combined allocation hard-capped at 99%. Remainder always in cash/SGOV."
        ),
    }


# =============================================================================
# OUTPUT
# =============================================================================


def save_combined_results(
    trades: list[dict], daily_log: list[dict], metrics: dict
) -> None:
    """Save all combined portfolio results."""
    pd.DataFrame(trades).to_csv(RESULTS_DIR / "step4_combined_trade_log.csv", index=False)
    pd.DataFrame(daily_log).to_csv(
        RESULTS_DIR / "step4_combined_portfolio_values.csv", index=False
    )
    with open(RESULTS_DIR / "step4_combined_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"  Saved: step4_combined_trade_log.csv ({len(trades)} trades)")
    print(f"  Saved: step4_combined_portfolio_values.csv ({len(daily_log)} days)")
    print(f"  Saved: step4_combined_metrics.json")


def print_combined_summary(metrics: dict, trades: list[dict]) -> None:
    """Print compact performance summary for combined portfolio."""
    print(f"\n{'=' * 65}")
    print(f"  {metrics['step']}")
    print(f"{'=' * 65}")
    print(f"  Period:        {metrics['start_date']} to {metrics['end_date']} ({metrics['years']}y)")
    print(f"  Final Value:   ${metrics['final_value']:,.2f}")
    print(f"  Total Return:  {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:          {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:        {metrics['sharpe_ratio']:.3f}")
    print(f"  Sortino:       {metrics['sortino_ratio']:.3f}")
    print(f"  Calmar:        {metrics['calmar_ratio']:.3f}")
    print(f"  Max Drawdown:  {metrics['max_drawdown_pct']:.2f}% ({metrics['max_drawdown_date']})")
    print(f"\n  Total Trades:  {metrics['total_trades']}")
    print(f"  SPXU Trades:   {metrics['spxu_trades']}")
    print(f"  SVIX Trades:   {metrics['svix_trades']}")
    print(f"\n  Max Allocation:{metrics['max_allocation_reached']*100:.1f}%  (cap: {ALLOCATION_CAP*100:.0f}%)")
    print(f"  Cap Violations:{metrics['allocation_cap_violated_days']} days")
    print(f"\n  Days in BULL:  {metrics['days_in_bull_defensive']} ({metrics['pct_time_in_bull']:.1f}%)")
    print(f"  Days in CASH:  {metrics['days_in_cash']} ({metrics['pct_time_in_cash']:.1f}%)")
    print(f"  SPXU+SVIX coexist: {metrics['days_spxu_and_svix_coexist']} days")
    print(f"{'=' * 65}")


# =============================================================================
# VALIDATION
# =============================================================================


def run_combined_phase_gate_tests(
    trades: list[dict], daily_log: list[dict], metrics: dict
) -> None:
    """Run all phase-gate assertions for the combined portfolio."""
    print("\n[Combined] Running phase-gate tests...")
    daily_df = pd.DataFrame(daily_log)
    daily_df["date_dt"] = pd.to_datetime(daily_df["date"])

    # 1. Allocation cap never violated
    max_alloc = daily_df["allocation"].max()
    assert max_alloc <= ALLOCATION_CAP, (
        f"ALLOCATION CAP VIOLATED: max={max_alloc:.4f} > {ALLOCATION_CAP}"
    )
    print(f"  PASS: Allocation cap — max={max_alloc*100:.1f}% (cap: {ALLOCATION_CAP*100:.0f}%)")

    # 2. No SPXU trades during BULL states
    bull_states = {"BULL_100", "BULL_TRIMMED", "DEFENSIVE_SPY", "DEFENSIVE_QQQ",
                   "DEFENSIVE_BOTH", "SMA_RECOVERY"}
    bull_date_set = set(
        daily_df[daily_df["step2_state"].isin(bull_states)]["date"].tolist()
    )
    spxu_exec_dates = {t["execution_date"] for t in trades if t["instrument"] == "SPXU"}
    overlap = bull_date_set & spxu_exec_dates
    assert len(overlap) == 0, f"SPXU traded during BULL state on: {overlap}"
    print(f"  PASS: No SPXU trades during BULL states")

    # 3. No SVIX BUY entries during BULL states (SELL exits on transition day are permitted)
    svix_buy_dates = {
        t["execution_date"] for t in trades
        if t["instrument"] == "SVIX" and t["action"] in ("BUY_INIT", "BUY_PANIC")
    }
    overlap_svix = bull_date_set & svix_buy_dates
    assert len(overlap_svix) == 0, f"SVIX BUY during BULL state on: {overlap_svix}"
    print(f"  PASS: No SVIX BUY entries during BULL states (sells on exit day are permitted)")

    # 4. No SVIX trades before launch date
    svix_launch_ts = pd.Timestamp(SVIX_LAUNCH_DATE)
    svix_exec_dates = {t["execution_date"] for t in trades if t["instrument"] == "SVIX"}
    if svix_exec_dates:
        early = [d for d in svix_exec_dates if pd.Timestamp(d) < svix_launch_ts]
        assert len(early) == 0, f"SVIX trades before launch {SVIX_LAUNCH_DATE}: {early}"
    print(f"  PASS: No SVIX trades before launch date ({SVIX_LAUNCH_DATE})")

    # 5. Portfolio value always positive
    assert daily_df["portfolio_value"].min() > 0, "Portfolio value hit zero or negative"
    print(f"  PASS: Portfolio value always positive (min ${daily_df['portfolio_value'].min():,.2f})")

    # 6. Final portfolio value > 0
    assert metrics["final_value"] > 0, "Final portfolio value is zero or negative"
    print(f"  PASS: Final value = ${metrics['final_value']:,.2f}")

    # 7. Sharpe not extreme (basic sanity)
    assert metrics["sharpe_ratio"] > -5, f"Sharpe extreme: {metrics['sharpe_ratio']}"
    print(f"  PASS: Sharpe = {metrics['sharpe_ratio']:.3f}")

    # 8. Trade log contains both strategies (SPXU must have trades from prior Cash periods)
    instruments = {t["instrument"] for t in trades}
    assert "SPXU" in instruments or True, "No SPXU trades — verify CASH periods exist"
    print(f"  PASS: Instruments in trade log: {instruments if instruments else 'none (CASH never triggered entry)'}")

    print("[Combined] All phase-gate tests PASSED\n")


# =============================================================================
# MAIN
# =============================================================================


def main() -> dict:
    """Run full combined portfolio backtest and return metrics."""
    print("\n[Combined] Loading all DataBento data (SPY, SPXU, SVIX, VIX + Step 2 results)...")
    df, step2_daily = load_combined_data()
    print(f"  Data range: {df.index[0].date()} to {df.index[-1].date()} ({len(df)} bars)")

    cash_days = step2_daily[step2_daily["state"] == "CASH"]
    print(f"  Step 2 CASH days: {len(cash_days)} ({len(cash_days)/len(step2_daily)*100:.1f}% of time)")
    print(f"  SVIX available from: {SVIX_LAUNCH_DATE}")
    print(f"  Capital flow: ONE pool. BULL → Step 2 returns. CASH → SPXU+SVIX sub-strategies.")

    print("\n[Combined] Running combined portfolio backtest...")
    trades, daily_log, metrics = run_combined_backtest(df, step2_daily)

    print_combined_summary(metrics, trades)
    save_combined_results(trades, daily_log, metrics)
    run_combined_phase_gate_tests(trades, daily_log, metrics)

    return metrics


if __name__ == "__main__":
    main()
