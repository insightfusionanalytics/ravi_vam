"""
VAM Parameter Runner — Client Configuration Interface
Client: Ravi Mareedu & Sudhir Vyakaranam

Reads vam_parameters.xlsx, validates all inputs with clear error messages,
then runs the combined VAM backtest with the provided parameters.
Outputs to clients/ravi_vam/results/run_latest/.

Usage:
  python vam_parameter_runner.py --params vam_parameters.xlsx
  python vam_parameter_runner.py --generate-template
  python vam_parameter_runner.py --test-invalid
"""

import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Add scripts directory to path for module imports
SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

import vam_step2_databento as _s2
import vam_step4_combined_databento as _s4

ENGINE_ROOT = Path(__file__).resolve().parent.parent  # repo root (was parents[3] in original monorepo layout)
DATA_DIR = ENGINE_ROOT / "data"
CLIENT_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = CLIENT_DIR / "results"
RUN_LATEST_DIR = RESULTS_DIR / "run_latest"

# =============================================================================
# PARAMETER SPECIFICATION TABLE
# =============================================================================

PARAM_SPECS: dict[str, dict] = {
    # Strategy 1 — VAM Split (vam_step2_databento.py)
    "SMA_SHORT":          {"min": 10,       "max": 200,        "type": int,   "default": 50},
    "SMA_LONG":           {"min": 50,       "max": 500,        "type": int,   "default": 200},
    "RSI_PERIOD":         {"min": 5,        "max": 30,         "type": int,   "default": 14},
    "RSI_TRIM_LEVEL":     {"min": 60,       "max": 95,         "type": float, "default": 75.0},
    "RSI_REBUY_LEVEL":    {"min": 30,       "max": 80,         "type": float, "default": 60.0},
    "VIX_KILL":           {"min": 20,       "max": 50,         "type": float, "default": 30.0},
    "SMA_CONFIRM_DAYS":   {"min": 1,        "max": 5,          "type": int,   "default": 2},
    "UPRO_WEIGHT":        {"min": 0.50,     "max": 0.95,       "type": float, "default": 0.75},
    "TQQQ_WEIGHT":        {"min": 0.05,     "max": 0.50,       "type": float, "default": 0.25},
    # Strategy 2 — Predatory Short (vam_step4_combined_databento.py)
    "SPXU_DEPLOY_PCT":    {"min": 0.10,     "max": 0.75,       "type": float, "default": 0.50},
    "VIX_SHORT_ENTRY":    {"min": 20,       "max": 50,         "type": float, "default": 30.0},
    "SPXU_EXIT_SMA":      {"min": 20,       "max": 200,        "type": int,   "default": 50},
    # Strategy 3 — Safety Valve (vam_step4_combined_databento.py)
    "SVIX_INIT_PCT":      {"min": 0.05,     "max": 0.30,       "type": float, "default": 0.10},
    "SVIX_PANIC_PCT":     {"min": 0.10,     "max": 0.50,       "type": float, "default": 0.30},
    "SVIX_PANIC_VIX":     {"min": 35,       "max": 70,         "type": float, "default": 50.0},
    "SGOV_BUFFER_PCT":    {"min": 0.50,     "max": 0.90,       "type": float, "default": 0.70},
    "SVIX_EXIT_VIX":      {"min": 10,       "max": 30,         "type": float, "default": 20.0},
    "VIX_CURVE_DAYS":     {"min": 1,        "max": 5,          "type": int,   "default": 2},
    # General
    "INITIAL_CAPITAL":    {"min": 1000,     "max": 10_000_000, "type": float, "default": 100000.0},
    "SLIPPAGE_NORMAL_BPS":{"min": 0,        "max": 50,         "type": float, "default": 5.0},
    "SLIPPAGE_STRESS_BPS":{"min": 5,        "max": 100,        "type": float, "default": 20.0},
    "RISK_FREE_RATE":     {"min": 0.00,     "max": 0.10,       "type": float, "default": 0.04},
}

TAB_PARAMS: dict[str, list[str]] = {
    "Strategy 1": [
        "SMA_SHORT", "SMA_LONG", "RSI_PERIOD", "RSI_TRIM_LEVEL",
        "RSI_REBUY_LEVEL", "VIX_KILL", "SMA_CONFIRM_DAYS",
        "UPRO_WEIGHT", "TQQQ_WEIGHT",
    ],
    "Strategy 2": ["SPXU_DEPLOY_PCT", "VIX_SHORT_ENTRY", "SPXU_EXIT_SMA"],
    "Strategy 3": [
        "SVIX_INIT_PCT", "SVIX_PANIC_PCT", "SVIX_PANIC_VIX",
        "SGOV_BUFFER_PCT", "SVIX_EXIT_VIX", "VIX_CURVE_DAYS",
    ],
    "General": [
        "INITIAL_CAPITAL", "START_DATE", "END_DATE",
        "SLIPPAGE_NORMAL_BPS", "SLIPPAGE_STRESS_BPS", "RISK_FREE_RATE",
    ],
}

PARAM_DESCRIPTIONS: dict[str, str] = {
    "SMA_SHORT": (
        "Short-term SMA period in days. Used to detect if SPY or QQQ is trending "
        "below their short-term average — triggers defensive mode."
    ),
    "SMA_LONG": (
        "Long-term SMA period in days. Used for the kill switch: "
        "if SPY drops below this average, the strategy goes fully to cash."
    ),
    "RSI_PERIOD": (
        "RSI calculation period in days. Measures how overbought or oversold the market is."
    ),
    "RSI_TRIM_LEVEL": (
        "If SPY RSI rises above this level, 25% of the portfolio is sold to cash "
        "(profit-taking trim). Re-enter when RSI drops below RSI_REBUY_LEVEL."
    ),
    "RSI_REBUY_LEVEL": (
        "After an RSI trim, the position is rebuilt when SPY RSI falls back below "
        "this level. Must be lower than RSI_TRIM_LEVEL."
    ),
    "VIX_KILL": (
        "VIX (market fear index) threshold for the kill switch. "
        "When VIX exceeds this level, all positions are sold to cash immediately."
    ),
    "SMA_CONFIRM_DAYS": (
        "Number of consecutive days SPY or QQQ must stay below their short-term SMA "
        "before defensive mode activates. Filters out false signals from one-day dips."
    ),
    "UPRO_WEIGHT": (
        "Target allocation to UPRO (3x S&P 500 ETF) in full bull mode. "
        "UPRO + TQQQ combined must not exceed 0.99 (99% cap)."
    ),
    "TQQQ_WEIGHT": (
        "Target allocation to TQQQ (3x Nasdaq ETF) in full bull mode. "
        "UPRO + TQQQ combined must not exceed 0.99 (99% cap)."
    ),
    "SPXU_DEPLOY_PCT": (
        "Fraction of sidelined cash to deploy into SPXU (inverse S&P 500) "
        "during crash conditions. Example: 0.50 = 50% of cash into SPXU."
    ),
    "VIX_SHORT_ENTRY": (
        "VIX must be ABOVE this level for Strategy 2 (SPXU) to activate. "
        "Also sets the lower bound for Strategy 3 (SVIX) entry window."
    ),
    "SPXU_EXIT_SMA": (
        "SMA period for the SPXU exit signal. When SPY closes above this average, "
        "SPXU position is sold (trend reversing from bear to bull)."
    ),
    "SVIX_INIT_PCT": (
        "Initial SVIX allocation when VIX is in the 30-40 range and falling. "
        "SVIX profits when market fear (VIX) subsides."
    ),
    "SVIX_PANIC_PCT": (
        "Total SVIX allocation if VIX spikes above SVIX_PANIC_VIX. "
        "Adds to existing position to capture larger volatility crush."
    ),
    "SVIX_PANIC_VIX": (
        "If VIX exceeds this level while already in SVIX, add more SVIX "
        "to reach SVIX_PANIC_PCT total. Represents extreme panic conditions."
    ),
    "SGOV_BUFFER_PCT": (
        "Minimum fraction of sidelined cash that must always remain in "
        "SGOV (T-Bill ETF) as a safety buffer. SVIX + SPXU cannot exceed (1 - SGOV_BUFFER_PCT)."
    ),
    "SVIX_EXIT_VIX": (
        "When VIX drops below this level, sell ALL SVIX. "
        "Signals that market panic has fully subsided."
    ),
    "VIX_CURVE_DAYS": (
        "Number of consecutive days VIX must be falling before SVIX entry triggers. "
        "Confirms that market fear is declining, not just a one-day noise."
    ),
    "INITIAL_CAPITAL": (
        "Starting capital in USD for the backtest. "
        "Example: 100000 = $100,000."
    ),
    "START_DATE": (
        "Backtest start date in YYYY-MM-DD format. "
        "Must be before END_DATE."
    ),
    "END_DATE": (
        "Backtest end date in YYYY-MM-DD format. "
        "Must be after START_DATE."
    ),
    "SLIPPAGE_NORMAL_BPS": (
        "Transaction slippage in normal market conditions, in basis points (bps). "
        "1 bps = 0.01%. Default: 5 bps = 0.05% per trade."
    ),
    "SLIPPAGE_STRESS_BPS": (
        "Transaction slippage during high-VIX or kill-switch conditions, in basis points. "
        "Default: 20 bps = 0.20% per trade. Applied to ALL SPXU and SVIX trades."
    ),
    "RISK_FREE_RATE": (
        "Annual risk-free rate used in the Sharpe and Sortino ratio calculations. "
        "Default: 0.04 = 4% (approximate average during 2020-2025)."
    ),
}

PARAM_VALID_RANGES: dict[str, str] = {
    "SMA_SHORT": "10 to 200 (integer)",
    "SMA_LONG": "50 to 500 (integer)",
    "RSI_PERIOD": "5 to 30 (integer)",
    "RSI_TRIM_LEVEL": "60 to 95",
    "RSI_REBUY_LEVEL": "30 to 80",
    "VIX_KILL": "20 to 50",
    "SMA_CONFIRM_DAYS": "1 to 5 (integer)",
    "UPRO_WEIGHT": "0.50 to 0.95",
    "TQQQ_WEIGHT": "0.05 to 0.50",
    "SPXU_DEPLOY_PCT": "0.10 to 0.75",
    "VIX_SHORT_ENTRY": "20 to 50",
    "SPXU_EXIT_SMA": "20 to 200 (integer)",
    "SVIX_INIT_PCT": "0.05 to 0.30",
    "SVIX_PANIC_PCT": "0.10 to 0.50",
    "SVIX_PANIC_VIX": "35 to 70",
    "SGOV_BUFFER_PCT": "0.50 to 0.90",
    "SVIX_EXIT_VIX": "10 to 30",
    "VIX_CURVE_DAYS": "1 to 5 (integer)",
    "INITIAL_CAPITAL": "1000 to 10,000,000",
    "START_DATE": "Any valid date before END_DATE",
    "END_DATE": "Any valid date after START_DATE",
    "SLIPPAGE_NORMAL_BPS": "0 to 50",
    "SLIPPAGE_STRESS_BPS": "5 to 100",
    "RISK_FREE_RATE": "0.00 to 0.10",
}


# =============================================================================
# EXCEL TEMPLATE GENERATOR
# =============================================================================


def generate_template(output_path: Path) -> None:
    """Create vam_parameters.xlsx with 5 tabs, formatted for client use."""
    wb = openpyxl.Workbook()

    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)
    value_fill = PatternFill(start_color="FFFACD", end_color="FFFACD", fill_type="solid")
    value_font = Font(bold=True)

    strategy_tabs = [
        ("Strategy 1", TAB_PARAMS["Strategy 1"]),
        ("Strategy 2", TAB_PARAMS["Strategy 2"]),
        ("Strategy 3", TAB_PARAMS["Strategy 3"]),
        ("General",    TAB_PARAMS["General"]),
    ]

    ws_first = wb.active
    ws_first.title = strategy_tabs[0][0]

    for i, (tab_name, param_names) in enumerate(strategy_tabs):
        ws = wb.active if i == 0 else wb.create_sheet(tab_name)
        ws.title = tab_name

        ws.column_dimensions["A"].width = 22
        ws.column_dimensions["B"].width = 18
        ws.column_dimensions["C"].width = 55
        ws.column_dimensions["D"].width = 28

        headers = ["Parameter", "Value (edit this)", "Description", "Valid Range"]
        for col, hdr in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=hdr)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")

        for row_idx, pname in enumerate(param_names, 2):
            ws.cell(row=row_idx, column=1, value=pname)

            spec = PARAM_SPECS.get(pname, {})
            if pname in ("START_DATE", "END_DATE"):
                default_val = "2020-01-01" if pname == "START_DATE" else "2025-12-31"
            else:
                default_val = spec.get("default", "")
            val_cell = ws.cell(row=row_idx, column=2, value=default_val)
            val_cell.fill = value_fill
            val_cell.font = value_font

            desc = PARAM_DESCRIPTIONS.get(pname, "")
            desc_cell = ws.cell(row=row_idx, column=3, value=desc)
            desc_cell.alignment = Alignment(wrap_text=True)
            ws.row_dimensions[row_idx].height = 45

            vrange = PARAM_VALID_RANGES.get(pname, "")
            ws.cell(row=row_idx, column=4, value=vrange)

    _add_documentation_tab(wb)
    wb.save(str(output_path))
    print(f"  Template created: {output_path}")


def _add_documentation_tab(wb: openpyxl.Workbook) -> None:
    """Add Documentation tab with plain-English explanations of every parameter."""
    ws = wb.create_sheet("Documentation")
    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 75

    title_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    title_font = Font(color="FFFFFF", bold=True, size=12)
    section_fill = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")
    section_font = Font(bold=True)

    ws.cell(row=1, column=1, value="VAM Strategy — Parameter Documentation").fill = title_fill
    ws.cell(row=1, column=1).font = title_font
    ws.cell(row=1, column=2, value="Edit values ONLY in the yellow cells of the other tabs.").fill = title_fill

    doc_rows = [
        ("WHAT IS UPRO?", "UPRO is a 3x leveraged ETF that tracks the S&P 500. When the S&P 500 rises 1%, "
         "UPRO rises ~3%. When it falls 1%, UPRO falls ~3%. High return potential, high risk."),
        ("WHAT IS TQQQ?", "TQQQ is a 3x leveraged ETF that tracks the Nasdaq 100. Same leverage mechanics "
         "as UPRO but focused on technology stocks (Apple, Microsoft, Nvidia, etc.)."),
        ("WHAT IS SPXU?", "SPXU is a 3x INVERSE S&P 500 ETF. When the market crashes, SPXU RISES. "
         "Strategy 2 uses SPXU only during crash conditions to profit from market declines."),
        ("WHAT IS SVIX?", "SVIX is an ETF that profits when market fear (measured by VIX) subsides. "
         "After a panic spike, VIX typically falls quickly — SVIX captures this 'fear crush'. "
         "SVIX only exists from February 2022 onwards; before that, Strategy 3 is inactive."),
        ("WHAT IS SGOV?", "SGOV holds 0-3 month US Treasury bills (T-Bills). Near-zero risk, "
         "earns the current short-term interest rate (~5% in 2022-2025). "
         "Sidelined cash is held here when no strategy is active."),
        ("WHAT IS VIX?", "The VIX is the CBOE Volatility Index — often called the 'fear gauge'. "
         "VIX > 30 signals high market fear/volatility. VIX < 20 is calm. "
         "The strategies use VIX as a primary trigger for risk-off mode."),
        ("HOW DOES THE 7-STATE MACHINE WORK?",
         "The strategy cycles through 7 states based on market conditions:\n"
         "1. BULL_100: Fully invested 75% UPRO + 25% TQQQ\n"
         "2. BULL_TRIMMED: 75% of bull position (RSI overbought)\n"
         "3. DEFENSIVE_SPY: SPY trending down, UPRO sleeve halved\n"
         "4. DEFENSIVE_QQQ: QQQ trending down, TQQQ sleeve halved\n"
         "5. DEFENSIVE_BOTH: Both trending down, both sleeves halved\n"
         "6. CASH: Kill switch fired — 100% in SGOV\n"
         "7. SMA_RECOVERY: Exiting cash cautiously"),
        ("HOW ARE TRADES EXECUTED?", "T+1 execution: signals are generated at end of day (4 PM), "
         "but trades are executed at the NEXT DAY's open price. This prevents look-ahead bias."),
        ("CROSS-PARAMETER RULES",
         "These rules are automatically checked and will prevent the backtest from running if violated:\n"
         "1. UPRO_WEIGHT + TQQQ_WEIGHT must be <= 0.99\n"
         "2. RSI_REBUY_LEVEL must be < RSI_TRIM_LEVEL\n"
         "3. START_DATE must be before END_DATE\n"
         "4. SMA_SHORT must be < SMA_LONG\n"
         "5. SVIX_PANIC_PCT + SPXU_DEPLOY_PCT must not exceed (1 - SGOV_BUFFER_PCT)"),
    ]

    current_row = 3
    for section, content in doc_rows:
        sec_cell = ws.cell(row=current_row, column=1, value=section)
        sec_cell.fill = section_fill
        sec_cell.font = section_font
        content_cell = ws.cell(row=current_row, column=2, value=content)
        content_cell.alignment = Alignment(wrap_text=True)
        line_count = content.count("\n") + 1
        ws.row_dimensions[current_row].height = max(30, 18 * line_count)
        current_row += 1

    current_row += 1
    ws.cell(row=current_row, column=1, value="PARAMETER QUICK REFERENCE")
    ws.cell(row=current_row, column=1).fill = title_fill
    ws.cell(row=current_row, column=1).font = title_font
    current_row += 1

    ws.cell(row=current_row, column=1, value="Parameter").font = section_font
    ws.cell(row=current_row, column=2, value="Plain-English Description").font = section_font
    current_row += 1

    for pname, desc in PARAM_DESCRIPTIONS.items():
        ws.cell(row=current_row, column=1, value=pname)
        desc_cell = ws.cell(row=current_row, column=2, value=desc)
        desc_cell.alignment = Alignment(wrap_text=True)
        ws.row_dimensions[current_row].height = 40
        current_row += 1


# =============================================================================
# EXCEL READER
# =============================================================================


def read_excel_params(xlsx_path: Path) -> dict:
    """Read all parameters from vam_parameters.xlsx.

    Returns a dict of {param_name: raw_value}.
    Raises ValueError on missing sheets or missing parameters.
    """
    if not xlsx_path.exists():
        raise ValueError(
            f"File not found: {xlsx_path}\n"
            "Run with --generate-template to create vam_parameters.xlsx first."
        )
    wb = openpyxl.load_workbook(str(xlsx_path), data_only=True)
    params: dict = {}

    for sheet_name, param_names in TAB_PARAMS.items():
        if sheet_name not in wb.sheetnames:
            raise ValueError(
                f"Required sheet '{sheet_name}' not found in {xlsx_path.name}. "
                f"Found sheets: {wb.sheetnames}"
            )
        ws = wb[sheet_name]
        row_map: dict = {}
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0] is not None:
                row_map[str(row[0]).strip()] = row[1]

        for pname in param_names:
            if pname not in row_map:
                raise ValueError(
                    f"Parameter '{pname}' missing from sheet '{sheet_name}'. "
                    f"Column A must contain the exact parameter name."
                )
            val = row_map[pname]
            if val is None:
                raise ValueError(
                    f"Parameter '{pname}' in sheet '{sheet_name}' has no value — "
                    f"column B is empty. Enter a value."
                )
            params[pname] = val

    return params


# =============================================================================
# PARAMETER VALIDATION
# =============================================================================


def validate_params(params: dict) -> None:
    """Validate all parameters. Prints errors to stderr and sys.exit(1) on failure.

    Checks:
    1. Per-parameter type and range
    2. Date format and logic
    3. Cross-parameter rules
    """
    errors: list[str] = []

    # Per-parameter range checks (excludes date params handled separately)
    for pname, spec in PARAM_SPECS.items():
        if pname not in params:
            errors.append(f"  {pname}: parameter not found in Excel")
            continue
        raw = params[pname]
        try:
            val = spec["type"](raw)
            params[pname] = val
        except (TypeError, ValueError):
            errors.append(
                f"  {pname}: value '{raw}' cannot be converted to "
                f"{spec['type'].__name__} — valid range: [{spec['min']}, {spec['max']}]"
            )
            continue
        if val < spec["min"] or val > spec["max"]:
            errors.append(
                f"  {pname} ({val}) is out of valid range "
                f"[{spec['min']}, {spec['max']}]"
            )

    # Date validation
    for dname in ("START_DATE", "END_DATE"):
        raw = params.get(dname)
        if raw is None:
            errors.append(f"  {dname}: missing from Excel")
            continue
        if isinstance(raw, datetime):
            params[dname] = raw.strftime("%Y-%m-%d")
        else:
            try:
                pd.Timestamp(str(raw))
                params[dname] = str(raw)[:10]
            except Exception:
                errors.append(f"  {dname}: '{raw}' is not a valid date (use YYYY-MM-DD)")

    if errors:
        print("\nVALIDATION FAILED — invalid parameter values:\n", file=sys.stderr)
        for e in errors:
            print(e, file=sys.stderr)
        print(
            "\nFix these values in vam_parameters.xlsx and re-run.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Cross-parameter validation
    cross_errors: list[str] = []

    total_bull = params["UPRO_WEIGHT"] + params["TQQQ_WEIGHT"]
    if total_bull > 1.0:
        cross_errors.append(
            f"  UPRO_WEIGHT ({params['UPRO_WEIGHT']}) + TQQQ_WEIGHT ({params['TQQQ_WEIGHT']}) "
            f"= {total_bull:.3f} — must be <= 1.0 (weights are proportional; 99% cap applied internally)"
        )

    if params["RSI_REBUY_LEVEL"] >= params["RSI_TRIM_LEVEL"]:
        cross_errors.append(
            f"  RSI_REBUY_LEVEL ({params['RSI_REBUY_LEVEL']}) must be less than "
            f"RSI_TRIM_LEVEL ({params['RSI_TRIM_LEVEL']}) — "
            f"invalid: rebuy level exceeds trim level"
        )

    if pd.Timestamp(params["START_DATE"]) >= pd.Timestamp(params["END_DATE"]):
        cross_errors.append(
            f"  START_DATE ({params['START_DATE']}) must be before END_DATE ({params['END_DATE']})"
        )

    if params["SMA_SHORT"] >= params["SMA_LONG"]:
        cross_errors.append(
            f"  SMA_SHORT ({params['SMA_SHORT']}) must be less than SMA_LONG ({params['SMA_LONG']})"
        )

    combined_alloc = params["SVIX_PANIC_PCT"] + params["SPXU_DEPLOY_PCT"]
    if combined_alloc >= 0.99:
        cross_errors.append(
            f"  SVIX_PANIC_PCT ({params['SVIX_PANIC_PCT']}) + "
            f"SPXU_DEPLOY_PCT ({params['SPXU_DEPLOY_PCT']}) = "
            f"{combined_alloc:.2f} — must be < 0.99 (combined allocation cap violated)"
        )

    if cross_errors:
        print("\nCROSS-PARAMETER VALIDATION FAILED:\n", file=sys.stderr)
        for e in cross_errors:
            print(e, file=sys.stderr)
        print("\nFix these values in vam_parameters.xlsx and re-run.", file=sys.stderr)
        sys.exit(1)

    print("  PASS: All 24 parameters valid (range checks + 5 cross-parameter rules).")


# =============================================================================
# STEP 2 — PARAMETERIZED INDICATOR COMPUTATION
# =============================================================================


def _compute_step2_indicators(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """Recompute Step 2 indicators with custom SMA/RSI periods.

    Column names match exactly what run_step2_backtest expects.
    """
    sma_s = params["SMA_SHORT"]
    sma_l = params["SMA_LONG"]
    rsi_p = params["RSI_PERIOD"]

    df = df.copy()
    df["SPY_SMA50"] = df["SPY_Close"].rolling(sma_s).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(sma_l).mean()
    df["SPY_RSI"] = _s2.calculate_rsi(df["SPY_Close"], rsi_p)
    df["SPY_below_50_streak"] = _s2.consecutive_streak(df["SPY_Close"] < df["SPY_SMA50"])
    df["SPY_above_50_streak"] = _s2.consecutive_streak(df["SPY_Close"] > df["SPY_SMA50"])
    df["QQQ_SMA50"] = df["QQQ_Close"].rolling(sma_s).mean()
    df["QQQ_below_50_streak"] = _s2.consecutive_streak(df["QQQ_Close"] < df["QQQ_SMA50"])
    df["QQQ_above_50_streak"] = _s2.consecutive_streak(df["QQQ_Close"] > df["QQQ_SMA50"])
    return df


def _rebuild_state_allocation(params: dict) -> dict:
    """Rebuild STATE_ALLOCATION with custom UPRO/TQQQ weights.

    Replicates the pattern from vam_step2_databento.py STATE_ALLOCATION.
    """
    from vam_step2_databento import State

    upro_w = params["UPRO_WEIGHT"]
    tqqq_w = params["TQQQ_WEIGHT"]
    cap = 0.99

    return {
        State.BULL_100:       (upro_w * cap, tqqq_w * cap),
        State.BULL_TRIMMED:   (upro_w * 0.75, tqqq_w * 0.75),
        State.DEFENSIVE_SPY:  (upro_w * 0.50, tqqq_w),
        State.DEFENSIVE_QQQ:  (upro_w, tqqq_w * 0.50),
        State.DEFENSIVE_BOTH: (upro_w * 0.50, tqqq_w * 0.50),
        State.CASH:           (0.0, 0.0),
        State.SMA_RECOVERY:   (upro_w * 0.75, tqqq_w * 0.75),
    }


def _patch_step2_module(params: dict) -> None:
    """Monkey-patch vam_step2_databento module constants with Excel params."""
    _s2.VIX_KILL = params["VIX_KILL"]
    _s2.SMA_CONFIRM_DAYS = params["SMA_CONFIRM_DAYS"]
    _s2.RSI_SELL = params["RSI_TRIM_LEVEL"]
    _s2.RSI_REBUY = params["RSI_REBUY_LEVEL"]
    _s2.SLIPPAGE_BPS_NORMAL = params["SLIPPAGE_NORMAL_BPS"]
    _s2.SLIPPAGE_BPS_STRESS = params["SLIPPAGE_STRESS_BPS"]
    _s2.INITIAL_CAPITAL = params["INITIAL_CAPITAL"]
    _s2.RISK_FREE_RATE = params["RISK_FREE_RATE"]
    _s2.STATE_ALLOCATION = _rebuild_state_allocation(params)


# =============================================================================
# STEP 4 COMBINED — PARAMETERIZED DATA + MODULE PATCHING
# =============================================================================


def _build_combined_df(params: dict) -> pd.DataFrame:
    """Load raw data and compute combined indicators with custom params.

    Replicates load_combined_data() from vam_step4_combined_databento.py
    but with configurable SMA periods and VIX_CURVE_DAYS.
    """
    spy = _s4.load_databento_csv(DATA_DIR / "databento" / "equities" / "SPY_daily.csv")
    spxu = _s4.load_databento_csv(DATA_DIR / "databento" / "equities" / "SPXU_daily.csv")
    svix = _s4.load_databento_csv(DATA_DIR / "databento" / "equities" / "SVIX_daily.csv")
    vix_df = _s4.load_databento_csv(DATA_DIR / "cboe" / "VIX_daily.csv")

    df = pd.DataFrame(index=spy.index)
    df["SPY_Close"] = spy["close"]
    df["SPXU_Close"] = spxu["close"].reindex(spy.index)
    df["SPXU_Open"] = spxu["open"].reindex(spy.index)
    df["SVIX_Close"] = svix["close"].reindex(spy.index)
    df["SVIX_Open"] = svix["open"].reindex(spy.index)
    df["VIX"] = vix_df["close"].reindex(spy.index)
    df = df.ffill().dropna(subset=["VIX", "SPY_Close"])

    # Custom SMA periods: SPXU_EXIT_SMA for exit signal, SMA_LONG for entry/kill
    df["SPY_SMA50"] = df["SPY_Close"].rolling(params["SPXU_EXIT_SMA"]).mean()
    df["SPY_SMA200"] = df["SPY_Close"].rolling(params["SMA_LONG"]).mean()

    # VIX_CURVE_DAYS consecutive falling days (rolling window)
    df["vix_fell"] = df["VIX"].diff() < 0
    curve_days = params["VIX_CURVE_DAYS"]
    df["vix_curve_down"] = df["vix_fell"].rolling(curve_days).sum() == curve_days

    return df


def _patch_combined_module(params: dict) -> None:
    """Monkey-patch vam_step4_combined_databento module constants."""
    _s4.SPXU_ALLOC = params["SPXU_DEPLOY_PCT"]
    _s4.VIX_SPXU_ENTRY = params["VIX_SHORT_ENTRY"]
    _s4.VIX_SPXU_EXIT = params["VIX_SHORT_ENTRY"]    # exit same as entry threshold
    _s4.SLIPPAGE_SPXU = params["SLIPPAGE_STRESS_BPS"]
    _s4.SVIX_INIT_PCT = params["SVIX_INIT_PCT"]
    _s4.SVIX_PANIC_PCT = params["SVIX_PANIC_PCT"]
    _s4.SVIX_PANIC_VIX = params["SVIX_PANIC_VIX"]
    _s4.VIX_SVIX_ENTRY_LOWER = params["VIX_SHORT_ENTRY"]
    _s4.VIX_SVIX_EXIT = params["SVIX_EXIT_VIX"]
    _s4.SGOV_BUFFER_PCT = params["SGOV_BUFFER_PCT"]
    _s4.SLIPPAGE_SVIX = params["SLIPPAGE_STRESS_BPS"]
    _s4.INITIAL_CAPITAL = params["INITIAL_CAPITAL"]
    _s4.RISK_FREE_RATE = params["RISK_FREE_RATE"]


# =============================================================================
# CHART GENERATOR
# =============================================================================


def _generate_chart(daily_log: list[dict], metrics: dict, output_dir: Path) -> Path:
    """Generate a performance chart PNG with equity curve and drawdown."""
    daily = pd.DataFrame(daily_log)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.set_index("date")

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), gridspec_kw={"height_ratios": [3, 1, 1]})
    fig.suptitle(
        f"VAM Combined Portfolio — Custom Parameters\n"
        f"{metrics['start_date']} to {metrics['end_date']}  |  "
        f"Sharpe: {metrics['sharpe_ratio']:.3f}  |  "
        f"CAGR: {metrics['cagr_pct']:+.1f}%  |  "
        f"Max DD: {metrics['max_drawdown_pct']:.1f}%",
        fontsize=11,
    )

    # Equity curve
    ax1 = axes[0]
    ax1.plot(daily.index, daily["portfolio_value"], color="#1565C0", linewidth=1.5, label="Portfolio")
    ax1.set_ylabel("Portfolio Value ($)")
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc="upper left")

    # Drawdown
    ax2 = axes[1]
    cummax = daily["portfolio_value"].cummax()
    drawdown = (daily["portfolio_value"] - cummax) / cummax * 100
    ax2.fill_between(daily.index, drawdown, 0, color="#C62828", alpha=0.6)
    ax2.set_ylabel("Drawdown (%)")
    ax2.grid(True, alpha=0.3)

    # VIX
    ax3 = axes[2]
    ax3.plot(daily.index, daily["vix"], color="#6A1B9A", linewidth=1.0, label="VIX")
    ax3.axhline(y=30, color="red", linestyle="--", alpha=0.7, linewidth=0.8, label="VIX=30")
    ax3.set_ylabel("VIX")
    ax3.set_xlabel("Date")
    ax3.grid(True, alpha=0.3)
    ax3.legend(loc="upper right", fontsize=8)

    plt.tight_layout()
    chart_path = output_dir / "performance_chart.png"
    plt.savefig(str(chart_path), dpi=120, bbox_inches="tight")
    plt.close()
    return chart_path


# =============================================================================
# MAIN BACKTEST RUNNER
# =============================================================================


def run_with_params(params: dict) -> dict:
    """Run full combined backtest with Excel params. Returns combined metrics."""
    start_ts = pd.Timestamp(params["START_DATE"])
    end_ts = pd.Timestamp(params["END_DATE"])

    # Step 1: Patch and run Step 2 (Strategy 1 — VAM Split)
    print("\n  [Step 2] Patching module constants...")
    _patch_step2_module(params)

    print("  [Step 2] Loading raw data...")
    df_raw = _s2.load_step2_data()

    print("  [Step 2] Computing indicators with custom SMA/RSI periods...")
    df_s2 = _compute_step2_indicators(df_raw, params)

    # Apply date filter
    df_s2 = df_s2[
        (df_s2.index >= start_ts) & (df_s2.index <= end_ts)
    ]

    print(f"  [Step 2] Running backtest ({len(df_s2)} bars)...")
    trades2, daily2, metrics2 = _s2.run_step2_backtest(df_s2)
    sharpe2 = metrics2.get("sharpe_ratio", metrics2.get("sharpe", 0.0))
    print(f"  [Step 2] Done: {len(trades2)} trades, Sharpe={sharpe2:.3f}")

    # Build step2_daily DataFrame for combined script
    step2_daily = pd.DataFrame(daily2)
    step2_daily["date"] = pd.to_datetime(step2_daily["date"])
    step2_daily = step2_daily.set_index("date").sort_index()

    # Step 2: Patch and run combined (Strategies 2 + 3)
    print("\n  [Combined] Patching module constants...")
    _patch_combined_module(params)

    print("  [Combined] Building combined data with custom SMA/VIX_CURVE_DAYS...")
    df_combined = _build_combined_df(params)
    df_combined = df_combined[
        (df_combined.index >= start_ts) & (df_combined.index <= end_ts)
    ]

    print(f"  [Combined] Running combined backtest ({len(df_combined)} bars)...")
    trades, daily_log, metrics = _s4.run_combined_backtest(df_combined, step2_daily)
    print(f"  [Combined] Done: {len(trades)} trades, Sharpe={metrics['sharpe_ratio']:.3f}")

    return trades, daily_log, metrics


def _save_run_outputs(
    trades: list[dict],
    daily_log: list[dict],
    metrics: dict,
    output_dir: Path,
) -> None:
    """Save all run outputs to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(trades).to_csv(output_dir / "trade_log.csv", index=False)
    pd.DataFrame(daily_log).to_csv(output_dir / "portfolio_values.csv", index=False)
    with open(output_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print(f"  Saved: trade_log.csv ({len(trades)} trades)")
    print(f"  Saved: portfolio_values.csv ({len(daily_log)} days)")
    print(f"  Saved: metrics.json")


# =============================================================================
# INVALID PARAMS TEST MODE
# =============================================================================


def _run_test_invalid() -> None:
    """Test that the runner rejects a known-invalid parameter set.

    Used by the phase-level test: python vam_parameter_runner.py --test-invalid
    Creates params with RSI_REBUY_LEVEL > RSI_TRIM_LEVEL and validates.
    """
    print("[test-invalid] Creating deliberately invalid parameter set...", file=sys.stderr)
    bad_params = {pname: spec["default"] for pname, spec in PARAM_SPECS.items()}
    bad_params["START_DATE"] = "2020-01-01"
    bad_params["END_DATE"] = "2025-12-31"
    # Violate: RSI_REBUY_LEVEL > RSI_TRIM_LEVEL
    bad_params["RSI_REBUY_LEVEL"] = 80.0
    bad_params["RSI_TRIM_LEVEL"] = 70.0
    print("[test-invalid] RSI_REBUY_LEVEL=80, RSI_TRIM_LEVEL=70 (rebuy > trim — invalid)", file=sys.stderr)
    validate_params(bad_params)
    # If we reach here, validation failed to catch the error
    print("ERROR: validate_params should have caught invalid RSI params", file=sys.stderr)
    sys.exit(1)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================


def _print_summary(metrics: dict) -> None:
    """Print compact performance summary."""
    print(f"\n{'=' * 60}")
    print(f"  VAM Combined — Custom Parameter Run Results")
    print(f"{'=' * 60}")
    print(f"  Period:       {metrics['start_date']} → {metrics['end_date']}")
    print(f"  Final Value:  ${metrics['final_value']:,.2f}")
    print(f"  Total Return: {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:         {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe:       {metrics['sharpe_ratio']:.3f}")
    print(f"  Sortino:      {metrics['sortino_ratio']:.3f}")
    print(f"  Max Drawdown: {metrics['max_drawdown_pct']:.2f}%")
    print(f"  Total Trades: {metrics['total_trades']}")
    print(f"{'=' * 60}")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="VAM Parameter Runner")
    parser.add_argument("--params", type=Path, default=None, help="Path to vam_parameters.xlsx")
    parser.add_argument("--generate-template", action="store_true", help="Create vam_parameters.xlsx")
    parser.add_argument("--test-invalid", action="store_true", help="Test invalid parameter rejection")
    args = parser.parse_args()

    if args.test_invalid:
        _run_test_invalid()
        return

    if args.generate_template:
        default_path = SCRIPTS_DIR / "vam_parameters.xlsx"
        generate_template(default_path)
        return

    # Default: run with params file
    params_path = args.params or (SCRIPTS_DIR / "vam_parameters.xlsx")
    print(f"\n[Runner] Reading parameters from: {params_path.name}")

    print("[Runner] Reading Excel...")
    try:
        params = read_excel_params(params_path)
    except ValueError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    print("[Runner] Validating parameters...")
    validate_params(params)

    print("[Runner] Running combined backtest with custom parameters...")
    trades, daily_log, metrics = run_with_params(params)

    _print_summary(metrics)

    print(f"\n[Runner] Saving outputs to: {RUN_LATEST_DIR.name}/")
    _save_run_outputs(trades, daily_log, metrics, RUN_LATEST_DIR)

    print("[Runner] Generating performance chart...")
    chart_path = _generate_chart(daily_log, metrics, RUN_LATEST_DIR)
    print(f"  Saved: {chart_path.name}")

    print(f"\n[Runner] COMPLETE — outputs at {RUN_LATEST_DIR}")


if __name__ == "__main__":
    main()
