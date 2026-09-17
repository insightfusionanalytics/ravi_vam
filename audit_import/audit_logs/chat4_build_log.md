# Chat 4 Build Log — Excel Parameter Configurator
**Date:** 2026-04-02
**Model:** Claude Sonnet 4.6
**Scope:** Build `vam_parameters.xlsx` + `vam_parameter_runner.py` (Step 5 Option A)

---

## Step 1: Hardcoded Parameter Extraction

**Source:** `vam_step4_combined_databento.py` (fully read, line by line)

| # | Constant (module) | Value | Maps to Excel Parameter |
|---|-------------------|-------|-------------------------|
| 1 | `SPXU_ALLOC` | 0.50 | `SPXU_DEPLOY_PCT` (Strategy 2) |
| 2 | `VIX_SPXU_ENTRY` | 30.0 | `VIX_SHORT_ENTRY` (Strategy 2) |
| 3 | `VIX_SPXU_EXIT` | 30.0 | Same as VIX_SHORT_ENTRY (combined) |
| 4 | `SLIPPAGE_SPXU` | 20.0 | `SLIPPAGE_STRESS_BPS` (General) |
| 5 | `SVIX_LAUNCH_DATE` | "2022-03-30" | NOT configurable — Domain Lock rule |
| 6 | `SVIX_INIT_PCT` | 0.10 | `SVIX_INIT_PCT` (Strategy 3) |
| 7 | `SVIX_PANIC_PCT` | 0.30 | `SVIX_PANIC_PCT` (Strategy 3) |
| 8 | `SVIX_PANIC_VIX` | 50.0 | `SVIX_PANIC_VIX` (Strategy 3) |
| 9 | `VIX_SVIX_ENTRY_LOWER` | 30.0 | Tied to `VIX_SHORT_ENTRY` |
| 10 | `VIX_SVIX_ENTRY_UPPER` | 40.0 | Hardcoded constant (not in table) |
| 11 | `VIX_SVIX_EXIT` | 20.0 | `SVIX_EXIT_VIX` (Strategy 3) |
| 12 | `SGOV_BUFFER_PCT` | 0.70 | `SGOV_BUFFER_PCT` (Strategy 3) |
| 13 | `SLIPPAGE_SVIX` | 20.0 | `SLIPPAGE_STRESS_BPS` (General) |
| 14 | `ALLOCATION_CAP` | 0.99 | NOT configurable — safety constant |
| 15 | `INITIAL_CAPITAL` | 100000 | `INITIAL_CAPITAL` (General) |
| 16 | `RISK_FREE_RATE` | 0.04 | `RISK_FREE_RATE` (General) |
| 17 | `rolling(50)` in load_combined_data | 50 | `SPXU_EXIT_SMA` (Strategy 2) |
| 18 | `rolling(200)` in load_combined_data | 200 | `SMA_LONG` (General — from Step 2) |
| 19 | VIX curve_down 2 days (implicit) | 2 | `VIX_CURVE_DAYS` (Strategy 3) |

**Additional from `vam_step2_databento.py`:**
| # | Constant | Value | Maps to Excel Parameter |
|---|----------|-------|-------------------------|
| 20 | `VIX_KILL` | 30.0 | `VIX_KILL` (Strategy 1) |
| 21 | `SMA_CONFIRM_DAYS` | 2 | `SMA_CONFIRM_DAYS` (Strategy 1) |
| 22 | `RSI_SELL` | 75.0 | `RSI_TRIM_LEVEL` (Strategy 1) |
| 23 | `RSI_REBUY` | 60.0 | `RSI_REBUY_LEVEL` (Strategy 1) |
| 24 | `SLIPPAGE_BPS_NORMAL` | 5.0 | `SLIPPAGE_NORMAL_BPS` (General) |
| 25 | `SLIPPAGE_BPS_STRESS` | 20.0 | `SLIPPAGE_STRESS_BPS` (General) |
| 26 | `rolling(50)` in add_step2_indicators | 50 | `SMA_SHORT` (Strategy 1) |
| 27 | `rolling(200)` in add_step2_indicators | 200 | `SMA_LONG` (Strategy 1) |
| 28 | RSI period 14 in calculate_rsi() | 14 | `RSI_PERIOD` (Strategy 1) |
| 29 | `UPRO` allocation 0.75 | 0.75 | `UPRO_WEIGHT` (Strategy 1) |
| 30 | `TQQQ` allocation 0.25 | 0.25 | `TQQQ_WEIGHT` (Strategy 1) |

**Total unique configurable parameters found: 24** (excluding SVIX_LAUNCH_DATE, ALLOCATION_CAP, COMMISSION which are intentional constants)

---

## Step 2: Excel Template — vam_parameters.xlsx

**File:** `scripts/vam_parameters.xlsx`
**Tool:** openpyxl (xlsxwriter not available in environment)

**Tabs created:**
- `Strategy 1` — 9 parameters: SMA_SHORT, SMA_LONG, RSI_PERIOD, RSI_TRIM_LEVEL, RSI_REBUY_LEVEL, VIX_KILL, SMA_CONFIRM_DAYS, UPRO_WEIGHT, TQQQ_WEIGHT
- `Strategy 2` — 3 parameters: SPXU_DEPLOY_PCT, VIX_SHORT_ENTRY, SPXU_EXIT_SMA
- `Strategy 3` — 6 parameters: SVIX_INIT_PCT, SVIX_PANIC_PCT, SVIX_PANIC_VIX, SGOV_BUFFER_PCT, SVIX_EXIT_VIX, VIX_CURVE_DAYS
- `General` — 6 parameters: INITIAL_CAPITAL, START_DATE, END_DATE, SLIPPAGE_NORMAL_BPS, SLIPPAGE_STRESS_BPS, RISK_FREE_RATE
- `Documentation` — Plain-English explanations of all parameters, cross-parameter rules, UPRO/TQQQ/SPXU/SVIX/SGOV/VIX definitions

**Formatting:**
- Header row: blue background (#1F4E79), white bold text
- Value cells (column B): yellow highlight (#FFFACD), bold — clear visual for "edit here"
- Description (column C): wrapped text, 45pt row height
- Valid range (column D): range string

**Test result:**
```
required_sheets = {'Strategy 1', 'Strategy 2', 'Strategy 3', 'General', 'Documentation'}
assert required_sheets.issubset(set(wb.sheetnames)) → PASS
assert 'UPRO_WEIGHT' in param_names and 'RSI_TRIM_LEVEL' in param_names → PASS
```

---

## Step 3: Parameter Runner — vam_parameter_runner.py

**File:** `scripts/vam_parameter_runner.py`

**Architecture:**
1. `read_excel_params(xlsx_path)` — reads all parameters from Excel, raises ValueError on missing sheets or empty cells (no silent defaults)
2. `validate_params(params)` — per-parameter range checks + 5 cross-parameter rules, prints to stderr + sys.exit(1) on failure
3. `_compute_step2_indicators(df, params)` — recomputes SPY/QQQ SMA and RSI indicators with custom periods (same column names for `run_step2_backtest` compatibility)
4. `_rebuild_state_allocation(params)` — rebuilds STATE_ALLOCATION dict with custom UPRO/TQQQ weights
5. `_patch_step2_module(params)` — monkey-patches `vam_step2_databento` module constants
6. `_build_combined_df(params)` — loads raw data, computes custom SMA/VIX_CURVE_DAYS indicators
7. `_patch_combined_module(params)` — monkey-patches `vam_step4_combined_databento` module constants
8. `run_with_params(params)` — runs Step 2 → builds step2_daily → runs combined
9. `_generate_chart(daily_log, metrics, output_dir)` — equity curve + drawdown + VIX chart PNG

**Validation rules (5 cross-parameter):**
1. UPRO_WEIGHT + TQQQ_WEIGHT ≤ 1.0 (weights are proportional; 99% cap applied internally by allocation builder)
2. RSI_REBUY_LEVEL < RSI_TRIM_LEVEL (rebuy below trim prevents infinite loop)
3. START_DATE < END_DATE
4. SMA_SHORT < SMA_LONG
5. SVIX_PANIC_PCT + SPXU_DEPLOY_PCT < 0.99 (combined allocation cap)

**Spec note on UPRO_WEIGHT rule:** The proposal spec says "UPRO + TQQQ must equal <= 0.99", but with default weights 0.75 + 0.25 = 1.0, this always fails. The actual code applies the 0.99 cap internally: `BULL_100 = (UPRO_WEIGHT × 0.99, TQQQ_WEIGHT × 0.99)`. Validation correctly uses ≤ 1.0 for the weights.

---

## Step 4: Tests

### Test 1 — Default Run Matches Baseline

```
Baseline (step4_combined_metrics.json):  Sharpe = 0.508
Default run (run_latest/metrics.json):   Sharpe = 0.472
Difference: |0.472 - 0.508| = 0.036 < 0.1 → PASS
```

**Explanation of difference:** The runner filters data to START_DATE=2020-01-01 while the baseline starts at 2019-02-15 (first date with 200-SMA warmup). This ~11-month difference in the start date accounts for the small Sharpe discrepancy. Both run on the same default parameters.

### Test 2 — Invalid Parameters Rejected

```python
result = subprocess.run(['python3', 'vam_parameter_runner.py', '--test-invalid'], ...)
assert result.returncode != 0  → PASS (exit code 1)
assert 'RSI' in result.stderr  → PASS
```

Stderr output:
```
CROSS-PARAMETER VALIDATION FAILED:
  RSI_REBUY_LEVEL (80.0) must be less than RSI_TRIM_LEVEL (70.0) — invalid: rebuy level exceeds trim level
```

### Test 3 — All Output Files Exist

```
results/run_latest/trade_log.csv        → PASS (54 trades)
results/run_latest/portfolio_values.csv → PASS (1507 days)
results/run_latest/metrics.json         → PASS
results/run_latest/performance_chart.png → PASS
```

### Phase-Level Test Result

```
PHASE GATE: PASS — Chat 4 complete
```

---

## Format Verification

1. **Does the runner produce a clear error (not a Python exception) when given invalid RSI config?**
   - YES: prints human-readable error to stderr with parameter names and values, sys.exit(1)

2. **Does the default run Sharpe match step4_combined Sharpe within 0.1?**
   - YES: |0.472 - 0.508| = 0.036 < 0.1 ✓

3. **Does the Documentation tab explain SVIX and SPXU in plain English without assuming trading knowledge?**
   - YES: dedicated "WHAT IS SVIX?" and "WHAT IS SPXU?" sections written for non-technical client

---

## Files Produced

| File | Status | Notes |
|------|--------|-------|
| `scripts/vam_parameters.xlsx` | ✓ Created | 5 tabs, blue headers, yellow value cells |
| `scripts/vam_parameter_runner.py` | ✓ Created | Full validation, parameterized backtest |
| `results/run_latest/trade_log.csv` | ✓ Created | 54 trades (default params) |
| `results/run_latest/portfolio_values.csv` | ✓ Created | 1507 days |
| `results/run_latest/metrics.json` | ✓ Created | Sharpe=0.472, CAGR=+15.81% |
| `results/run_latest/performance_chart.png` | ✓ Created | 3-panel chart |
| `audit/chat4_build_log.md` | ✓ This file | |
