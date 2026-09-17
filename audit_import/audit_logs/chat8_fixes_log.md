# Chat 8 — Auditor Findings Round 2 Fix Log

**Date:** 2026-04-05
**Engineer:** IFA / Claude
**Scope:** 4 auditor-flagged issues fixed across 5 backtest scripts and Strategy Explanation PDF

---

## Issue 1 — PDF shows 50% UPRO for BULL_TRIMMED (code uses 75%)

**Root cause:** PDF generator had wrong allocations for Step 1 BULL_TRIMMED state and Step 2 all states.

**Code audit:** `vam_step1_databento.py` `STATE_ALLOCATION[BULL_TRIMMED] = 0.75` (75%). PDF said 50%.

**Fix — `audit_package/docs/generate_strategy_pdf.py`:**
- Step 1 state table: `BULL_TRIMMED` allocation `"50% UPRO"` → `"75% UPRO"`
- Step 1 signals table: RSI TRIM rule `"trim to 50%"` → `"trim to 75%"`
- Step 2 state table: complete rewrite to match code
  - `BULL_FULL / 50% / 50%` → `BULL_100 / 74% / 25%` (code: 0.7425 / 0.2475)
  - `BULL_TRIMMED / 25% / 25%` → `56% / 19%` (code: 0.5625 / 0.1875)
  - `DEF_SPY / 0% / 50%` → `DEFENSIVE_SPY / 38% / 25%` (code: 0.375 / 0.25)
  - `DEF_QQQ / 50% / 0%` → `DEFENSIVE_QQQ / 75% / 13%` (code: 0.75 / 0.125)
  - `DEF_BOTH / 0% / 0%` → `DEFENSIVE_BOTH / 38% / 13%` (code: 0.375 / 0.125)

**Test result:** Grepped generate_strategy_pdf.py — zero wrong "50% UPRO" allocation instances remaining. DEFENSIVE "50% UPRO" preserved (correct by design).

---

## Issue 2 — Flat $1 commission in PDF (all scripts already use IBKR tiered)

**Root cause:** PDF "Other Parameters" table said `"$1.00 per trade (flat)"`. All 5 scripts already used `ibkr_commission()` (0.005/share, min $1, max 1% of trade value). Only the PDF was wrong.

**Fix — `audit_package/docs/generate_strategy_pdf.py`:**
- `"$1.00 per trade (flat)"` → `"IBKR tiered: $0.005/share, min $1.00, max 1% of trade value"`

**Script audit:**
| Script | Commission | Status |
|--------|-----------|--------|
| vam_step1_databento.py | ibkr_commission() ✓ | OK |
| vam_step2_databento.py | ibkr_commission() ✓ | OK |
| vam_step3_databento.py | ibkr_commission() ✓ | OK |
| vam_step4_svix_databento.py | ibkr_commission() ✓ | OK |
| vam_step4_combined_databento.py | ibkr_commission() ✓ | OK |

---

## Issue 3 — Slippage threshold off-by-one (vix > 25 vs vix >= 25)

**Root cause:** Code used `vix > 25` (strictly greater). PDF said "VIX > 25". Auditor specification requires `>=` for the conservative/correct interpretation.

**Fix — scripts (step1 and step2 only — steps 3/4 use fixed 20bps, no VIX-25 check):**
- `vam_step1_databento.py` line 248: `if is_kill or vix > 25:` → `if is_kill or vix >= 25:`
- `vam_step2_databento.py` line 290: `if is_kill or vix > 25:` → `if is_kill or vix >= 25:`

**Fix — PDF:**
- Slippage table: `"VIX > 25 OR kill switch fires"` → `"VIX >= 25 OR kill switch fires"`

**Note:** The `*-Anmol's MacBook Pro.py` backup files still show `vix > 25` — these are stale backups, not active scripts. Not modified.

**Test result:** Grepped all 5 active scripts — zero `vix > 25` remaining. Both step1 and step2 confirmed `vix >= 25`.

---

## Issue 4 — No 2-day confirmation for DEFENSIVE exit in PDF

**Code audit:**
- `vam_step1_databento.py` line ~200: `if above_50_streak >= SMA_CONFIRM_DAYS and vix < VIX_KILL:` — EXIT **does** require 2-day confirmation.
- `vam_step2_databento.py` lines 240/247/252: `spy_above = spy_above_streak >= SMA_CONFIRM_DAYS` — same pattern.

**Finding:** Code uses 2-day confirmation for DEFENSIVE exit. PDF was silent on exit rule.

**Fix — `audit_package/docs/generate_strategy_pdf.py`:**
Added to DEFENSIVE signal description:
> "EXIT from DEFENSIVE also requires 2 consecutive days ABOVE 50-day SMA (same confirmation rule as entry)."

---

## Backtest Re-run Results (post-fix)

Results changed due to slippage fix (more trades now trigger 20bps instead of 5bps when VIX = 25.0 exactly).

| Script | Final Value | CAGR | Status |
|--------|-------------|------|--------|
| Step 1 UPRO Only | $333,662 | +19.17% | PASSED |
| Step 2 UPRO+TQQQ | $408,259 | +22.72% | PASSED |
| Step 3 Predatory Short | $69,476 | -5.16% | PASSED (all phase-gate tests) |
| Step 4 SVIX standalone | $109,808 | +1.23% | PASSED (all phase-gate tests) |
| Step 4 Combined | $296,800 | +17.15% | PASSED (all phase-gate tests) |

---

## Files Modified

| File | Change |
|------|--------|
| `scripts/vam_step1_databento.py` | `vix > 25` → `vix >= 25` in get_slippage_bps |
| `scripts/vam_step2_databento.py` | `vix > 25` → `vix >= 25` in get_slippage_bps |
| `audit_package/docs/generate_strategy_pdf.py` | Issues 1, 2, 3, 4 all fixed |
| `audit_package/docs/VAM_Strategy_Explanation.pdf` | Regenerated from fixed generator |
| `results/step1_databento_*.csv/.json` | Regenerated |
| `results/step2_databento_*.csv/.json` | Regenerated |
| `results/step3_databento_*.csv/.json` | Regenerated |
| `results/step4_svix_databento_*.csv/.json` | Regenerated |
| `results/step4_combined_*.csv/.json` | Regenerated |

## Files NOT Modified (no changes needed)

| File | Reason |
|------|--------|
| `scripts/vam_step3_databento.py` | Uses fixed 20bps SPXU slippage, no VIX-25 check |
| `scripts/vam_step4_svix_databento.py` | Uses fixed 20bps SVIX slippage, no VIX-25 check |
| `scripts/vam_step4_combined_databento.py` | Uses fixed 20bps for both, no VIX-25 check |

## Delivery Package

All updated files copied to `/tmp/ravi-vam-audit-update/`.
