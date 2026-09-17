# Chat Log: Parameterization Refactor — All 5 Backtest Scripts

**Date:** 2026-04-06
**Session type:** Code refactor — parameterize all flagged variables
**Source of truth:** `transcripts/strategy_extraction.md`

---

## Objective

Extract all 10 flagged parameters from `strategy_extraction.md` into configurable variables at the top of each script, using the current hardcoded values as defaults. Every unconfirmed parameter is now a single named constant — no more hardcoded duplicates buried in logic.

---

## Parameters Added (with flag numbers from strategy_extraction.md)

```python
RSI_PERIOD = 14              # Flag 1: Not specified by client. Industry standard.
RSI_REBUY = 60               # Flag 2: Not specified by client. IFA default.
DEFENSIVE_CONFIRM_DAYS = 2   # Flag 3: 2-day confirmation. Entry not confirmed, exit confirmed.
KILL_SWITCH_LOGIC = "OR"     # Flag 4: "OR" = VIX>30 OR SPY<200SMA. "AND" = both required.
REENTRY_VIX = 30             # Flag 5: Conflicting — Ravi said 30 and 60. Using 30.
REENTRY_REQUIRE_50SMA = True # Flag 6: Not confirmed. IFA addition for safety.
SPXU_EXIT_VIX = 30           # Flag 7: Not discussed. IFA default: exit when VIX < 30.
SPXU_EXIT_SPY_50SMA = True   # Flag 7b: Exit when SPY reclaims 50-SMA.
USE_SGOV = True              # Flag 8: Not mentioned. IFA default: idle cash in SGOV.
COMMISSION_MODEL = "IBKR"    # Flag 10: Not discussed. Using IBKR tiered.
```

SVIX-specific (Strategy 3 — entire spec is IFA assumption):
```python
SVIX_ENTRY_VIX_LOW = 30
SVIX_ENTRY_VIX_HIGH = 40
SVIX_PANIC_VIX = 50
SVIX_INITIAL_ALLOC = 0.10
SVIX_PANIC_ALLOC = 0.30
SVIX_SGOV_BUFFER = 0.70
SVIX_EXIT_VIX = 20
```

---

## Files Modified

| Script | Changes |
|--------|---------|
| `vam_step1_databento.py` | Full PARAMETERS section added. `DEFENSIVE_CONFIRM_DAYS`, `RSI_PERIOD`, `KILL_SWITCH_LOGIC`, `REENTRY_VIX`, `REENTRY_REQUIRE_50SMA` wired into logic. |
| `vam_step2_databento.py` | Same PARAMETERS section. `SMA_RECOVERY` re-entry updated with `REENTRY_VIX` and `REENTRY_REQUIRE_50SMA`. |
| `vam_step3_databento.py` | `SPXU_EXIT_VIX`, `SPXU_EXIT_SPY_50SMA` added and wired into exit logic. |
| `vam_step4_svix_databento.py` | All 7 SVIX params renamed from old internal names. Prominent `STRATEGY 3: ENTIRE SPEC IS IFA ASSUMPTION` block added. |
| `vam_step4_combined_databento.py` | All 10 flags present. Both SPXU and SVIX exit logic updated. |

---

## Bugs Encountered and Fixed

### Double-Replacement Bug (vam_step4_svix_databento.py)

**Root cause:** The new constants block was written with `SVIX_EXIT_VIX: float = 20.0`. When `replace_all` of `VIX_EXIT` → `SVIX_EXIT_VIX` was applied, it matched the substring `VIX_EXIT` inside `SVIX_EXIT_VIX` itself (S + VIX_EXIT + _VIX), producing `SSVIX_EXIT_VIX_VIX` on that line.

**Fix:** Manually corrected line 57 from `SSVIX_EXIT_VIX_VIX: float = 20.0` back to `SVIX_EXIT_VIX: float = 20.0`.

**Lesson:** When doing substring replace_all, check that the new string doesn't contain the old string as a substring. If it does, write the constants block first, then do targeted edits instead of replace_all.

---

## Verification Results

All 5 scripts ran cleanly after parameterization. Results match prior runs:

| Script | Key Output |
|--------|-----------|
| `vam_step1_databento.py` | Confirmed: same state transitions, same CAGR as pre-refactor |
| `vam_step2_databento.py` | Confirmed: same 7-state machine results |
| `vam_step3_databento.py` | Confirmed: same SPXU trade count and P&L |
| `vam_step4_svix_databento.py` | Confirmed: same SVIX trade logic |
| `vam_step4_combined_databento.py` | Final Value: $258,919.34 / CAGR: +20.06% / 32 trades (26 SPXU + 6 SVIX) — all phase-gate tests PASSED |

---

## What Ravi Must Confirm Before Go-Live

These are the 10 flags that need Ravi's explicit sign-off:

1. **RSI period (14)** — not in any transcript
2. **RSI re-buy threshold (60)** — not in any transcript
3. **2-day confirmation window** — exit confirmed, entry not confirmed
4. **Kill switch logic (OR vs AND)** — Ravi never specified
5. **Re-entry VIX threshold (30)** — conflicting: transcript says both 30 and 60
6. **50-SMA re-entry gate** — IFA addition, not client-specified
7. **SPXU exit conditions** — VIX < 30 and SPY reclaims 50-SMA — not discussed
8. **SGOV for idle cash** — not mentioned by Ravi
9. **SVIX (entire Strategy 3)** — all parameters are IFA assumptions
10. **Commission model (IBKR tiered)** — not discussed

---

## Repos Updated

- `~/Projects/IFA-Perfect` (main)
- `https://github.com/insightfusionanalytics/ravi-vam-audit.git` (auditor copy)
