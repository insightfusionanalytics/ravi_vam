# Ravi VAM Strategy — Audit Guide for Ameesh

Start here. Read in this order.

---

## 1. Strategy Context — 10 min
- `docs/VAM_Strategy_Explanation.pdf` — Understand what VAM does before touching any code
- `docs/AUDIT_CHECKLIST.pdf` — Your verification checklist; keep it open throughout

## 2. Audit Logs (What Was Already Checked) — 15 min
- `audit_logs/mathematical_audit.md` — Math already verified; check for any FAIL items
- `audit_logs/data_quality_audit.md` — Data integrity check; note any flags
- `audit_logs/chat5_final_audit_log.md` — Final session summary before delivery
- Skip: `chat1_` through `chat4_` logs — build history, not findings

## 3. Code Review — 30 min
- `scripts/vam_step1_databento.py` — Baseline UPRO strategy (4-state machine) — read first
- `scripts/vam_step2_databento.py` — UPRO+TQQQ (7-state machine) — compare to Step 1
- `scripts/vam_step3_databento.py` — Predatory Short SPXU logic
- `scripts/vam_step4_combined_databento.py` — Combined portfolio output
- `scripts/vam_parameter_runner.py` — Parameter sweep runner
- Skip: `*-Anmol's MacBook Pro.py` files — duplicates with path differences only

## 4. Results Verification — 20 min
- `results/step1_databento_metrics.json` — Key performance numbers for Step 1
- `results/step2_databento_metrics.json` — Key performance numbers for Step 2
- `results/step1_databento_trade_log.csv` — Spot-check 10 random trades vs code logic
- `results/EXECUTIVE_SUMMARY_databento.txt` — Client-facing numbers; verify they match metrics JSONs
- Skip: `results/strategy_sweep_*` and `results/vam_15yr_*` — exploratory only, not in final delivery

## 5. Parameter Config — 5 min
- `Step_5_Parameter_Config/vam_parameters.xlsx` — Confirm all parameters match what's in the scripts

## 6. Delivery Check — 5 min
- `delivery/backtest_results/` — Confirm interactive charts render and display correct step metrics

---

## What to Skip
- `data/` CSVs — source data, no code issues here
- `scripts/generate_*.py` — output formatters, not strategy logic
- `scripts/vam_matrix_crosscheck.py` — internal verification tool, already used in audit logs

---

## What I Need Back

1. PASS or FAIL on the mathematical audit — specifically: do annual returns compound to the total return?
2. Any code issues in Step 2 (7-state machine) not caught in `mathematical_audit.md`
3. Confirm: does the TQQQ split date in the code match any corporate actions record you can verify?
4. Flag any parameter in `vam_parameters.xlsx` that does not match the hardcoded defaults in the scripts
