> **Ameesh: Start with [AUDIT_GUIDE.md](AUDIT_GUIDE.md) — it tells you what to read, in what order, and what to skip.**

# Ravi VAM Strategy — Full Auditor Package

## What's Inside
- **scripts/** — All backtest Python code (Steps 1-4 + parameter runner)
- **data/** — Raw DataBento CSVs (SPY, UPRO, QQQ, TQQQ, SPXU, SVIX, SGOV, VIX)
- **results/** — All output files (trade logs, portfolio values, metrics, charts)
- **audit_logs/** — Logs from 5 independent build/audit sessions
- **docs/** — Strategy explanation PDF, audit checklist PDF, proposal, execution prompts

## How to Audit
1. Read docs/IFA_Ravi_Proposal_v2.pdf — the client contract
2. Read docs/VAM_Strategy_Explanation.pdf — full strategy spec
3. Read docs/AUDIT_CHECKLIST.pdf — step-by-step verification guide
4. Review code in scripts/ line by line
5. Re-run each script and compare to results/
6. Read audit_logs/ to see what was already checked

## Quick Start
pip install pandas numpy reportlab openpyxl
cd scripts && python vam_step1_databento.py

## Steps Built
- Step 1: UPRO only (4-state) — vam_step1_databento.py
- Step 2: UPRO+TQQQ 75/25 (7-state) — vam_step2_databento.py
- Step 3: Predatory Short SPXU — vam_step3_databento.py
- Step 4: Safety Valve SVIX + combined portfolio — vam_step4_svix_databento.py + vam_step4_combined_databento.py
- Step 5: Excel Parameter Configurator — vam_parameter_runner.py + vam_parameters.xlsx
