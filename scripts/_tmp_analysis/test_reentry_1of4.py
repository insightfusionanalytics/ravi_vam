import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import numpy as np
from app.engines import step2_reentry_variant
from app.optimize.folds import DEV_FOLDS
from app.optimize.objective import run_fold

baseline_params = {
    'vixThreshold': 30, 'smaKill': 200, 'smaDef': 50, 'rsiOB': 75, 'rsiRe': 60,
    'confirmDays': 2, 'cooldown': 2, 'defSell': 75, 'rsiPeriod': 24, 'rsiTrim': 50, 'uproSplit': 10,
}

print(f"{'reentryMinConditions':>22s} {'fold':>22s} {'CAGR':>8s} {'Calmar':>8s} {'MaxDD':>8s} {'Trades':>7s}")
for n in [1, 2, 3, 4]:
    p = {**baseline_params, 'reentryMinConditions': n}
    scores = []
    for fold_start, fold_end in DEV_FOLDS:
        r = run_fold(step2_reentry_variant, p, fold_start, fold_end)
        m = r['metrics']
        scores.append(m['calmar'])
        print(f"{n:22d} {fold_start}/{fold_end:>10s} {m['cagr_pct']:8.2f} {m['calmar']:8.3f} {m['max_drawdown_pct']:8.2f} {m['total_trades']:7d}")
    med = np.median(scores)
    iqr = np.percentile(scores,75)-np.percentile(scores,25)
    print(f"  -> median={med:.3f} iqr={iqr:.3f} objective(median-0.5iqr)={med-0.5*iqr:.4f}\n")

full_dev = {'startDate': '2011-01-01', 'endDate': '2021-12-31', 'fullHistory': True}
print("Full dev period (2011-2021), continuous:")
for n in [1, 2, 3, 4]:
    r = step2_reentry_variant.run({**baseline_params, 'reentryMinConditions': n, **full_dev})
    m = r['metrics']
    print(f"  reentryMinConditions={n}: CAGR {m['cagr_pct']:6.2f}%  Calmar {m['calmar']:.3f}  MaxDD {m['max_drawdown_pct']:7.2f}%  Trades {m['total_trades']}")
