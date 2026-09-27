import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from app.engines import step2_killfix_variant
from app.optimize.folds import DEV_FOLDS, HOLDOUT

baseline_params = {
    'vixThreshold': 30, 'smaKill': 200, 'smaDef': 50, 'rsiOB': 75, 'rsiRe': 60,
    'confirmDays': 2, 'cooldown': 2, 'defSell': 75, 'rsiPeriod': 24, 'rsiTrim': 50, 'uproSplit': 10,
    'killBufferBps': 0, 'killConfirmDays': 1,
}
variant_params = dict(baseline_params)
variant_params.update({'killBufferBps': 75, 'killConfirmDays': 4})

print(f"{'fold':>22s} {'base CAGR':>10s} {'fix CAGR':>10s} {'base Calmar':>12s} {'fix Calmar':>11s} {'base MaxDD':>11s} {'fix MaxDD':>10s}")
for fold_start, fold_end in DEV_FOLDS:
    p_b = {**baseline_params, 'startDate': fold_start, 'endDate': fold_end, 'fullHistory': True}
    p_v = {**variant_params, 'startDate': fold_start, 'endDate': fold_end, 'fullHistory': True}
    rb = step2_killfix_variant.run(p_b)['metrics']
    rv = step2_killfix_variant.run(p_v)['metrics']
    print(f"{fold_start}/{fold_end:>10s} {rb['cagr_pct']:10.2f} {rv['cagr_pct']:10.2f} {rb['calmar']:12.3f} {rv['calmar']:11.3f} {rb['max_drawdown_pct']:11.2f} {rv['max_drawdown_pct']:10.2f}")

print(f"\n{'='*70}\nSEALED HOLDOUT ({HOLDOUT[0]} to {HOLDOUT[1]}) -- ONE LOOK\n{'='*70}")
p_b_h = {**baseline_params, 'startDate': HOLDOUT[0], 'endDate': HOLDOUT[1], 'fullHistory': True}
p_v_h = {**variant_params, 'startDate': HOLDOUT[0], 'endDate': HOLDOUT[1], 'fullHistory': True}
rb_h = step2_killfix_variant.run(p_b_h)
rv_h = step2_killfix_variant.run(p_v_h)
mb, mv = rb_h['metrics'], rv_h['metrics']
print(f"baseline (today's confirmed-locked tuned config): CAGR {mb['cagr_pct']:.2f}%  Sharpe {mb['sharpe']:.3f}  Calmar {mb['calmar']:.3f}  MaxDD {mb['max_drawdown_pct']:.2f}%  Trades {mb['total_trades']}")
print(f"with kill-switch fix (buffer=75bps, confirm=4d):  CAGR {mv['cagr_pct']:.2f}%  Sharpe {mv['sharpe']:.3f}  Calmar {mv['calmar']:.3f}  MaxDD {mv['max_drawdown_pct']:.2f}%  Trades {mv['total_trades']}")

# whipsaw check on holdout
import pandas as pd
def whipsaw_stats(daily_log):
    d = pd.DataFrame(daily_log)
    states = d['state'].tolist()
    prices = d['upro_close'].tolist()
    runs = []
    i = 0
    while i < len(states):
        if states[i] == 'CASH':
            j = i
            while j < len(states) and states[j] == 'CASH':
                j += 1
            runs.append((i, j))
            i = j
        else:
            i += 1
    short = [(i,j) for i,j in runs if (j-i) <= 10]
    false_alarms = sum(1 for i,j in short if prices[min(j,len(prices)-1)] > prices[i])
    return len(runs), len(short), false_alarms

nb, sb, fb = whipsaw_stats(rb_h['daily_log'])
nv, sv, fv = whipsaw_stats(rv_h['daily_log'])
print(f"\nKill-switch (CASH) episodes on holdout: baseline {nb} total ({sb} short, {fb} false alarms) -> fixed {nv} total ({sv} short, {fv} false alarms)")

Path('/Users/chirag/ravi_vam/scripts/_tmp_analysis/killfix_holdout_result.json').write_text(json.dumps({'baseline': mb, 'variant': mv}, indent=2, default=str))
