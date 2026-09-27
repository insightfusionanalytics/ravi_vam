import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import app.engines.step2 as step2
import app.engines.step2_killfix_variant as variant

DEV = {'startDate': '2011-01-01', 'endDate': '2021-12-31', 'fullHistory': True}
BASE = {'vixThreshold': 30, 'smaKill': 200, 'cooldown': 5, 'smaDef': 50, 'confirmDays': 2, 'defSell': 50, 'rsiPeriod': 14, 'rsiOB': 75, 'rsiRe': 60, 'rsiTrim': 25, 'uproSplit': 75}

def run(engine, extra):
    p = {**BASE, **DEV, **extra}
    return engine.run(p)

def whipsaw_stats(daily_log, cash_state='CASH', trim_states=('DEF_SPY','DEF_QQQ','DEF_BOTH')):
    import pandas as pd
    d = pd.DataFrame(daily_log)
    prices = d['upro_close'].tolist()
    states = d['state'].tolist()
    all_states = {cash_state} | set(trim_states)
    runs = []
    i = 0
    while i < len(states):
        if states[i] in all_states:
            j = i
            while j < len(states) and states[j] == states[i]:
                j += 1
            runs.append((states[i], i, j))
            i = j
        else:
            i += 1
    kill_runs = [(s,i,j) for s,i,j in runs if s == cash_state]
    short_kill = [(s,i,j) for s,i,j in kill_runs if (j-i) <= 10]
    false_alarms = sum(1 for s,i,j in short_kill if prices[min(j,len(prices)-1)] > prices[i])
    return len(kill_runs), len(short_kill), false_alarms

print(f"{'variant':45s} {'CAGR':>8s} {'Calmar':>8s} {'MaxDD':>8s} {'Trades':>7s}  {'kill-eps':>8s} {'short':>6s} {'falsealm':>8s}")

r0 = run(step2, {})
n,s,f = whipsaw_stats(r0['daily_log'])
m = r0['metrics']
print(f"{'BASELINE (today)':45s} {m['cagr_pct']:8.2f} {m['calmar']:8.3f} {m['max_drawdown_pct']:8.2f} {m['total_trades']:7d}  {n:8d} {s:6d} {f:8d}")

for buf, conf in [(0.5, 2), (1.0, 2), (1.5, 2), (1.0, 3), (2.0, 2)]:
    r = run(variant, {'killBufferPct': buf, 'killConfirmDays': conf})
    n,s,f = whipsaw_stats(r['daily_log'])
    m = r['metrics']
    label = f"A: buffer={buf}% confirm={conf}d"
    print(f"{label:45s} {m['cagr_pct']:8.2f} {m['calmar']:8.3f} {m['max_drawdown_pct']:8.2f} {m['total_trades']:7d}  {n:8d} {s:6d} {f:8d}")

for immun in [3, 5, 10, 15]:
    r = run(variant, {'killReentryImmunityDays': immun})
    n,s,f = whipsaw_stats(r['daily_log'])
    m = r['metrics']
    label = f"B: reentry-immunity={immun}d"
    print(f"{label:45s} {m['cagr_pct']:8.2f} {m['calmar']:8.3f} {m['max_drawdown_pct']:8.2f} {m['total_trades']:7d}  {n:8d} {s:6d} {f:8d}")

# combined
for buf, conf, immun in [(1.0, 2, 5), (1.0, 2, 10)]:
    r = run(variant, {'killBufferPct': buf, 'killConfirmDays': conf, 'killReentryImmunityDays': immun})
    n,s,f = whipsaw_stats(r['daily_log'])
    m = r['metrics']
    label = f"C: buffer={buf}% confirm={conf}d + immun={immun}d"
    print(f"{label:45s} {m['cagr_pct']:8.2f} {m['calmar']:8.3f} {m['max_drawdown_pct']:8.2f} {m['total_trades']:7d}  {n:8d} {s:6d} {f:8d}")
