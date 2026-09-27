import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import app.engines.step2_killfix_variant as variant

DEV = {'startDate': '2011-01-01', 'endDate': '2021-12-31', 'fullHistory': True}
BASE = {'vixThreshold': 30, 'smaKill': 200, 'cooldown': 5, 'smaDef': 50, 'confirmDays': 2, 'defSell': 50, 'rsiPeriod': 14, 'rsiOB': 75, 'rsiRe': 60, 'rsiTrim': 25, 'uproSplit': 75}

print(f"{'buffer%':>8s} {'confirm':>8s} {'CAGR':>8s} {'Calmar':>8s} {'MaxDD':>8s} {'Trades':>7s}")
for buf in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
    for conf in [2, 3, 4]:
        p = {**BASE, **DEV, 'killBufferPct': buf, 'killConfirmDays': conf}
        r = variant.run(p)
        m = r['metrics']
        print(f"{buf:8.1f} {conf:8d} {m['cagr_pct']:8.2f} {m['calmar']:8.3f} {m['max_drawdown_pct']:8.2f} {m['total_trades']:7d}")
