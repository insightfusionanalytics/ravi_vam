import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import pandas as pd
from app.engines import step2

DEFAULT2 = {'vixThreshold': 30, 'smaKill': 200, 'cooldown': 5, 'smaDef': 50, 'confirmDays': 2, 'defSell': 50, 'rsiPeriod': 14, 'rsiOB': 75, 'rsiRe': 60, 'rsiTrim': 25, 'uproSplit': 75, 'fullHistory': True}
r2 = step2.run(DEFAULT2)
trades = pd.DataFrame(r2['trades'])
trades['execution_date'] = pd.to_datetime(trades['execution_date'])

window = trades[(trades['execution_date'] >= '2021-11-22') & (trades['execution_date'] <= '2023-03-10')]
# collapse to one row per state transition (both UPRO/TQQQ trade rows share the same transition)
transitions = window.drop_duplicates(subset=['execution_date','state_from','state_to','trigger_reason'])
transitions = transitions[['execution_date','state_from','state_to','trigger_reason']].sort_values('execution_date')
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 140)
print(transitions.to_string(index=False))

print(f"\nTotal transitions in this window: {len(transitions)}")
print(f"Transitions caused by KILL (kill switch): {transitions['trigger_reason'].str.contains('KILL').sum()}")
print(f"Transitions caused by DEFENSIVE (trim): {transitions['trigger_reason'].str.contains('DEFENSIVE').sum()}")
print(f"Re-entries (any ->BULL_FULL): {(transitions['state_to']=='BULL_FULL').sum()}")

# Now check gaps between consecutive kill-switch firings specifically
kills = transitions[transitions['trigger_reason'].str.contains('KILL')]
if len(kills) > 1:
    gaps = kills['execution_date'].diff().dt.days.dropna()
    print(f"\nGaps (trading-calendar days) between consecutive KILL firings: {gaps.tolist()}")
