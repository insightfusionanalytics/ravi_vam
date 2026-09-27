import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import pandas as pd, numpy as np
from app.engines import step1, step2

DEFAULT1 = {'vixThreshold': 30, 'smaKill': 200, 'smaDef': 50, 'confirmDays': 2, 'rsiPeriod': 14, 'rsiOB': 75, 'rsiRe': 60, 'uproSplit': 100, 'fullHistory': True}
DEFAULT2 = {'vixThreshold': 30, 'smaKill': 200, 'cooldown': 5, 'smaDef': 50, 'confirmDays': 2, 'defSell': 50, 'rsiPeriod': 14, 'rsiOB': 75, 'rsiRe': 60, 'rsiTrim': 25, 'uproSplit': 75, 'fullHistory': True}

def episodes_exact_state(daily, states_of_interest):
    s = daily['state'].tolist()
    runs = []
    i = 0
    while i < len(s):
        if s[i] in states_of_interest:
            j = i
            while j < len(s) and s[j] == s[i]:
                j += 1
            runs.append((s[i], i, j))
            i = j
        else:
            i += 1
    return runs

def mechanism_breakdown(name, daily, cash_state, trim_states):
    prices = daily['upro_close'].tolist()
    all_states = {cash_state} | set(trim_states)
    runs = episodes_exact_state(daily, all_states)
    short = [(s,i,j) for (s,i,j) in runs if (j-i) <= 10]

    print(f"\n=== {name}: decomposing each whipsaw episode into 'decline before we act' vs 'recovery before we react' ===")
    pre_decline = []   # price move in the 5 days BEFORE exit (already happened before we sold)
    during_to_trough = []  # move from exit to the trough within the episode
    trough_to_reentry = []  # move from trough to re-entry (recovered while we still sat out)
    trough_position = []  # trough day index as fraction of episode length (0=at start, 1=at end)

    for s, i, j in short:
        lookback = max(0, i-5)
        pre = (prices[i]-prices[lookback])/prices[lookback]*100
        window = prices[i:min(j+1, len(prices))]
        if len(window) < 2:
            continue
        trough_idx = int(np.argmin(window))
        trough_price = window[trough_idx]
        exit_price = window[0]
        reentry_price = window[-1]
        during1 = (trough_price - exit_price)/exit_price*100
        during2 = (reentry_price - trough_price)/trough_price*100
        pre_decline.append(pre)
        during_to_trough.append(during1)
        trough_to_reentry.append(during2)
        trough_position.append(trough_idx/(len(window)-1) if len(window)>1 else 0)

    print(f"  n={len(pre_decline)} short episodes")
    print(f"  Avg price move in the 5 days BEFORE exit (already lost before selling):  {np.mean(pre_decline):+.2f}%")
    print(f"  Avg move from exit price to the episode's trough (extra decline caught by exiting): {np.mean(during_to_trough):+.2f}%")
    print(f"  Avg move from trough back up to re-entry price (recovery missed by re-entry lag):  {np.mean(trough_to_reentry):+.2f}%")
    print(f"  Median trough position within episode (0=day of exit, 1=day of re-entry): {np.median(trough_position):.2f}")
    print(f"  Fraction of episodes where trough occurs in first half of episode: {np.mean([t<=0.5 for t in trough_position])*100:.0f}%")

r1 = step1.run(DEFAULT1)
d1 = pd.DataFrame(r1['daily_log'])
mechanism_breakdown("STEP 1 -- UPRO Only", d1, cash_state='CASH', trim_states=['DEFENSIVE'])

r2 = step2.run(DEFAULT2)
d2 = pd.DataFrame(r2['daily_log'])
mechanism_breakdown("STEP 2 -- UPRO+TQQQ (flagship)", d2, cash_state='CASH', trim_states=['DEF_SPY','DEF_QQQ','DEF_BOTH'])
