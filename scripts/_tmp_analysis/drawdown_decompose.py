import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
import pandas as pd, numpy as np
from app.engines import step2

DEFAULT2 = {'vixThreshold': 30, 'smaKill': 200, 'cooldown': 5, 'smaDef': 50, 'confirmDays': 2, 'defSell': 50, 'rsiPeriod': 14, 'rsiOB': 75, 'rsiRe': 60, 'rsiTrim': 25, 'uproSplit': 75, 'fullHistory': True}
r2 = step2.run(DEFAULT2)
daily = pd.DataFrame(r2['daily_log'])
daily['date'] = pd.to_datetime(daily['date'])
pv = daily.set_index('date')['portfolio_value']
states = daily.set_index('date')['state']
upro = daily.set_index('date')['upro_close']

episodes = [
    ("2021-11-22", "2023-03-10", "2024-03-20"),
    ("2018-01-29", "2019-08-05", "2020-02-11"),
    ("2015-05-22", "2016-06-27", "2017-03-01"),
    ("2012-04-03", "2012-06-25", "2013-04-10"),
    ("2020-02-20", "2020-07-24", "2020-12-04"),
    ("2024-07-17", "2024-09-06", "2025-09-15"),
]

for peak, trough, recov in episodes:
    peak, trough, recov = pd.Timestamp(peak), pd.Timestamp(trough), pd.Timestamp(recov)
    depth = (pv.loc[trough] - pv.loc[peak]) / pv.loc[peak] * 100

    phase1 = states.loc[peak:trough]  # peak -> trough
    phase2 = states.loc[trough:recov]  # trough -> recovery

    def count_reentries(s):
        arr = s.tolist()
        n = 0
        for k in range(1, len(arr)):
            if arr[k] == 'BULL_FULL' and arr[k-1] != 'BULL_FULL':
                n += 1
        return n

    def pct_days_bull_full(s):
        return (s == 'BULL_FULL').mean() * 100

    re1 = count_reentries(phase1)
    re2 = count_reentries(phase2)
    days1 = (trough - peak).days
    days2 = (recov - trough).days
    bullpct1 = pct_days_bull_full(phase1)
    bullpct2 = pct_days_bull_full(phase2)
    upro_trough_to_recov = (upro.loc[recov] - upro.loc[trough]) / upro.loc[trough] * 100
    strat_trough_to_recov = (pv.loc[recov] - pv.loc[trough]) / pv.loc[trough] * 100

    print(f"\n{peak.date()} -> trough {trough.date()} ({depth:.1f}%) -> recovered {recov.date()}")
    print(f"  PEAK->TROUGH ({days1}d): {re1} re-entries into BULL_FULL, {bullpct1:.0f}% of days fully invested")
    print(f"  TROUGH->RECOVERY ({days2}d): {re2} re-entries into BULL_FULL, {bullpct2:.0f}% of days fully invested")
    print(f"  During trough->recovery: raw UPRO rose {upro_trough_to_recov:+.1f}%, strategy portfolio rose {strat_trough_to_recov:+.1f}%")
