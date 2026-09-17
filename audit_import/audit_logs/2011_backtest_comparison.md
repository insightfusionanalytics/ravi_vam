# VAM Backtest Comparison — 2020-2025 vs 2011-2025
**Generated:** 2026-05-03  
**2020-2025 period:** 2020-10-16 -> 2025-12-30 (5.2y)  
**2011-2025 period:** 2011-10-18 -> 2025-12-30 (14.2y)  

**Data sources:**
- 2020-2025: DataBento equities CSVs (existing, split-adjusted in-memory)
- 2011-2025: Polygon REST API for 2011-2019 + DataBento for 2020-2025, merged into `data/merged/*_daily_full.csv`
- VIX: DataBento (already covers 1990+); Polygon I:VIX returned 0 bars on free tier
- SVIX (Option C): real data only from 2022-03-30 — no synthetic pre-2022 SVIX

## Results

| Metric | 2020-2025 | 2011-2025 |
|--------|-----------|-----------|
| Step 1 Final Value | $262,698 | $574,388 |
| Step 1 CAGR (%) | +20.39 | +13.10 |
| Step 1 Sharpe | 0.637 | 0.431 |
| Step 1 Max DD (%) | -48.43 | -51.03 |
| Step 2 Final Value | $263,058 | $914,063 |
| Step 2 CAGR (%) | +20.42 | +17.15 |
| Step 2 Sharpe | 0.618 | 0.540 |
| Step 2 Max DD (%) | -50.09 | -50.09 |
| Step 3 Final Value | $86,661 | $62,946 |
| Step 3 CAGR (%) | -2.71 | -3.21 |
| Step 3 Sharpe | -0.459 | -0.446 |
| Step 3 Max DD (%) | -17.81 | -48.27 |
| Step 4 SVIX Final | $109,808 | $109,808 |
| Step 4 SVIX CAGR (%) | +1.57 | +0.63 |
| Step 4 SVIX Sharpe | -1.343 | -2.957 |
| Combined Final Value | $251,143 | $653,002 |
| Combined CAGR (%) | +19.35 | +14.13 |
| Combined Sharpe | 0.573 | 0.445 |
| Combined Max DD (%) | -50.88 | -53.52 |

## Notes

- **SPXU scale factor:** Polygon SPXU prices were rescaled by 0.002489 to align with the DataBento price basis (the two providers apply reverse-split adjustments differently). Verified at the 2018-05-01 overlap day.
- **UPRO/TQQQ:** DataBento raw prices have the existing forward-split adjustments applied at merge time; Polygon prices arrive already adjusted.
- **SVIX layer (Option C):** before 2022-03-30 the Step 4 combined engine simply skips SVIX signals — no proxy, no synthetic series. SPXU still operates throughout.
- **Step 1 backtest start:** 2011-10-18 (after 200-day SMA warmup from 2011-01-03).
- **Step 2 backtest start:** 2012-01-06 (after warmup; QQQ Polygon data begins 2011-03-23).
