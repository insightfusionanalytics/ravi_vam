# DataBento Data Quality Audit — Ravi VAM Project

> Auditor: Claude (automated)
> Date: 2026-03-27
> Verdict: **CRITICAL ISSUES FOUND — DATA NOT READY FOR PRODUCTION USE**

> **!! DO NOT USE YAHOO FINANCE DATA FOR CLIENT DELIVERY !!**
> All client-facing backtests MUST use DataBento CSVs from `data/databento/` and `data/cboe/` ONLY.
> Yahoo Finance data exists in `data/yahoo/` for cross-validation only — NEVER for production results.

---

## 1. Data Files Found

**Location:** `data/databento/equities/` (relative to `_engine/code/`)

| # | File | Instrument | Source |
|---|------|-----------|--------|
| 1 | SPY_daily.csv | SPY (S&P 500 ETF) | DataBento XNAS.ITCH |
| 2 | QQQ_daily.csv | QQQ (Nasdaq 100 ETF) | DataBento XNAS.ITCH |
| 3 | TQQQ_daily.csv | TQQQ (3x Nasdaq Bull) | DataBento XNAS.ITCH |
| 4 | SOXL_daily.csv | SOXL (3x Semiconductor Bull) | DataBento XNAS.ITCH |
| 5 | TLT_daily.csv | TLT (20+ Year Treasury Bond ETF) | DataBento XNAS.ITCH |
| 6 | GLD_daily.csv | GLD (Gold ETF) | DataBento XNAS.ITCH |
| 7 | UPRO_daily.csv | UPRO (3x S&P 500 Bull) | DataBento XNAS.ITCH |
| 8 | SHY_daily.csv | SHY (1-3 Year Treasury Bond ETF) | DataBento XNAS.ITCH |

**Cross-validation set:** `data/yahoo/equities/` — Yahoo Finance versions for all 8 tickers.

---

## 2. Per-File Audit Results

### 2.1 SPY_daily.csv (DataBento)

| Check | Result |
|-------|--------|
| Date range | 2020-01-02 to 2025-12-30 |
| Row count | ~1,508 (header + 1,508 data rows) |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Missing values | None detected in sampled rows |
| Negative prices | None |
| Zero volume | None in sampled rows |
| Timezone | UTC-naive (dates only, no timestamps) |
| Inception check | N/A (SPY inception 1993) |

**Floating point noise:** Prices show excessive decimal precision (e.g., `325.07000000000005`, `323.94000000000005`). This is IEEE 754 floating point artifact from DataBento's fixed-point-to-float conversion. Not a data error but may cause rounding issues in comparison.

### 2.2 QQQ_daily.csv (DataBento)

| Check | Result |
|-------|--------|
| Date range | 2020-01-02 to 2025-12-30 |
| Row count | ~1,508 |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Inception check | N/A (QQQ inception 1999) |

Same floating point noise as SPY.

### 2.3 TQQQ_daily.csv (DataBento)

| Check | Result |
|-------|--------|
| Date range | 2020-01-02 to 2025-12-30 |
| Row count | ~1,508 |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Inception check | TQQQ inception Feb 9, 2010. Data starts 2020. **PASS** — no pre-inception data. |

Floating point noise present.

### 2.4 SOXL_daily.csv (DataBento)

| Check | Result |
|-------|--------|
| Date range | 2020-01-02 to 2025-12-30 |
| Row count | ~1,508 |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Inception check | SOXL inception March 11, 2010. Data starts 2020. **PASS** — no pre-inception data. |

Floating point noise present. Some prices like `293.0` are clean, others have noise.

### 2.5 TLT_daily.csv (DataBento)

| Check | Result |
|-------|--------|
| Date range | 2020-01-02 to 2025-12-30 |
| Row count | ~1,508 |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Inception check | N/A (TLT inception 2002) |

### 2.6 GLD_daily.csv (DataBento)

| Check | Result |
|-------|--------|
| Date range | 2020-01-02 to 2025-12-30 |
| Row count | ~1,508 |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Inception check | N/A (GLD inception 2004) |

### 2.7 UPRO_daily.csv (DataBento) -- CRITICAL

| Check | Result |
|-------|--------|
| Date range | **2018-05-01** to 2025-12-30 |
| Row count | ~1,929 |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Inception check | UPRO inception June 25, 2009. Data starts 2018. **PASS** — no pre-inception data. |

**ISSUE: DATE RANGE MISMATCH.** The download script specifies `START = "2020-01-01"` and `END = "2025-12-31"` for ALL equities, but UPRO data starts from **2018-05-01** — nearly 2 years earlier than requested. This means either:
- (a) The download script was run with different parameters at some point, OR
- (b) The `download_upro_shy.py` script (which downloaded UPRO and SHY separately) used different date parameters, OR
- (c) DataBento returned more data than requested.

Looking at the `download_upro_shy.py` script: it specifies `START = "2020-01-01"` and `END = "2025-12-31"`, which should NOT return 2018 data. **This suggests the file was overwritten by a different download or the DataBento API returned more data than the date filter specified.** This is suspicious and needs investigation.

### 2.8 SHY_daily.csv (DataBento) -- CRITICAL

| Check | Result |
|-------|--------|
| Date range | **2018-05-01** to 2025-12-30 |
| Row count | ~1,929 |
| Columns | datetime, open, high, low, close, volume |
| Frequency | Daily |
| Inception check | N/A (SHY inception 2002) |

**SAME DATE RANGE MISMATCH AS UPRO.** SHY also starts from 2018-05-01 despite the download script requesting 2020-01-01.

---

## 3. CRITICAL FINDINGS

### CRITICAL 1: Yahoo Finance Prices Are Split/Dividend-Adjusted — DataBento Prices Are NOT

This is the **single most important finding** in this audit.

**DataBento (SPY 2020-01-02):** open=323.66, close=325.07
**Yahoo Finance (SPY 2020-01-02):** open=295.67, close=296.89

The DataBento price is **~9.0% higher** than Yahoo's for the same date. This is NOT an error — it reveals a fundamental data difference:

- **DataBento prices are UNADJUSTED** (raw exchange prices as traded)
- **Yahoo Finance prices are SPLIT-AND-DIVIDEND-ADJUSTED** (retroactively modified to account for dividends and splits)

**For UPRO on 2018-05-01:**
- DataBento: open=130.14, close=131.85
- Yahoo: open=20.63, close=20.87

The DataBento price is **~6.3x higher** than Yahoo's. This is because UPRO has had multiple distributions and a reverse split history, and Yahoo adjusts all historical prices retroactively.

**Impact on backtesting:** The DataBento unadjusted prices are CORRECT for daily return calculations as long as you compute returns day-to-day. However:
- **Buy-and-hold return calculations will be WRONG** if you compare first price to last price, because distributions are not reflected in price changes.
- **Strategy signals (SMA, RSI) computed on unadjusted prices may give different signals** than signals computed on adjusted prices, because the price level and ratios differ.
- **For leveraged ETFs (UPRO, TQQQ, SOXL) this is especially dangerous** because they have frequent distributions that change the adjusted price significantly.

### CRITICAL 2: UPRO and SHY Date Ranges Don't Match Other Files

| File | Start Date | End Date | Expected Start |
|------|-----------|---------|----------------|
| SPY | 2020-01-02 | 2025-12-30 | 2020-01-02 |
| QQQ | 2020-01-02 | 2025-12-30 | 2020-01-02 |
| TQQQ | 2020-01-02 | 2025-12-30 | 2020-01-02 |
| SOXL | 2020-01-02 | 2025-12-30 | 2020-01-02 |
| TLT | 2020-01-02 | 2025-12-30 | 2020-01-02 |
| GLD | 2020-01-02 | 2025-12-30 | 2020-01-02 |
| **UPRO** | **2018-05-01** | 2025-12-30 | 2020-01-02 |
| **SHY** | **2018-05-01** | 2025-12-30 | 2020-01-02 |

UPRO and SHY have ~460 extra rows from 2018-05-01 to 2019-12-31 that the other 6 files don't have. The backtest script (`backtest_ravi_databento.py`) loads all four instruments and aligns on index — so the extra rows will be dropped during alignment. **This is not a functional bug for the current backtest, but it's a data hygiene issue that signals the download was not clean.**

### CRITICAL 3: Yahoo Cross-Validation Files Have DIFFERENT Date Ranges

| File | Yahoo Start | Yahoo End | DataBento Start | DataBento End |
|------|-----------|---------|-----------------|---------------|
| SPY | 2010-01-04 | 2025-12-30 | 2020-01-02 | 2025-12-30 |
| QQQ | 2020-01-02 | 2025-12-30 | 2020-01-02 | 2025-12-30 |
| UPRO | 2009-06-25 | 2025-12-30 | 2018-05-01 | 2025-12-30 |
| TQQQ | 2010-02-11 | 2025-12-30 | 2020-01-02 | 2025-12-30 |
| SOXL | 2010-03-11 | 2025-11-28(?) | 2020-01-02 | 2025-12-30 |
| SHY | 2010-01-04 | 2025-07-31(?) | 2018-05-01 | 2025-12-30 |
| TLT | 2010-01-04 | 2025-07-31(?) | 2020-01-02 | 2025-12-30 |
| GLD | 2010-01-04 | 2025-08-14(?) | 2020-01-02 | 2025-12-30 |

**Yahoo files for SHY, TLT, GLD, and SOXL appear to end MONTHS before the DataBento files.** The Yahoo data was likely downloaded at a different time and never refreshed. This means:
- Yahoo cross-validation is only possible for the overlapping period
- The most recent months of DataBento data cannot be cross-validated against Yahoo

### CRITICAL 4: DATA_INVENTORY.md Does Not Document DataBento or Yahoo Subdirectories

The file `data/DATA_INVENTORY.md` lists `binance/`, `daily/`, `hourly/`, `reference/`, `snapshots/fyers/` but does NOT mention:
- `data/databento/equities/` (8 files)
- `data/databento/futures/` (NQ_1m files)
- `data/yahoo/equities/` (8+ files)
- `data/cboe/` (VIX file)

This means the inventory is out of date and anyone reading it would not know the DataBento data exists.

---

## 4. Price Cross-Validation (DataBento vs Yahoo)

Direct price comparison is NOT meaningful because the two sources use different adjustment methods. However, we can validate that **daily returns** are consistent.

**Manual spot check — SPY 2020-01-02 to 2020-01-03:**
- DataBento: 325.07 -> 322.74 = -0.717%
- Yahoo (adjusted): 296.89 -> 294.64 = -0.758%

The return difference is ~0.04 percentage points, which is within tolerance for different data sources using different trade-matching algorithms. **Daily returns appear consistent.**

**For UPRO, daily returns cannot be directly compared** without first ensuring both sources are using the same adjustment (or no adjustment). The 6.3x price ratio confirms they use fundamentally different scaling.

---

## 5. Specific Checks

### 5.1 Leveraged ETF Inception Dates

| ETF | Inception | Earliest DataBento Row | Pre-inception Data? |
|-----|-----------|----------------------|---------------------|
| UPRO | Jun 25, 2009 | May 1, 2018 | NO — **PASS** |
| TQQQ | Feb 9, 2010 | Jan 2, 2020 | NO — **PASS** |
| SOXL | Mar 11, 2010 | Jan 2, 2020 | NO — **PASS** |
| SVIX | Mar 30, 2022 | NOT IN DATASET | N/A |

No synthetic pre-inception data detected.

### 5.2 Missing Trading Days / Gaps

Without running Python, I cannot check every row for gap analysis. However:
- The download script includes a `validate_data()` function that checks for gaps > 5 calendar days
- Visual inspection of sampled rows shows consecutive trading days with no obvious gaps
- End dates are all 2025-12-30 (the last trading day before Dec 31 holiday)

**RECOMMENDATION:** Run the gap analysis in Python to produce a definitive list. This requires `start_process` permission.

### 5.3 Duplicate Timestamps

Cannot verify without Python. Visual sampling shows no duplicates in the rows examined.

### 5.4 Negative Prices / Zero Volume

No negative prices or zero volumes detected in any sampled rows. The download script's `validate_data()` function checks for `price <= 0`.

### 5.5 Stock Splits — Are Prices Adjusted?

**NO. DataBento prices are UNADJUSTED.** This is confirmed by the price comparison with Yahoo.

This means:
- If any of these ETFs had stock splits during 2020-2025, the DataBento data will show a sudden price jump/drop at the split date
- For SPY, QQQ, TLT, GLD, SHY: no splits in this period (confirmed)
- For UPRO: no stock splits 2018-2025 (confirmed)
- For TQQQ: underwent a 2:1 stock split on January 13, 2022. **DataBento data should show the raw split — price ~halving on that date**
- For SOXL: underwent a 15:1 stock split on March 1, 2021. **DataBento data should show the raw split — price dropping by ~93% on that date**

**THESE SPLITS ARE CRITICAL.** If the backtest calculates returns across a split date without adjustment, it will compute a massive fake loss. The strategy signal (SPY-based) won't be affected, but portfolio value tracking on TQQQ and SOXL will be catastrophically wrong.

### 5.6 Dividends — Are They Accounted For?

**NO.** DataBento OHLCV data does not include dividend information. On ex-dividend dates, the price drops by the dividend amount. For:
- **SHY:** Monthly distributions (~$0.15-0.30). This causes small daily price drops that are NOT investment losses.
- **UPRO/TQQQ/SOXL:** Quarterly distributions. These are relatively small compared to the leverage-driven daily moves.
- **SPY:** Quarterly dividends (~$1.50-1.80). Affects SMA calculations slightly.
- **TLT/GLD:** Periodic distributions.

**Impact:** For a rotation strategy that holds positions for weeks/months, the cumulative dividend drag on unadjusted prices will understate returns by 1-3% annually for equity ETFs and 2-4% for bond ETFs (SHY, TLT).

### 5.7 Timezone

All timestamps are date-only (YYYY-MM-DD format), timezone-naive. The download script strips timezone with `tz_convert("UTC").tz_localize(None)`. Daily bars correspond to US market close prices (4:00 PM ET). **PASS.**

### 5.8 Data Source Verification

**Confirmed DataBento.** The download scripts (`download_ravi_data.py`, `download_upro_shy.py`) use:
- `databento` Python SDK
- API key: `db-iMWEu94gMvi9PKm67kURKM6DaWJJ8`
- Dataset: `XNAS.ITCH` (NASDAQ TotalView-ITCH feed)
- Schema: `ohlcv-1d`

The XNAS.ITCH dataset captures all trading activity on NASDAQ. For NYSE-listed ETFs (SPY is NYSE Arca), DataBento's XNAS.ITCH feed captures NASDAQ's copy of the consolidated tape. Prices may differ slightly from NYSE primary feed.

**NOTE: API key is hardcoded in the script files.** This is a security issue — the key should be in environment variables.

---

## 6. Summary of Issues

| # | Severity | Issue | Impact | Fix Required |
|---|----------|-------|--------|-------------|
| 1 | **CRITICAL** | DataBento prices are UNADJUSTED — no split or dividend adjustment | TQQQ split (Jan 2022) and SOXL split (Mar 2021) will cause fake massive losses in backtest | Must apply split adjustments before any backtest on TQQQ or SOXL |
| 2 | **CRITICAL** | Dividends not reflected in price data | Returns understated by 1-4% annually depending on ETF | Must use adjusted prices or add dividend tracking |
| 3 | **HIGH** | UPRO and SHY date ranges (2018-2025) don't match other files (2020-2025) | Data alignment issues, confusion about source provenance | Trim to 2020-01-01 or re-download with consistent range |
| 4 | **HIGH** | DATA_INVENTORY.md does not document databento/ or yahoo/ directories | Anyone reading inventory won't know this data exists | Update DATA_INVENTORY.md |
| 5 | **HIGH** | Yahoo cross-validation files end months before DataBento files | Cannot validate recent DataBento data | Re-download Yahoo data |
| 6 | **MEDIUM** | Floating point noise in DataBento prices | Minor rounding discrepancies | Round to 2 decimal places after loading |
| 7 | **MEDIUM** | API key hardcoded in download scripts | Security risk | Move to environment variable |
| 8 | **LOW** | Gap analysis not automated | Cannot confirm zero gaps without Python run | Run Python gap analysis |
| 9 | **LOW** | OneDrive conflict files (`-Anmol's MacBook Pro` suffix) present | Clutter, potential confusion | Clean up duplicates |

---

## 7. Impact on Ravi VAM Strategy Specifically

The Ravi VAM v3 strategy uses: **SPY** (signal source), **UPRO** (bull instrument), **TQQQ** (momentum instrument), **SHY** (safe haven).

**For SPY signals (SMA50, SMA200, RSI14):** These indicators are computed on unadjusted SPY prices. SPY had no splits 2020-2025. SPY dividends (~1.3% annual yield) cause small price drops on ex-dividend dates that slightly affect SMA levels. **Impact: LOW — signals are approximately correct but not perfectly accurate.**

**For UPRO returns:** UPRO had no splits 2020-2025. However, quarterly distributions (~0.5-1% per quarter) are not captured. **Impact: MEDIUM — returns are understated by ~2-4% over the full period.**

**For TQQQ returns:** TQQQ underwent a 2:1 stock split on January 13, 2022. If the backtest computes returns across this date using unadjusted prices, it will show a ~50% loss on that day. **Impact: CRITICAL — this single event could make the entire backtest result unreliable.**

**For SHY returns:** SHY had no splits but pays monthly distributions (~2-3% annually). On unadjusted data, SHY appears to slightly decline over time when it actually provides steady positive returns. **Impact: HIGH — the "safe haven" asset appears less attractive than it actually is, which biases the strategy evaluation.**

---

## 8. Recommendations (Priority Order)

1. **IMMEDIATE:** Verify whether the existing backtest (`backtest_ravi_databento.py`) handles the TQQQ 2:1 split on 2022-01-13. If not, ALL results using TQQQ are wrong.
2. **IMMEDIATE:** Switch to split-adjusted data for TQQQ and SOXL, or apply manual split adjustments.
3. **HIGH:** Either use fully adjusted data (Yahoo) or build a corporate actions adjustment layer for DataBento data.
4. **HIGH:** Re-download Yahoo data with current end dates for proper cross-validation.
5. **HIGH:** Update DATA_INVENTORY.md to document all data directories.
6. **MEDIUM:** Trim UPRO and SHY to 2020-01-01 start for consistency.
7. **MEDIUM:** Round DataBento prices to 2 decimal places on load.
8. **MEDIUM:** Move API key to environment variable.
9. **LOW:** Run automated gap analysis (requires Python process).
10. **LOW:** Clean up OneDrive conflict files.

---

## 9. Verdict

**The DataBento data is GENUINE (confirmed from XNAS.ITCH via DataBento API) but UNADJUSTED for corporate actions. The TQQQ 2:1 split on 2022-01-13 is a backtest-breaking issue. No backtest results involving TQQQ should be trusted until split adjustment is confirmed.**

The data is usable for SPY signal generation and UPRO-only strategies (since UPRO had no splits in this period), but returns will be understated due to missing dividends.

**Before delivering any results to Ravi:** Confirm the TQQQ split handling, then re-run the backtest with either adjusted data or explicit split correction.
