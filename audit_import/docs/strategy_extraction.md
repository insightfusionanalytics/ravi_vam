# Ravi VAM Strategy Extraction — Definitive Source of Truth
**Extracted from: feb10_strategy_discussion.srt + mar1_meeting.srt**
**Extracted by: IFA — April 2026**
**Purpose: Every parameter below is cited to transcript timestamp and exact quote. No assumptions.**

---

## LEGEND
- ✅ CONFIRMED — Ravi explicitly stated this
- ⚠️ PARTIAL — Mentioned but ambiguous or conflicting
- ❌ NOT IN TRANSCRIPT — IFA assumption / industry standard, must be confirmed with Ravi
- 🚩 FLAG — Discrepancy between what Ravi said and what the current PDF shows

---

## STRATEGY 1 — VAM Split (Growth Engine: UPRO + TQQQ)

### Bull Allocation (UPRO/TQQQ split)
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:09:02-08** — Ravi (iPhone 115): *"Remember, we are 75% Spy product and 25% is QQQ product, right? So Spy product is Upro."*
- **mar1_meeting.srt 00:10:42-47** — Ravi (iPhone 115): *"75 DUPRO, 75, that was the initial, if you see bull... 75, 25, that's the same one."*
- **Current PDF says:** 75% UPRO / 25% TQQQ ✅ MATCHES

### Moving Average Type (SMA vs EMA)
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:08:24-28** — Rupesh: *"we will check the SMA, if SPI is greater than 200 SMA"*
- **feb10_strategy_discussion.srt 00:10:51-56** — Ravi: *"second hierarchy would be SMAs, the third hierarchy would be RSI"*
- Both transcripts use "SMA" throughout. EMA never mentioned.
- **Current PDF says:** SMA ✅ MATCHES

### MA Periods
**STATUS: ✅ CONFIRMED**
- **feb10_strategy_discussion.srt 00:11:24-26** — Ravi: *"50-Day Moving Average, has breached to the downside"*
- **feb10_strategy_discussion.srt 00:13:32** — Ravi: *"once you breach SMA200"*
- **mar1_meeting.srt 00:08:24** — Rupesh: *"if SPI is greater than 200 SMA"*
- **mar1_meeting.srt 00:11:51** — Ravi: *"prior to SMA breach less than 50"*
- **Current PDF says:** 50-day SMA and 200-day SMA ✅ MATCHES

### RSI Period
**STATUS: ❌ NOT IN TRANSCRIPT**
- Neither transcript mentions a specific RSI period (14-day, 9-day, 21-day, etc.).
- **IFA assumption: 14-day RSI (industry standard)**
- **🚩 FLAG FOR RAVI: What RSI period? Industry standard is 14. Confirm or override.**
- **Current PDF says:** 14-day RSI — NOT CONFIRMED BY CLIENT

### RSI Overbought Trigger (trim threshold)
**STATUS: ✅ CONFIRMED**
- **feb10_strategy_discussion.srt 00:09:38-42** — Rupesh: *"the defensive trim, where RSA is greater than 75"*
- **feb10_strategy_discussion.srt 00:31:35-38** — Ravi: *"we want to kind of, ah, we, we put 75 RSI there. That's somewhat arbitrary. Is it 72 versus 75?"*
- Note: Ravi acknowledged the number is somewhat arbitrary and asked about optimization, but confirmed 75 as the working threshold.
- **Current PDF says:** RSI > 75 ✅ MATCHES

### RSI Rebuy Threshold (exit from BULL_TRIMMED)
**STATUS: ❌ NOT IN TRANSCRIPT**
- Neither transcript specifies what RSI level triggers the rebuy from BULL_TRIMMED back to BULL_100.
- **IFA assumption: RSI < 60 (industry standard)**
- **🚩 FLAG FOR RAVI: What RSI level triggers rebuy? Current code uses 60. Confirm.**
- **Current PDF says:** RSI < 60 → rebuy to 99% — NOT CONFIRMED BY CLIENT

### RSI Trim Amount (how much is sold)
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:27:22-24** — Rupesh (confirmed by Ravi): *"bull trimmed state, it will be 75%"*
- BULL_TRIMMED state = 75% allocation (sell 25% from the 100% bull position).
- **Current PDF says:** BULL_TRIMMED = 75% (sell 25%) ✅ MATCHES

### Defensive Trigger (50-SMA breach)
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:11:51-12:00** — Ravi: *"prior to SMA breach less than 50... SMA 50% UPRO, yes, QQQ less than 50, Defensive Trim 50% DQQ"*
- **feb10_strategy_discussion.srt 00:11:24-26** — Ravi: *"50-Day Moving Average, has breached to the downside. So you're only 50% in... in Upro currently"*
- **Current PDF says:** SPY below 50-day SMA triggers DEFENSIVE ✅ MATCHES

### Defensive Confirmation Days (how many days below 50-SMA to trigger)
**STATUS: ⚠️ PARTIAL — IFA interpretation, not explicitly stated**
- **mar1_meeting.srt 00:15:24-30** — Ravi: *"Apply state-based logic and not trim it next day also, because the ratios tomorrow will not be exactly 50%"*
- **feb10_strategy_discussion.srt 00:11:53-58** — Ravi: *"you wait for it to either go higher than 50-Day Moving Average for a couple of consecutive days, then you buy"* (this is for RE-ENTRY from defensive, not entry)
- What the transcript confirms: Once in DEFENSIVE, you do NOT re-trim daily. You wait for the next signal (state-based). The "couple of consecutive days" quote refers to RE-ENTRY confirmation, not entry confirmation.
- **IFA interpretation for entry:** 2 consecutive days below 50-SMA to avoid whipsaw.
- **🚩 FLAG FOR RAVI: Does entry into DEFENSIVE require 1 day below 50-SMA, or 2 consecutive days? Current code uses 2 consecutive days.**
- **Current PDF says:** 2 consecutive days for entry AND exit — ENTRY portion not confirmed by client

### Defensive Trim Amount
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:12:00** — Ravi reading Rupesh's diagram: *"Defensive Trim 50% DQQ"*
- **feb10_strategy_discussion.srt 00:11:24** — Ravi: *"So you're only 50% in... in Upro currently"*
- **mar1_meeting.srt 00:27:32-35** — Rupesh: *"if SPY is greater than 50 [SMA], then we will go to 50% to pro"* (in reverse: if SPY < 50-SMA, trim to 50%)
- **Current PDF says:** DEFENSIVE = 50% UPRO ✅ MATCHES

### Kill Switch Indicator
**STATUS: ✅ CONFIRMED**
- **feb10_strategy_discussion.srt 00:10:04-09** — Ravi: *"VIX would be the first hierarchy. VIX would be the kill switch"*
- **feb10_strategy_discussion.srt 00:10:09-11** — Ravi: *"Kill switch, basically. VIX is number one. Once kill switch is there, nothing else matters."*
- **mar1_meeting.srt 00:03:06-07** — Rupesh: *"signal hierarchy using the first priority to kill switch"*
- **Current PDF says:** VIX is kill switch ✅ MATCHES

### Kill Switch VIX Threshold
**STATUS: ✅ CONFIRMED (VIX threshold) / 🚩 FLAGGED (exact trigger condition)**
- **mar1_meeting.srt 00:38:10-13** — Ravi: *"when the [SPY is below] 200 SMA, okay. And the VIX is, is about 30"*
- **mar1_meeting.srt 00:38:18-22** — Ravi: *"If the VIX is [SPY below 200-SMA] and VIX is more than 30, you enter XBXU [SPXU]"*
- Note: The transcript is garbled (auto-transcription errors). The logical reading is: when SPY < 200-SMA AND VIX > 30 → predatory short. The kill switch → CASH is separate.
- **feb10_strategy_discussion.srt 00:09:53** — Rupesh: *"RSA greater than 75 and SPI closes below 50 MDMA and avoid anti-spikes 31"* — "anti-spikes 31" may reference VIX 31 as kill switch threshold.
- **🚩 FLAG FOR RAVI: Is the kill switch triggered by VIX > 30 ALONE, or VIX > 30 AND SPY < 200-SMA? Current code uses "VIX > 30 OR SPY < 200-SMA". Transcripts suggest both conditions together for predatory short entry, but kill switch condition is less clear.**
- **Current PDF says:** Kill switch = VIX > 30 OR SPY < 200-SMA — OR logic NOT confirmed by client

### Kill Switch Confirmation Period
**STATUS: ✅ CONFIRMED (immediate, no delay)**
- **feb10_strategy_discussion.srt 00:10:09-11** — Ravi: *"Kill switch, basically. VIX is number one. Once kill switch is there, nothing else matters."*
- No mention of any delay or confirmation period for kill switch.
- **Current PDF says (in Known Issues):** "No confirmation period on VIX leg by design (per client requirements)" ✅ MATCHES

### Re-entry from CASH
**STATUS: ⚠️ PARTIAL — VIX threshold conflicting**
- **mar1_meeting.srt 00:08:24-32** — Rupesh: *"we will check the SMA, if SPI is greater than 200 SMA, yes, we will check QQQ, if no, we will extend cache"*
- **mar1_meeting.srt 00:10:18-22** — Ravi: *"once a spy is met more than 200, then you check the results. If it's less than 30, then you enter."* → VIX < 30 for re-entry
- **mar1_meeting.srt 00:13:50-53** — Ravi: *"Spy more than 200,000. And QQQ more than 200, and Wix less than 60, go to Bull, right?"* → VIX < 60 for re-entry (CONFLICTING)
- **feb10_strategy_discussion.srt 00:13:35-39** — Ravi: *"once you breach SMA200, the re-entry should be 100%, ideally be 100% in"*
- **🚩 CONFLICT: Two different VIX thresholds for re-entry appear in the transcript. 00:10:18 says VIX < 30, and 00:13:50 says VIX < 60. The 60 may be a transcription error of "30". Must confirm with Ravi.**
- **🚩 FLAG FOR RAVI: Also confirm — for re-entry, is it SPY > 200-SMA only, or ALSO SPY > 50-SMA? Current code requires both.**
- **Current PDF says:** Re-entry = VIX < 30 AND SPY > 200-SMA AND SPY > 50-SMA — SPY > 50-SMA requirement and exact VIX threshold NOT confirmed

### Signal Priority Hierarchy
**STATUS: ✅ CONFIRMED**
- **feb10_strategy_discussion.srt 00:10:51-56** — Ravi: *"first hierarchy is Kill Switch, second hierarchy would be SMAs, the third hierarchy would be RSI"*
- **mar1_meeting.srt 00:03:06-10** — Rupesh: *"signal hierarchy using the first priority to kill switch, second priority to SMS signals, and third priority to RSI"*
- **Current PDF says:** Kill Switch (1) → SMA (2) → RSI (3) ✅ MATCHES

### State-Based Logic (no daily rebalancing)
**STATUS: ✅ CONFIRMED**
- **feb10_strategy_discussion.srt 00:11:07-11** — Ravi: *"you should do a state-based analysis, not based on the analysis because of the state-based is..."*
- **mar1_meeting.srt 00:15:24-30** — Ravi: *"Apply state-based logic and not trim it next day also"*
- **mar1_meeting.srt 00:16:12-14** — Sudhir: *"Once you reach a state, you wait for the next signal, right? Until then, you'll be in the same position, right?"*
- **Current PDF says:** State-based logic ✅ MATCHES

### Step 1 vs Step 2 Architecture
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:23:46-24:09** — Ravi: *"Bull would be 100% you pro. There's no TQQ and there's no QQQ signal... Just do pure three signals, RSI, 200, 50 day average. NUpro, just keep it simple. Let's look at the numbers and then we can... Then we add 25% basket."*
- **mar1_meeting.srt 00:30:09-12** — Ravi: *"Yeah, just, just, this is like, Like step 1, step 2 would be adding TQQ."*
- Step 1 = 100% UPRO only (no TQQQ). Step 2 = add 25% TQQQ.
- **Current PDF says:** Step 1 = UPRO only, Step 2 = UPRO + TQQQ ✅ MATCHES

---

## STRATEGY 2 — Predatory Short (SPXU)

### Instrument
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:35:19-23** — Rupesh: *"In the messages, what is sent you is SPXU."*
- **mar1_meeting.srt 00:35:26-28** — Ravi: *"Yes. It's really active. Did you do SPXU or not?"*
- **Current PDF does not explicitly cover Strategy 2 (Predatory Short). Not yet in generator.**

### Activation — From CASH only
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:04:59-05:07** — Rupesh: *"Yeah, we will go from cash state to short state."*
- **mar1_meeting.srt 00:05:36-43** — Rupesh: *"from casted we will go to if the short entry condition conditions are met, we will go to the short state."*
- **mar1_meeting.srt 00:18:08-22** — Ravi: *"from cash to short, we need to make an entry logic for the short entry and the short exit"*

### Entry Conditions
**STATUS: ✅ CONFIRMED (both conditions)**
- **mar1_meeting.srt 00:38:10-25** — Ravi: *"when the [SPY is below] 200 SMA, okay. And the VIX is, is about 30... If the VIX is [SPY below 200] and VIX is more than 30, you enter XBXU [SPXU]"*
- Transcript is auto-transcribed with errors but context is clear: SPY < 200-SMA AND VIX > 30 → enter SPXU from CASH.

### Capital Deployed
**STATUS: ✅ CONFIRMED (50% of cash)**
- **mar1_meeting.srt 00:38:25-27** — Ravi: *"which is the reverse of your, uhh, 50%. We'll see you of the cash you should put in XBXU."*
- 50% of the cash position deployed into SPXU.

### Exit Conditions
**STATUS: ❌ NOT SPECIFIED IN TRANSCRIPT**
- Ravi deferred this: **mar1_meeting.srt 00:38:32** — *"But that's a strategy number three, don't worry, we'll do it later."*
- **🚩 FLAG FOR RAVI: No exit conditions for SPXU were discussed. What triggers exit from SPXU back to CASH?**

### SGOV (Cash buffer during short)
**STATUS: ❌ NOT MENTIONED IN TRANSCRIPT**
- SGOV is not referenced in either transcript.
- **🚩 FLAG FOR RAVI: Should remaining 50% cash be parked in SGOV during SPXU position?**

---

## STRATEGY 3 — Safety Valve

### STATUS: ❌ ENTIRE STRATEGY NOT DISCUSSED IN THESE TWO TRANSCRIPTS

- **mar1_meeting.srt 00:24:12-23** — Ravi: *"Then there are two short strategies that we have... that will boost in 2020 and 2022. That will boost the returns kind of stuff later."*
- Ravi mentioned "two short strategies" but only SPXU (predatory short) was discussed in detail.
- SVIX is NOT mentioned in either transcript.
- "Safety Valve" label is IFA's terminology.
- **🚩 FLAG FOR RAVI: Strategy 3 (Safety Valve / SVIX) details were never discussed in Feb 10 or Mar 1 meetings. All parameters (SVIX instrument, VIX entry threshold 30-40, panic entry at VIX > 50, 10%/30% allocation, SGOV buffer, exit at VIX < 20) are IFA assumptions. Entire Strategy 3 spec needs to come from Ravi.**

---

## EXECUTION PARAMETERS

### Signal Generation Time
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:03:15-18** — Rupesh: *"Daily, this, uh, all this data will be checked at close, like around 4 p.m."*
- **Current PDF says:** Signal at market close (4 PM ET) ✅ MATCHES

### Execution Time (T+1)
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:03:21-26** — Rupesh: *"T plus 1, 30 minutes after open market, execution will be happening."*
- **feb10_strategy_discussion.srt 00:12:37-42** — Ravi: *"next day would be like 30 minutes after opening or something like that... next day morning, 30 minutes after opening would be reasonable for now"*
- **Current PDF says:** T+1, next-day open ✅ MATCHES (PDF uses open price, consistent with "30 min after open")

### Slippage
**STATUS: ⚠️ PARTIAL (baseline confirmed, stress scenario is IFA addition)**
- **feb10_strategy_discussion.srt 00:04:08-10** — Rupesh: *"it will be 0.05%, the slippage, resumptions"*
- **mar1_meeting.srt 00:03:28-35** — Rupesh: *"I took the slippage commission, these values, but we'll change according to you as per your suggestion."*
- Baseline 0.05% (5 bps) slippage was confirmed by Ravi/Rupesh.
- 20 bps stress slippage (VIX >= 25) is IFA's addition.
- **Current PDF says:** Normal=5 bps, Stress=20 bps (VIX>=25) — stress tier is IFA assumption, NOT from client

### Commission Model
**STATUS: ❌ NOT SPECIFIED IN TRANSCRIPT**
- Neither transcript mentions IBKR, commission rates, or per-share fees.
- **IFA assumption: IBKR tiered at $0.005/share**
- **Current PDF says:** IBKR tiered — NOT CONFIRMED BY CLIENT

### Initial Capital
**STATUS: ✅ CONFIRMED**
- **mar1_meeting.srt 00:33:23-25** — Rupesh: *"initial amount invested was $100,000"*
- **Current PDF says:** $100,000 ✅ MATCHES

---

## CAPITAL ALLOCATION (Multi-Strategy)

### Strategy 2 vs Strategy 3 when both active
**STATUS: ❌ NOT DISCUSSED**
- Strategy 3 was never fully discussed. This question is moot until Strategy 3 is specified.

### Max Allocation Cap (99% vs 100%)
**STATUS: ❌ NOT SPECIFIED IN TRANSCRIPT**
- 99% allocation cap is IFA's engineering decision to prevent negative cash.
- **Current PDF says:** 99% cap — IFA decision, not from client

---

## SUMMARY TABLE — CONFIRMED vs ASSUMED

| Parameter | Transcript Status | Value | Source |
|-----------|------------------|-------|--------|
| UPRO/TQQQ split | ✅ CONFIRMED | 75% / 25% | mar1 00:09:02, 00:10:42 |
| MA type | ✅ CONFIRMED | SMA | Both transcripts |
| MA periods | ✅ CONFIRMED | 50-day, 200-day | Both transcripts |
| RSI period | ❌ NOT IN TRANSCRIPT | 14-day (IFA default) | — |
| RSI trim trigger | ✅ CONFIRMED | RSI > 75 | feb10 00:09:38, 00:31:35 |
| RSI rebuy threshold | ❌ NOT IN TRANSCRIPT | 60 (IFA default) | — |
| RSI trim → to 75% | ✅ CONFIRMED | 75% allocation | mar1 00:27:22 |
| Defensive trigger | ✅ CONFIRMED | SPY < 50-SMA | feb10 00:11:24, mar1 00:11:51 |
| Defensive trim amount | ✅ CONFIRMED | 50% allocation | mar1 00:12:00, feb10 00:11:24 |
| Defensive confirmation (entry) | ⚠️ PARTIAL | 2 days (IFA) | feb10 00:11:53 (re-entry only) |
| Kill switch indicator | ✅ CONFIRMED | VIX | feb10 00:10:04 |
| Kill switch VIX level | ✅ CONFIRMED | VIX > 30 | mar1 00:38:10-22 |
| Kill switch trigger logic | ⚠️ PARTIAL | VIX > 30 (confirmed); OR SPY < 200-SMA (not confirmed) | mar1 00:38:10 |
| Kill switch confirmation | ✅ CONFIRMED | Immediate (no delay) | feb10 00:10:09 |
| Re-entry VIX threshold | ⚠️ CONFLICTING | 30 (one ref) vs 60 (another ref) | mar1 00:10:18, 00:13:50 |
| Re-entry SPY condition | ✅ CONFIRMED | SPY > 200-SMA | mar1 00:08:24, feb10 00:13:35 |
| Re-entry SPY > 50-SMA | ❌ NOT IN TRANSCRIPT | Added by IFA | — |
| Signal hierarchy | ✅ CONFIRMED | Kill Switch → SMA → RSI | feb10 00:10:51, mar1 00:03:06 |
| State-based logic | ✅ CONFIRMED | Yes | feb10 00:11:07, mar1 00:15:24 |
| SPXU (predatory short) | ✅ CONFIRMED | SPXU | mar1 00:35:19 |
| Short from CASH only | ✅ CONFIRMED | Yes | mar1 00:04:59, 00:05:36 |
| Short entry: SPY < 200-SMA | ✅ CONFIRMED | Yes | mar1 00:38:10 |
| Short entry: VIX > 30 | ✅ CONFIRMED | Yes | mar1 00:38:10-22 |
| Short capital: 50% of cash | ✅ CONFIRMED | 50% | mar1 00:38:25 |
| Short exit conditions | ❌ NOT IN TRANSCRIPT | Not discussed | — |
| SGOV | ❌ NOT IN TRANSCRIPT | Not mentioned | — |
| Strategy 3 (SVIX/Safety Valve) | ❌ NOT IN TRANSCRIPT | All params IFA assumptions | — |
| T+1 execution | ✅ CONFIRMED | T+1, 30min after open | mar1 00:03:21, feb10 00:12:37 |
| Slippage baseline | ✅ CONFIRMED | 0.05% (5 bps) | feb10 00:04:08 |
| Slippage stress | ❌ NOT IN TRANSCRIPT | 20 bps (IFA) | — |
| Commission | ❌ NOT IN TRANSCRIPT | IBKR (IFA assumption) | — |
| Initial capital | ✅ CONFIRMED | $100,000 | mar1 00:33:23 |
| Allocation cap (99%) | ❌ NOT IN TRANSCRIPT | IFA engineering decision | — |
| Step 1 = UPRO only | ✅ CONFIRMED | Yes | mar1 00:23:46 |
| Step 2 = add TQQQ | ✅ CONFIRMED | Yes | mar1 00:30:09 |

---

## CRITICAL FLAGS FOR RAVI (must confirm before final audit)

1. **RSI period** — Is it 14-day? Current code uses 14.
2. **RSI rebuy threshold** — What RSI level triggers rebuy from BULL_TRIMMED? Current code uses 60.
3. **Kill switch condition** — Is it VIX > 30 alone, or VIX > 30 AND SPY < 200-SMA, or VIX > 30 OR SPY < 200-SMA?
4. **Re-entry VIX threshold** — Two references in transcript: one says < 30, one says < 60. Which is correct?
5. **Re-entry SPY > 50-SMA requirement** — Required or not?
6. **Defensive entry: 1 day or 2 consecutive days** below 50-SMA to trigger DEFENSIVE state?
7. **SPXU exit conditions** — What signals exit from predatory short back to CASH?
8. **SGOV** — Should remaining cash be in SGOV during SPXU? During full CASH state?
9. **Strategy 3 (Safety Valve)** — Entire spec needed from Ravi. Current params (SVIX, VIX 30-40, 10%/30% allocation) are IFA assumptions.
10. **Commission model** — IBKR tiered? Or different broker?

---

*Extraction complete. All parameters above are sourced directly from transcript audio with timestamps.*
*IFA assumptions are clearly labeled ❌ and flagged for client confirmation.*
