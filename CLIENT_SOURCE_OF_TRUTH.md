# Ravi & Sudhir's Requested Strategy — Client Source of Truth

Built entirely from the six recorded meetings (10 Feb, 1 Mar, 9 Apr, 16 Apr, 15 May, 24 May 2026). This is what the client actually asked for, confirmed, corrected, or flagged as broken — in their own words, not what the codebase currently does. Use this as the "ground truth" side of the comparison against the built strategies.

---

## 1. Who's involved, and how this evolved

- **Ravi** and **Sudhir** — the clients. Both physicians, longtime leveraged-ETF investors, explicitly *not* professional traders. Ravi drives almost all of the detailed technical review; Sudhir frames risk/goals and asks the "why" questions.
- **Insight Fusion Analytics** — the vendor. **Anmol** (founder, business/strategy framing), **Rupesh** (built the first state-machine diagrams in meeting 2, then largely steps back), **Ameesh** (the analyst who actually built and ran every backtest from meeting 3 onward — most of the detailed back-and-forth is with him).
- Timeline: kickoff and requirements (meeting 1) → first architecture diagram and a deliberate scope-down decision (meeting 2) → first real DataBento-based results and a live line-by-line trade-log audit (meeting 3) → short-strategy (SPXU) results and a confirmed bug fix (meeting 4) → first interactive dashboard demo (meeting 5) → continued audit, a new unresolved anomaly, then the call ends and the recording continues into an unrelated internal team meeting (meeting 6).

## 2. The strategy the client actually described

Stated as a strict, three-level priority order, unchanged across all six meetings:

1. **Kill switch (highest priority, always checked first):** VIX above 30, OR price below its 200-day moving average. Either one alone is enough — "once kill switch is there, nothing else matters."
2. **Defensive trim (second priority):** price below its 50-day moving average for a confirmation period (originally discussed as "a couple of consecutive days") — cut the position back, don't sell fully.
3. **Overheated trim (third priority, only relevant when fully invested):** RSI above 75 — trim a bit; RSI back below 60 — buy back in.

This is a *state-based* system by explicit client design, not a fresh-every-day recalculation: once a state is reached, the strategy waits for the next actual signal (an up-cross or down-cross), rather than re-computing and nudging the allocation daily. Ravi specifically asked for this to avoid meaningless day-to-day rebalancing noise.

### The staged build-out the client themselves defined
The client explicitly asked to build this in stages, in this order, and paused after each one to review real results before moving on:

- **Step 1 — UPRO only.** One instrument, four modes (100% / 75% trimmed / 50% defensive / cash). Deliberately stripped down from an earlier, more complex draft ("let's go stepwise... take away the TQQQ basket... just keep it simple") specifically so bugs would be easy to catch.
- **Step 2 — add TQQQ as an independent sleeve.** SPY's signal drives the UPRO allocation, QQQ's signal drives the TQQQ allocation, independently — each sleeve can be defensive while the other stays fully invested. (This independence was originally *missing* from the first draft — see section 6.)
- **Step 3 — a short strategy using SPXU** (a 3x inverse S&P fund), layered on top of the long strategy.
- **Step 4 — a second short strategy using an inverse-volatility fund** (referred to inconsistently across meetings as SVXY / "SVIX" / "Xvicks" — all point to the same instrument family), plus everything combined together.

### Confirmed numeric specifics
- VIX kill threshold: **30**
- Trend lines: **50-day and 200-day** moving averages on SPY (and independently on QQQ for the TQQQ sleeve)
- RSI: overbought trim at **75**, rebuy/recovery at **60**
- Step 1 allocations: 100% / 75% / 50% / 0%
- Step 2 allocations: ~74–75% UPRO + ~25% TQQQ when fully invested, trimmed and independently-defensive variants below that
- Execution: signal generated at the previous day's **close**, trade executed at the **next day's open** (this settled here after meetings 1–2 had initially asked for "30 minutes after open" — the client accepted the simpler next-day-open convention once it was actually built and never objected to the change)
- Commission/slippage: client's own rule of thumb was **~0.05%** combined, though they explicitly said they didn't have precise numbers and left it to the vendor's judgment

## 3. The single most important confirmed fix — the "60-day re-entry blanketing" rule

This came up in nearly every meeting from #3 onward and is the most load-bearing, most-discussed refinement in the entire engagement. Worth being precise about what kind of problem this actually was: **it's a strategy design flaw, not a coding bug.** The code did exactly what the three-priority rulebook told it to do — nobody had thought through what should happen in this specific situation until they watched it play out on real historical data.

**The simple version of what was going wrong:** the strategy measures the trend with two "rulers" — a fast one (the 50-day average) and a slow one (the 200-day average). After a crash, the recovery tends to go like this:

1. Market crashes, strategy bails to cash.
2. Market recovers enough that price climbs back above the **slow ruler** (200-day average) — the rule says that's the all-clear, buy back in at full size (100%).
3. Right after that, the **fast ruler** (50-day average) is often still sitting just above the price, or price wobbles under it for a day or two — completely normal early in a recovery, not a warning sign.
4. But a *separate* rule says "if price has been below the fast ruler for 2 days, cut the position to 50%." That rule has no idea a fresh, full-size re-entry just happened — it just sees "below the fast ruler, two days" and fires anyway.
5. Result: the strategy buys back in at 100%, then almost immediately gets chopped down to 50% by its own second rule — sometimes flip-flopping a few times before the fast ruler catches up. Ravi's own summary: "buy high and sell low."

Each rule made sense on its own — "buy back in once the big trend turns" and "trim if the short-term trend weakens" are both reasonable. The flaw only shows up once you combine them and watch a real recovery unfold: the two rules step on each other right at the moment that matters most, and the strategy ends up holding a smaller position exactly during the early days of a rally, which is often where a lot of the gains happen.

**The fix, confirmed and demoed working:** give a freshly-bought 100% position a grace period where the fast-ruler trim rule is simply switched off — trust the slow ruler's all-clear signal for a while and hold the full position no matter what the fast ruler is doing. The length of that grace period was picked, not derived: Ravi first suggested "20 to 45 days" based on his own eyeballing of how long the 50-day average typically takes to catch up, then the two settled on **60 trading days** as the number to actually build. Ameesh built it as an adjustable counter (called "defensive confirm days" or "blanketing" in different meetings), demoed it live in the May 15 dashboard, and reported a measured, positive effect: **CAGR improved from ~20.3%→23% (Step 1) and ~20.4%→22.6% (Step 2)** with the rule enabled versus the plain 2-day version. In a specific 2015 audit in the final meeting, the client attributed a swing from a losing to a much less bad year specifically to this rule removing 50-day-average whipsaw noise around the 200-day line.

**Status as of the last meeting:** treated by the client as essentially confirmed and important enough that Ravi called it possibly the most important open question of the whole engagement — good idea, with a measured improvement to back it up. But it wasn't fully debugged: in that same final meeting, a run *with* this rule enabled on 2018 data produced a nonsensical **-90% return** that both sides flagged as clearly a bug, not a real result, and it was never resolved on camera.

## 4. The short strategies (Step 3 and Step 4)

**Step 3 — SPXU short, fully specified and confirmed in meeting 4:**
- Entry: from Cash, when VIX > 30 (i.e., during an active kill-switch condition) → put 50% of idle cash into SPXU
- Exit: VIX < 30, **OR** SPY reclaims its 50-day average → sell all SPXU, return to 100% cash

**Result when tested in isolation (Oct 2020–2025, DataBento):** a net **-5%** return, 47 total trades, active only in 2020 and 2022. The entry logic was judged correct by the client; the exit logic was judged clearly wrong — a single March 2020 trade (bought SPXU ~$24, sold ~$16 during the most violent week of the COVID crash) lost roughly 35% on its own and dominated the entire multi-year result.

### Was this a bug or a flaw? A flaw — and a fairly fundamental one, unresolved as of the last meeting.

Think of SPXU as **insurance you buy when you're scared the market is about to keep crashing.** The rule, in plain terms, was: buy the insurance when things look scary (VIX high, market already broken down), and sell the insurance once things calm back down (VIX drops, or the market climbs back above its 50-day average). That sounds sensible on paper — and it's worth noting Ameesh had to *guess* this exit rule in the first place, because the client had never specified one; Ravi reviewed the guess afterward and approved it as reasonable. So the code did exactly what both sides had agreed to. The problem only showed up once it was actually tested against a real crash.

Two things combined to make it fail badly on the one real test case available:

1. **SPXU is itself a 3x leveraged fund** — it has the exact same "whipsaw decay" problem as UPRO and TQQQ. During the wildest, choppiest days of a crash (which is precisely when you're holding this insurance), a leveraged fund can quietly bleed value from the sheer back-and-forth chop, even while the market is generally moving in the direction that should be helping it.
2. **The exit rule waits for the market to calm down or recover before selling the insurance.** But insurance, by its nature, is worth the *most* while things are still scary and worth the *least* once the market has recovered — since it moves opposite the market. So the rule, just by how it's built, tends to hold on right through the worst of the chop and then sell exactly once the insurance is worth the least. It's structurally set up to lock in a bad exit, not just occasionally unlucky about it.

Put those two together, and the single trade placed during the sharpest week of the COVID crash — buy the insurance at the first sign of panic, hold through weeks of violent chop while it quietly decays, sell once the market finally settles down and the insurance is nearly worthless — lost about 35% on its own, and that one trade was large enough to sink the entire multi-year result into a net loss.

**Status:** never actually fixed, only diagnosed. Two fixes were proposed but not built as of the last meeting: a cooldown/"blanketing" period so the strategy can't re-enter a short for 30–45 days after exiting one (cutting the whipsaw trade count from roughly 40 down to something like 4 over two years), and tightening the exit so it cuts losses faster instead of waiting for a full market recovery. Unlike the 60-day re-entry rule in section 3, there is no confirmed-working version of this fix anywhere in the meetings.

**Step 4 — the second short instrument (SVXY-family) plus full combination:** discussed and planned throughout, but as of the final meeting **still not built** ("we have to do the second short strategy and then add all three combined... we are not there yet").

**Neither Step 3 nor Step 4 ever reached a state the client considered clean or final.**

## 5. Performance reality check — the number that should worry you most

Every result discussed in meetings 1–4 came from a short, favorable window (2020–2025, later Oct 2020–2025). In **meeting 5**, when Ameesh finally ran the long-only UPRO strategy back to **2011**, the picture changed sharply: **alpha over SPY collapsed to "barely 0.5%"** — versus the 5–6%+ that the shorter window had suggested. This directly triggered Sudhir to ask, on-camera, "should we ditch this idea?" As of the last meeting this question was not resolved — Ravi's position was to finish gathering the longer-window data (2011+ for the long strategy, plus whichever short instrument has data that far back) before deciding, not to abandon the approach outright.

**Any comparison against the current codebase's stored results should treat this as the headline caveat**: those stored numbers all come from the short, favorable window the client themselves suspected of being cherry-picked, and the one time they tested the honest longer window, the strategy's edge nearly disappeared.

## 6. Things Ravi repeatedly flagged as wrong or broken

This is the recurring pattern of corrections Ravi made to Ameesh's (and earlier, Rupesh's) work, across meetings 2 through 6. Listed roughly by how often/insistently each one came up:

1. **The Cash→Bull re-entry whipsaw (the single most-repeated correction — raised in meetings 3, 4, 5, and still being audited in 6).** Re-entering at 100% only to get trimmed back to 50% almost immediately, because the 50-day average hadn't caught up to the 200-day average yet. Ravi called this out as a real, recurring, return-damaging bug the first time he saw it in the trade logs ("this has been a laggard for the returns... buy high and sell low is what's going on"), and it's the issue that led directly to the 60-day blanketing rule in section 3. Even after the fix was built and demoed, Ravi kept re-auditing specific years (2015, 2018) in the final meeting to make sure it was actually behaving — and one of those checks turned up the -90% anomaly, meaning as of the last meeting, this area still wasn't fully trusted.

2. **Wrong day's price shown in the trade log ("gap %" / "close" column bug), meeting 4.** Ameesh's trade log labeled a column as the "same day close," but Ravi worked through the actual numbers by hand and showed it was actually reporting the *execution* day's close, not the *signal* day's close — an off-by-one-day data labeling bug. Ameesh confirmed it was a genuine bug affecting "one common engine," i.e., all the trade logs, not just Step 1. The same category of confusion ("is it the day of execution or the trigger date?") came up again later in the same meeting from Sudhir, suggesting it wasn't fully resolved on the first pass.

3. **The independent-sleeve bug in the original 7-state diagram, meeting 2.** Rupesh's first decision-tree design required *both* SPY and QQQ conditions to be true together before adjusting either the UPRO or TQQQ allocation. Ravi caught this directly on the call ("that flowsheet is wrong though... SPY and QQQ should be individual for each one, not together") — each sleeve's allocation should react only to its own underlying index's signal.

4. **A short state with no defined action, meeting 2.** Rupesh's diagram had a documented *entry condition* for the short state but never specified what to actually buy or do once triggered. Ravi pushed on this directly ("you're saying it's short and it ends — it doesn't lead to any action") until it was clarified that the intended instrument was SPXU.

5. **The SPXU exit logic itself, meeting 4.** Distinct from the trade-log formatting bug above — Ravi judged the *entry* rule for the short strategy to be working correctly, but the *exit* rule to be genuinely wrong, given the outsized single-trade loss it produced during the COVID whipsaw. This was still unresolved as of the last meeting.

6. **Partial-year data mislabeled as a full year, meeting 3.** Ameesh presented a "2020" annual return figure that Ravi noticed was actually only from October 2020 onward, not the full calendar year — a labeling/reporting error rather than a strategy bug, but one more instance of Ravi independently double-checking Ameesh's numbers and catching a discrepancy.

7. **An unexplained portfolio-value jump, meeting 6 (unresolved).** While auditing a specific trade, Ravi noticed portfolio value jump from $53,000 to $107,000 between two trade-log rows with no explanation that made sense to him ("I don't understand that math... the numbers look very bizarre"). Ameesh could not explain it live on the call and said he'd look into it — no resolution appears anywhere in the recorded meetings.

8. **The -90% "blanket enabled" result on 2018 data, meeting 6 (unresolved).** Same meeting, same audit session — flagged by Ravi as clearly not a real result ("there's something wrong about the logs"), never resolved on camera.

If your last call with Ravi referenced something not clearly matching one of these eight, it's likely something that surfaced after this transcript window closes — worth asking him to point to the specific trade or date so it can be cross-checked the same way items 1–8 were here.

## 7. A strategy family that never comes up

Across all six meetings, there is **no mention** of: ATR, a drawdown circuit breaker measured against a trailing account peak, SHY, GLD, or a plain binary trend-only regime (no VIX at all). That entire design direction — which exists in the codebase as three separate strategies — does not trace back to anything Ravi or Sudhir asked for in any of these six calls. Combined with the internal team meeting's own candid description of pre-generating large batches of "profitable-looking" variations to keep on hand for clients generally, the working hypothesis is that this family was never a deliverable for this engagement specifically. Worth confirming directly rather than assuming it belongs in the client-facing product.

## 8. Explicit goals and risk tolerance, stated directly

- Beat SPY by **4–6%** a year "in a perfect world" — not aiming for outsized returns
- Target roughly **15–20% CAGR**, sustained over a **10–15 year** horizon, "not too greedy... don't want to hit every ball to a six"
- Willing to **cap the upside** in exchange for **real downside protection** — stated explicitly by Sudhir: "we are okay with capping the upside a little bit, but we want a downside protection"
- Even a **42% single-year drawdown** (Step 1, calendar year 2022) was called "quite a bit" and "wish it was in the mid-thirties" — meaning the client's own stated comfort zone is *tighter* than what Step 1/Step 2 currently produce, let alone the far larger drawdowns seen in the untested-by-this-client strategy family in section 7
- A recurring, explicit motivation: replace their own emotional, inconsistent past decisions (both admit to years of "guessing," Sudhir joking that avoiding each other's calls over the years would have made them millions richer) with an objective, rules-based system

## 9. Open threads, unresolved as of the last recorded meeting

- The 60-day blanketing rule's -90% anomaly (section 3/6)
- The unexplained portfolio-value jump (section 6)
- SPXU's exit logic (section 4)
- The second short instrument (SVXY-family) and full Step 4 combination — not yet built
- Extending real (non-synthetic) data back to 2011+ for a fair long-term read — in progress, not complete
- Synthetic UPRO/TQQQ/SVXY data to go back further (1980s+) — discussed, never started
- A proposed "1–3% band" tolerance buffer around the SMA thresholds (to reduce whipsaw further, beyond what the 60-day rule already fixes) — proposed by Ravi in the final meeting, not yet built
- The core existential question raised in meeting 5 — is this strategy worth continuing once tested honestly over the long window — was explicitly left open, not answered
