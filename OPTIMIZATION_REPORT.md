# Parameter Optimization — Results Report

Ravi asked for the most profitable set of settings to be found automatically, rather than guessed by hand. This report covers that work: what was tried, what actually held up, and what didn't.

**The one-line summary: only one of the six optimization passes run actually produced a result that survived contact with data it had never seen. The other five either found nothing worth changing or made things worse. That is a real, useful finding — it's not a failure of the exercise, it's the exercise doing its job.**

This report covers three strategies:

- **UPRO + TQQQ** — the main, flagship two-fund strategy.
- **UPRO-Only** — the simpler single-fund version.
- **SPXU Hedge Overlay** — the optional "buy insurance during a panic" add-on.

A fourth strategy, **SVIX Safety Valve**, was excluded from this round entirely — see the note at the bottom of the headline table for why.

---

## How to read every number in this report

Every strategy below has two kinds of number:

- **The tuned number** — how good a setting looked while the computer was searching for it, on 2011–2021 data. This number is easy to produce and easy to be fooled by. It is shown here only in the methodology sections, never as a headline.
- **The sealed number** — how that same exact setting performed on 2022–2025, years the search was never allowed to look at while choosing anything. **This is the only number that means anything about whether the setting actually works.** It appears once per candidate in this report, because a sealed-off test that gets re-run repeatedly stops being sealed and just becomes more tuning data.

Every result below is reported this way on purpose, even the ones that are disappointing.

---

## Headline results (sealed data, 2022-02-01 to 2025-12-30)

| Strategy | Candidate | CAGR | Max drawdown | Calmar | vs. today's settings |
|---|---|---|---|---|---|
| **UPRO + TQQQ** (flagship) | Today's settings | 15.5% | -30.4% | 0.511 | — |
| UPRO + TQQQ | **Tuned, Ravi's numbers kept** | **21.2%** | -40.7% | 0.522 | **Real gain in return and risk-adjusted return; worse drawdown** |
| UPRO + TQQQ | Tuned, everything free | 19.8% | -41.0% | 0.483 | Worse than doing nothing on Calmar, despite scoring far higher in the search |
| **UPRO-Only** | Today's settings | 14.5% | -28.5% | 0.508 | — |
| UPRO-Only | Tuned, Ravi's numbers kept | 9.5% | -17.6% | 0.542 | Lower return, lower drawdown, underperforms buy-and-hold SPY |
| UPRO-Only | Tuned, everything free | 8.9% | -16.1% | 0.555 | Same pattern, slightly more so |
| **SPXU Hedge Overlay** | Today's settings | -3.0% | -17.8% | -0.168 | — (already loses money; this is disclosed and expected) |
| SPXU Hedge Overlay | Tuned, Ravi's numbers kept | -8.2% | -30.6% | -0.269 | **Worse on every measure** |
| SPXU Hedge Overlay | Tuned, everything free | -11.1% | -40.0% | -0.279 | Worse still |
| SVIX Safety Valve | — | — | — | — | Not run: this fund didn't exist before 2022-03-30, so there isn't enough independent history to validate anything without serious look-ahead risk. Excluded, same as the three internal-R&D-only strategies not covered in this report. |

**The one candidate worth actually adopting is UPRO + TQQQ, tuned with Ravi's confirmed settings held fixed.** Everything else either didn't beat today's settings or made them worse, once tested honestly.

---

## UPRO + TQQQ (the flagship strategy) — the one real finding

**What was tuned (never touched: the fear-gauge threshold, both trend-line lengths, and both overheat levels — Ravi's actual numbers):**

| Setting | Today | Tuned |
|---|---|---|
| Cooldown after kill switch (days) | 5 | 2 |
| Defensive sell % | 50% | 75% |
| RSI lookback period | 14 | 24 |
| RSI trim % | 25% | 50% |
| UPRO/TQQQ split | 75% / 25% | 10% / 90% |

**Sealed result: CAGR 15.5% → 21.2%, Sharpe 0.625 → 0.696.** This held up. It's a genuine improvement from better-calibrated implementation details, not from changing what Ravi actually asked for — confirmed directly by checking that the strategy spends exactly the same share of days in its defensive states as it does today, fold for fold, before and after tuning.

**The honest catch: max drawdown got worse, not better (-30.4% → -40.7%).** The gain in return didn't come free — it came with more pain along the way. Calmar (return divided by pain) only stayed roughly flat because both sides moved together. Whether that trade is worth taking is a real decision, not something this report can make on Ravi's behalf.

**The everything-free version scored dramatically higher in the search (38% higher than the Ravi's-numbers version) and then did *worse* on sealed data than doing nothing at all.** That's the search finding a pattern in 2011–2021 that didn't generalize — exactly the failure this whole two-pass structure exists to catch. Its winning settings moved the fear-gauge threshold from VIX>30 to VIX>41 and the trend-line length from 200 days to 290 days — genuinely different from what Ravi specified, and the sealed test says that departure wasn't worth it.

One setting the search kept pushing to a range boundary, both passes: **UPRO allocation kept dropping — 10% in the Ravi's-numbers version, near the floor in the free version too.** That's very likely the specific 2011–2021 window rewarding tech-heavy TQQQ over UPRO (a QQQ-led decade), not a durable finding — the same trade did *not* show up as a win on the 2022-2025 sealed data (CAGR was lower there, not higher). Flagged so it isn't mistaken for a robust discovery.

**A follow-up question worth asking directly: is the extra drawdown (-30.4%→-40.7%) a side-effect of scoring for return-divided-by-worst-drop specifically, or is it just how this parameter space behaves?** Tried a second scoring method that explicitly rewards shorter, shallower drawdowns instead (not just the single worst point) — same search, same guardrails. It converged to almost the identical drawdown profile in the search, and on the sealed data it was **worse on every measure** — lower return (18.3% vs 21.2%), worse risk-adjusted return, and only 2 points better on drawdown despite giving up 3 points of CAGR to get it (its Calmar, 0.470, actually landed *below* doing nothing at all, 0.511). Two different scoring methods agreeing that this trade-off is unavoidable is a stronger answer than either one alone: **the extra pain isn't an artifact of how the search was scored — it appears to be a genuine, structural feature of this strategy's parameter space.** The choice of whether to accept it stands as stated above.

---

## UPRO-Only — found nothing worth changing

Tuning the knobs nobody ever specified (RSI period, UPRO allocation) moved the search score by essentially zero — an honest "nothing here" result. Freeing Ravi's own numbers too found a bigger-looking search score, but **both versions underperformed today's settings on sealed data, and both turned buy-and-hold SPY into the better choice** (alpha went negative in both cases). The drawdown did improve substantially in both cases (-28.5% → -17.6%/-16.1%), so if minimizing pain matters more than maximizing return, there's a real trade-off worth a conversation — but neither candidate is a straightforward upgrade.

---

## SPXU Hedge Overlay — tuning made an already-losing strategy worse

This overlay's disclosed flaw (sells the SPXU hedge near the worst possible time) means it already loses money at today's settings, on purpose left unfixed so the effect stays visible. Tuning the one never-confirmed knob (how quickly it exits) cut the loss roughly in half *in the search* — but on sealed data, **every tuned version performed worse than today's settings**, including a drawdown more than double the baseline's in the everything-free case (-17.8% → -40.0%). The 2011-2021 window apparently rewarded a faster exit; 2022-2025 punished it. Ravi's own confirmed entry condition and 50% position sizing held up fine throughout — freeing them added nothing and, if anything, hurt. **Recommendation: leave this overlay exactly as it is; nothing found here should change it.**

---

## Methodology, so this is reproducible

- **Development period:** 2011-01-01 to 2021-12-31, split into 4 contiguous ~2.75-year blocks. Score = median Calmar across the 4 blocks, minus half the spread between blocks (so a setting that only works in one lucky block loses to one that works everywhere).
- **Sealed holdout:** 2022-02-01 to 2025-12-30, one month embargoed after the development period ends. Touched once per candidate reported above, with one deliberate exception: after finding the UPRO + TQQQ trade-off (more return, more drawdown), a second scoring method was tried specifically to check whether the trade-off was an artifact of the first one — that follow-up candidate was also checked against this same sealed period, disclosed here rather than silently. Beyond that one named exception, this period should not be re-checked again without an equally good reason — repeated checks against the same sealed data quietly turn it into more tuning data.
- **Search:** Optuna, TPE sampler, fixed seed 42, up to 200 trials per pass, median pruner (kills an obviously-bad trial after its first block instead of running all 4).
- **Hard rejections** (return a large negative, not just a low score): drawdown worse than -65% in any block; fewer trades in a block than the strategy's own confirmed settings produce there (catches a setting that trades so rarely one lucky trade would dominate its score, without punishing a strategy that's *supposed* to sit idle — e.g. the SPXU overlay's rare-event nature); defensive-state usage below 40% of what the confirmed settings produce in that block (see below); time spent sitting entirely out of the market more than 75% above what the confirmed settings produce in that block — added so a future scoring method can't be gamed by a candidate that avoids losses by avoiding participation instead of navigating the market well.
- **Picking the winner:** not the single best-scoring trial. The top ~15% of trials are clustered by how close their settings are to each other; the winner is the center of the densest cluster, not the highest lone score. A lone spike is usually luck; a cluster of neighboring good scores is much harder to produce by chance. Where the density-pick and the raw best trial agreed closely (which happened in most passes here), that's itself a good sign the result isn't a fluke.
- **A safety backstop worth naming directly:** an earlier pass let the search push the UPRO + TQQQ strategy's "confirm days" setting up to 14, which real market streaks almost never sustain — the search wasn't tuning the defensive-trim trigger, it was quietly switching it off, and it scored well purely because 2011-2021 rewarded staying invested no matter what. That setting is now held fixed at 2 (close to the median length of a real weak stretch: 3 days), and a general rule was added so no other setting can find the same trick: nothing may drop defensive-state usage below 40% of what Ravi's own confirmed settings produce in the same block.
- **What "the search score" is worth on its own:** every tuned number in this report came from choosing the best (or best-region) of up to 150-200 evaluated candidates. Picking the best of that many attempts is expected to land meaningfully above the "true" average even if every candidate were equally good, purely from having many chances. That bias is exactly why the sealed number, not the search number, is the one that matters — and in half of the six passes above, the sealed number showed that bias plainly by coming in worse than doing nothing.

---

## What this report recommends, in one place

1. **Adopt the UPRO + TQQQ strategy tuned with Ravi's numbers kept** (cooldown 2 days, defensive sell 75%, RSI period 24, RSI trim 50%, UPRO/TQQQ split 10%/90%) — real, sealed-data-tested improvement in return and Sharpe. Decide first whether the added drawdown (-40.7% vs -30.4%) is an acceptable trade for that gain — that's a preference call, not a technical one.
2. **Do not adopt** any of the other five tuned candidates (UPRO + TQQQ everything-free, both UPRO-Only candidates, both SPXU Hedge Overlay candidates) on the evidence in this report. They either found nothing or actively underperformed today's settings once tested honestly.
3. **SVIX Safety Valve remains untested** — there isn't enough history for that fund to validate anything without serious look-ahead risk.
