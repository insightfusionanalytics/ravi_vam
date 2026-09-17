# Our Build vs. What Ravi Actually Asked For

A side-by-side check: what's actually running on the website today, versus what Ravi and Sudhir asked for across six meetings and the WhatsApp thread. For everything that doesn't match, this says whether it was **our mistake** or **a flaw in the idea itself that nobody could have seen coming until it was tested.**

## The four simple labels used below

- **Coding flaw** — we knew exactly what to build, and the code just didn't do it. A typing/logic mistake on our side.
- **Strategy flaw** — the code does *exactly* what was agreed. The rule itself, once tested on real data, turned out to cause a problem nobody thought of when the rule was designed. Nobody's fault — this is what backtesting is *for*.
- **Never asked for** — something that exists in our build that Ravi never requested, in any meeting or message.
- **Not finished** — something Ravi asked for that simply hasn't been built yet. Not broken, just not done.

---

## Quick scoreboard

| # | Item | Ravi wanted | We have | Right or Wrong | Type |
|---|---|---|---|---|---|
| 1 | Order of rules | Fear-gauge check first, then trend check, then overheated check | Exactly that order | **Right** | — |
| 2 | Step 1 scope | Just one fund (UPRO), four simple modes | Exactly that | **Right** | — |
| 3 | Step 2's two funds | Each fund reacts to its *own* signal, not each other's | Exactly that (this was wrong in an early draft, caught and fixed before it shipped) | **Right** | — |
| 4 | When trades happen | Decide at yesterday's close, trade at today's open | Exactly that | **Right** | — |
| 5 | The 60-day "give it room to breathe" rule | A specific, tested, agreed-on fix for a real whipsaw problem | **Fixed 2026-09-18** — built into the live dashboard's engines (Step 1 and Step 2), toggleable, on by default | **Right (now)** | Strategy flaw (the fix itself was good — it just wasn't in this build until now) |
| 6 | The two short (insurance) strategies | Two fully planned strategies (Step 3, Step 4) | **Don't exist at all** | **Wrong** | Not finished (and the one that was tested had its own strategy flaw — see below) |
| 7 | The "Autopilot" strategies (the three that use a drawdown safety-net instead of the fear gauge) | Never mentioned once, in six meetings or the whole chat | **Exist, and are presented as part of the same product** | **Wrong** | Never asked for |
| 8 | Which years count as "the real result" | Test over a long, honest stretch (2011+) before trusting the numbers | **Fixed 2026-09-18** — a "History Range" toggle now lets Ravi switch any strategy's dashboard between "Recent (~2020-2025)" and "Full History (2011-2025)" himself | **Right (now)** | Not finished (the long test was run once, found the edge nearly vanished, and that finding never made it into what was shown — now it's one click away instead of buried in an audit log) |
| 9 | Trade log showing the right day's price | Show the signal day's closing price next to each trade, so the overnight gap is visible | **Fixed 2026-09-18** in both offline report scripts — now shows the real signal-day close and a genuine overnight gap (verified against real data, e.g. one trade showed a real -2.9% overnight move that used to be invisible) | **Right (now)** | Coding flaw (this is the exact bug Ravi caught by hand — it was reportedly fixed once already, drifted back in, now fixed a second time and verified by direct code read) |

---

## What we got right

1. **The three-step priority checklist.** Fear gauge first (always wins), then the slower trend line, then the overheated/RSI check last. This is exactly what Ravi described from the very first meeting, and it's exactly what the code does today.
2. **Step 1 being just one fund, four simple modes.** Ravi deliberately asked for this simplified starting point ("let's keep it simple, just UPRO and SPY") specifically so bugs would be easy to spot. That's exactly what got built.
3. **Step 2's two funds acting independently.** Early on, an earlier draft had a bug where both funds needed to agree before either one's allocation changed. Ravi caught this himself on a call and it was fixed before anything shipped — the version running today correctly lets one fund be defensive while the other stays fully invested.
4. **Trade timing.** Decide using yesterday's closing price, execute at today's opening price. This is what the client settled on and never objected to once it was actually built.

## What we got wrong or missing

### 1. The 60-day "give it room to breathe" rule — FIXED 2026-09-18
**The simple version:** right after the strategy jumps back in at full size following a crash, a *different, unrelated* rule was chopping the position back down almost immediately — because a slower part of the strategy said "get back in" while a faster part was still catching up and said "get defensive." Nobody designed it to fight itself like that; it only showed up once real crash-and-recovery data was tested. Both sides agreed on a fix — give a fresh full-size position 60 days of immunity from that faster rule — and it was built, tested, and shown to genuinely help (CAGR went up a few points).

**Status: this is now built into the live dashboard**, in both Step 1 and Step 2's actual engines (not just the offline scripts). Re-verified directly against the 2020 crash window that the original fix was validated on: CAGR moved from 19.6% to 23.0% with the rule on — matching the client's own validated 20.3%→23% almost exactly — and it correctly suppresses the September 2020 defensive trim that would otherwise have fired just 12 days after a fresh re-entry. It's exposed as a configurable parameter (`reentryImmunityDays`, default 60) rather than hardcoded, and every day's audit log now shows whether the immunity is active and how many days are left on each clock.

### 2. The two short (insurance) strategies — don't exist at all
Ravi and Sudhir asked for two additional pieces, layered on top of the main strategy: one that buys a kind of market "insurance" during a panic (Step 3), and a second, related one (Step 4). Neither exists in what's running today — the site only has the main, "stay invested or go to cash" strategies. On top of that, the one version of the insurance strategy that *was* tested (outside of what's currently live) had its own problem: it tended to buy the insurance when scared and sell it only once things calmed down — which, by nature, is close to the worst possible time to sell insurance, since insurance is worth the least exactly when the danger has passed. That's a strategy flaw, not a coding mistake — everyone agreed the rule sounded reasonable until the numbers showed otherwise. It was never actually fixed before this project moved on to other things.

### 3. Three strategies nobody asked for
Three of the five strategies currently live on the site (the ones that use a "how far below your recent high are you" safety net instead of the fear gauge) never come up anywhere — not in six meetings, not in the WhatsApp chat, not once. They're not wrong in the sense of being broken; they're wrong in the sense of **being on the website as if they were part of what Ravi asked for, when nothing on record shows he ever did.** Worth a direct conversation before presenting these as part of the deliverable.

### 4. The numbers being shown only tell the good part of the story — FIXED 2026-09-18
Every result previously saved and shown came from a short, kind stretch of the market (2020-2025). The one time the team actually tested the strategy over a longer, more honest stretch (back to 2011), the strategy's edge over just buying the S&P 500 nearly disappeared — down to about half a percent, instead of the 5-6% edge the short stretch suggested. That finding was real, it happened, and it wasn't reflected anywhere in what was presented as "the" performance. This isn't a bug in the code and it isn't really a flaw in the strategy either — it was simply an incomplete, unfinished piece of honest testing that never made it into the final picture.

**Status: fixed.** Every strategy's dashboard (Step 1 through Step 4) now has a "History Range" toggle: "Recent (~2020-2025)" or "Full History (2011-2025)," built on the real 2011-2025 merged dataset (Polygon 2011-2019 + DataBento 2020-2025, already split-adjusted). This is no longer something Ravi has to take on faith or dig out of an audit log — he can click the toggle himself on any strategy.

**Update (2026-09-18, after a separate bug fix below):** the Recent-window numbers themselves were quietly wrong until the double-split-adjustment bug was found and fixed (see the bugs table below). With the corrected numbers, the finding is actually *stronger* than first reported: Step 1's alpha over SPY is now 1.7% even in the "flattering" Recent window, barely different from 1.2% in Full History — meaning the vanishing edge isn't only a long-horizon phenomenon, it was already true in the short window once the price data was correct. Step 2 fares a little better (3.6% Recent vs 6.7% Full History, alpha actually *higher* over the long run for this one).

---

### 5. Trade log shows the wrong day's price — FIXED 2026-09-18 (see item #9 above)
Every trade record is supposed to show the price at the moment the signal actually fired, so you can see how much the price moved overnight before the trade executed the next morning. The code was pulling the closing price from the *execution* day — the day *after* the signal — and labeling it as if it were the signal day's number, with placeholder fields (`signal_spy_close`, `signal_vix`, and so on) always left blank. This is the exact bug Ravi caught by manually checking the numbers by hand in one of the meetings; he was told at the time it had been fixed, but reading the actual code showed that fix never made it into what shipped.

**Status: fixed and verified.** Both offline report scripts (Step 1 and Step 2) now capture the actual signal-day snapshot (SPY/VIX/RSI/UPRO/TQQQ values at the moment the signal fired) and use it correctly. Verified against real data — trades now show real overnight gaps, including one case where the price moved -2.9% overnight, information that was completely invisible under the old bug. This was a pure reporting fix — the underlying trading behavior and dollar results are unchanged (confirmed: metrics identical before/after).

## All the specific software bugs found along the way

Separate from the bigger issues above, these are the one-off "something in the code just did the wrong thing" moments that came up during testing. Plain and simple, one at a time:

| Bug | Ever fixed? | Matters for today's build? |
|---|---|---|
| The "-90%" nonsense result | Yes, same day | No — the feature it lived in isn't in this build |
| The go-to-cash mixup | Yes, same day | No — same reason |
| Portfolio value mysteriously doubling | Unclear — never explained on the record | Can't tell — feature not in this build either, but see the new bug below, which is the same *symptom* |
| Wrong day's price in the trade log | Said to be fixed at the time, drifted back in, **fixed again and verified 2026-09-18** | Yes — now fixed for real |
| "2020" that was really "Oct 2020" | Fixed by rerunning the report | No — one-off reporting mistake, not a code problem |
| **Double split-adjustment inflating Step 1/2's entire return** | **Found and fixed 2026-09-18, while building the automated test suite** | **Yes — this was live and active in today's build until fixed** |

**The "-90%" nonsense result.** While testing the 60-day rule on 2018 data, one run spat out a -90% return — a number so extreme neither side believed it was real. It got fixed the same evening.

**The go-to-cash mixup.** This is *why* the -90% happened. The 60-day rule was supposed to just switch off one specific rule (the fast-moving-average trim) for 60 days, leaving everything else running normally. Instead, it had accidentally been built to dump the whole strategy into 100% cash and stop trading entirely for those 60 days — a much bigger, wrong behavior. Ravi caught it by reading the trade logs ("that's not what we wanted"), and it was corrected within the same conversation.

**Portfolio value mysteriously doubling.** While auditing a different trade, the account balance jumped from about $53,000 to about $107,000 between two lines in the log, with nothing in between to explain it. Nobody ever explained this one on the record — it's a genuine loose end.

**Wrong day's price in the trade log.** The log was supposed to show the price at the moment a signal fired, so you could see how much the price moved overnight before the trade actually happened the next morning. It showed the price from the day *after* — the execution day — mislabeled as if it were the right one. This was reported as fixed at the time, but reading the actual code showed that fix never made it into what shipped. Fixed and verified again on 2026-09-18 (see item #9 above).

**"2020" that was really "Oct 2020."** A yearly return being shown as "2020's performance" turned out to only cover October through December of that year, not the full twelve months. A labeling mistake in one report, not a bug in the strategy itself — fixed by simply rerunning the report from January 1st.

**Double split-adjustment inflating Step 1/2's entire return.** UPRO and TQQQ have both split their shares a few times (e.g. UPRO 2-for-1 on 2022-01-13). The original code was written assuming raw, unadjusted price data, so it manually halves/thirds every price before each split date — necessary for real DataBento data. But the live app has actually been running on Yahoo Finance data since the DataBento feed wasn't available, and Yahoo's prices already come pre-adjusted for every split. Doing the manual adjustment *again* on top of already-adjusted data corrupted the price on every split date: UPRO's price on 2022-01-13 was artificially cut in half in every prior trading day's history, creating a fake, one-day "+91% gain" that never happened. Since the strategy happened to be fully invested at that exact moment, this fake gain permanently inflated the portfolio's value for the rest of every backtest run afterward — Step 1's reported CAGR was overstated at roughly 30% when the real number is closer to 18%; Step 2's was overstated at roughly 39% versus a real number closer to 20%. This was caught by writing an automated test that checks for exactly this kind of thing (price continuity across known split dates) — the same kind of check that would have caught the still-unexplained $53k→$107k jump from meeting 6, had one existed at the time. Fixed by skipping the manual adjustment whenever the data is detected as already-adjusted Yahoo data.

**A separate thing worth remembering, not really a "bug" so much as a modeling gap:** an attempt to extend history further back using a man-made ("synthetic") version of UPRO was tested and found to understate real returns by a large margin — because the formula forgot to include reinvested dividends. Not part of what's live today, but worth remembering if anyone tries to extend the backtest further back in the future.
