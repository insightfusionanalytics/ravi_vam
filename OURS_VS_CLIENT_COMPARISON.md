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
| 5 | The 60-day "give it room to breathe" rule | A specific, tested, agreed-on fix for a real whipsaw problem | **Missing entirely** | **Wrong** | Strategy flaw (the fix itself was good — it's just not in this build) |
| 6 | The two short (insurance) strategies | Two fully planned strategies (Step 3, Step 4) | **Don't exist at all** | **Wrong** | Not finished (and the one that was tested had its own strategy flaw — see below) |
| 7 | The "Autopilot" strategies (the three that use a drawdown safety-net instead of the fear gauge) | Never mentioned once, in six meetings or the whole chat | **Exist, and are presented as part of the same product** | **Wrong** | Never asked for |
| 8 | Which years count as "the real result" | Test over a long, honest stretch (2011+) before trusting the numbers | Only the short, favorable 2020-2025 stretch is shown as "the" result | **Wrong** | Not finished (the long test was run once, found the edge nearly vanished, and that finding never made it into what's shown) |
| 9 | Trade log showing the right day's price | Show the signal day's closing price next to each trade, so the overnight gap is visible | Shows the *execution* day's closing price instead, labeled as if it were correct | **Wrong** | Coding flaw (this is the exact bug Ravi caught by hand — it was reportedly fixed at the time, but a direct code read confirms it's still in what's shipped) |

---

## What we got right

1. **The three-step priority checklist.** Fear gauge first (always wins), then the slower trend line, then the overheated/RSI check last. This is exactly what Ravi described from the very first meeting, and it's exactly what the code does today.
2. **Step 1 being just one fund, four simple modes.** Ravi deliberately asked for this simplified starting point ("let's keep it simple, just UPRO and SPY") specifically so bugs would be easy to spot. That's exactly what got built.
3. **Step 2's two funds acting independently.** Early on, an earlier draft had a bug where both funds needed to agree before either one's allocation changed. Ravi caught this himself on a call and it was fixed before anything shipped — the version running today correctly lets one fund be defensive while the other stays fully invested.
4. **Trade timing.** Decide using yesterday's closing price, execute at today's opening price. This is what the client settled on and never objected to once it was actually built.

## What we got wrong or missing

### 1. The 60-day "give it room to breathe" rule — missing (strategy flaw, already solved once)
**The simple version:** right after the strategy jumps back in at full size following a crash, a *different, unrelated* rule was chopping the position back down almost immediately — because a slower part of the strategy said "get back in" while a faster part was still catching up and said "get defensive." Nobody designed it to fight itself like that; it only showed up once real crash-and-recovery data was tested. Both sides agreed on a fix — give a fresh full-size position 60 days of immunity from that faster rule — and it was built, tested, and shown to genuinely help (CAGR went up a few points). **That fix simply isn't present anywhere in what's running today.** This is the single most important thing to bring back, since it's the one thing Ravi personally pushed hardest on, repeatedly, across four separate meetings.

### 2. The two short (insurance) strategies — don't exist at all
Ravi and Sudhir asked for two additional pieces, layered on top of the main strategy: one that buys a kind of market "insurance" during a panic (Step 3), and a second, related one (Step 4). Neither exists in what's running today — the site only has the main, "stay invested or go to cash" strategies. On top of that, the one version of the insurance strategy that *was* tested (outside of what's currently live) had its own problem: it tended to buy the insurance when scared and sell it only once things calmed down — which, by nature, is close to the worst possible time to sell insurance, since insurance is worth the least exactly when the danger has passed. That's a strategy flaw, not a coding mistake — everyone agreed the rule sounded reasonable until the numbers showed otherwise. It was never actually fixed before this project moved on to other things.

### 3. Three strategies nobody asked for
Three of the five strategies currently live on the site (the ones that use a "how far below your recent high are you" safety net instead of the fear gauge) never come up anywhere — not in six meetings, not in the WhatsApp chat, not once. They're not wrong in the sense of being broken; they're wrong in the sense of **being on the website as if they were part of what Ravi asked for, when nothing on record shows he ever did.** Worth a direct conversation before presenting these as part of the deliverable.

### 4. The numbers being shown only tell the good part of the story
Every result currently saved and shown comes from a short, kind stretch of the market (2020-2025). The one time the team actually tested the strategy over a longer, more honest stretch (back to 2011), the strategy's edge over just buying the S&P 500 nearly disappeared — down to about half a percent, instead of the 5-6% edge the short stretch suggested. That finding was real, it happened, and it isn't reflected anywhere in what's currently presented as "the" performance. This isn't a bug in the code and it isn't really a flaw in the strategy either — it's simply an incomplete, unfinished piece of honest testing that never made it into the final picture.

---

### 5. Trade log shows the wrong day's price (confirmed by direct code read — see item #9 above)
Every trade record is supposed to show the price at the moment the signal actually fired, so you can see how much the price moved overnight before the trade executed the next morning. Instead, the code pulls the closing price from the *execution* day — the day *after* the signal — and labels it as if it were the signal day's number. There are even placeholder fields sitting in the code for the correct signal-day values (`signal_spy_close`, `signal_vix`, and so on) that are always left blank and never actually filled in. This is the exact bug Ravi caught by manually checking the numbers by hand in one of the meetings; he was told at the time it had been fixed, but reading the actual code shows that fix never made it into what's running today.

## All the specific software bugs found along the way

Separate from the bigger issues above, these are the one-off "something in the code just did the wrong thing" moments that came up during testing. Plain and simple, one at a time:

| Bug | Ever fixed? | Matters for today's build? |
|---|---|---|
| The "-90%" nonsense result | Yes, same day | No — the feature it lived in isn't in this build |
| The go-to-cash mixup | Yes, same day | No — same reason |
| Portfolio value mysteriously doubling | Unclear — never explained on the record | Can't tell — feature not in this build either |
| Wrong day's price in the trade log | Said to be fixed at the time | **Yes — confirmed still broken in the code today** |
| "2020" that was really "Oct 2020" | Fixed by rerunning the report | No — one-off reporting mistake, not a code problem |

**The "-90%" nonsense result.** While testing the 60-day rule on 2018 data, one run spat out a -90% return — a number so extreme neither side believed it was real. It got fixed the same evening.

**The go-to-cash mixup.** This is *why* the -90% happened. The 60-day rule was supposed to just switch off one specific rule (the fast-moving-average trim) for 60 days, leaving everything else running normally. Instead, it had accidentally been built to dump the whole strategy into 100% cash and stop trading entirely for those 60 days — a much bigger, wrong behavior. Ravi caught it by reading the trade logs ("that's not what we wanted"), and it was corrected within the same conversation.

**Portfolio value mysteriously doubling.** While auditing a different trade, the account balance jumped from about $53,000 to about $107,000 between two lines in the log, with nothing in between to explain it. Nobody ever explained this one on the record — it's a genuine loose end.

**Wrong day's price in the trade log.** The log was supposed to show the price at the moment a signal fired, so you could see how much the price moved overnight before the trade actually happened the next morning. Instead it showed the price from the day *after* — the execution day — mislabeled as if it were the right one. This was reported as fixed at the time, but reading the actual code shows that fix never made it into what's running today (see item #9 above).

**"2020" that was really "Oct 2020."** A yearly return being shown as "2020's performance" turned out to only cover October through December of that year, not the full twelve months. A labeling mistake in one report, not a bug in the strategy itself — fixed by simply rerunning the report from January 1st.

**A separate thing worth remembering, not really a "bug" so much as a modeling gap:** an attempt to extend history further back using a man-made ("synthetic") version of UPRO was tested and found to understate real returns by a large margin — because the formula forgot to include reinvested dividends. Not part of what's live today, but worth remembering if anyone tries to extend the backtest further back in the future.
