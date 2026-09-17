# Understanding the Strategy — Plain-Language Guide

This document explains, from scratch, what this project's trading strategy actually does, using everyday comparisons instead of jargon, and a real worked example for every rule so you can see *why* it exists, not just *what* it checks. No coding or finance background assumed.

---

## 1. What problem is this solving?

Imagine you have $100,000 you want to invest in the stock market. You have two extreme choices:

- **Buy and hold forever.** Simple, but you ride out every crash fully exposed.
- **Try to time the market yourself.** Constantly watch the news and guess when to buy or sell. Stressful, and humans are bad at this — we panic-sell at the bottom and get greedy at the top.

This project is a middle path: **a fixed, mechanical rulebook that decides, every single day, how much to be invested versus how much to hold back in cash or safe assets** — based only on a handful of numbers read off the market, never gut feeling or news.

It doesn't touch real money. It's a **testing tool**: feed it rules and past market data, and it tells you "if you'd followed this rulebook every day for the last several years, here's what would've happened."

## 2. The five gauges it watches

Think of these as dashboard gauges in a car. Every rule later in this document is just "if this gauge crosses that line, do this."

- **The trend gauge (moving average).** Take the average closing price over the last 50 days, and separately over the last 200 days. If today's price is above its own recent average, that's an uptrend; below, a downtrend. It's like judging whether you're gaining or losing weight by comparing today's number to your monthly average, not one noisy daily reading. The 200-day version is the slow, big-picture read ("what's the multi-month climate?"); the 50-day version reacts faster ("what's happened this quarter?").

- **The "overheated" gauge (RSI).** A 0-100 score for how hot a recent run-up has been. Above ~75 means "this has gone up very fast, very recently" — like a car engine's temperature needle creeping into the red without necessarily meaning the engine is about to fail. Below ~30 means the opposite — a stretch of selling has gotten stretched too. It's used to fine-tune *how much* to hold within an uptrend, not to decide the trend itself.

- **The fear gauge (VIX).** A completely separate number that spikes when investors are panicking and rushing to buy downside insurance. Think of it as a smoke detector — it doesn't measure the fire itself (the price), it measures how scared everyone in the building suddenly is.

- **The choppiness gauge (ATR).** Measures how wide the daily price swings have been lately, as a percentage. High ATR = the market's lurching around in a wide range; low ATR = calm, narrow days. Meant to catch "something's off" even before the trend or fear gauges confirm it.

- **The "how far off my own peak am I" gauge (drawdown).** Not a market reading at all — a check on your *own account balance*. It looks at the highest point your balance has hit in roughly the last three months and asks: how far below that recent high am I right now?

## 3. The idea of "modes"

Rather than smoothly dialing exposure from 0% to 100%, the strategy jumps between a small number of fixed modes — like gears in a car, not a volume knob. And once it decides to shift gears, it doesn't do it instantly: the decision made today gets carried out the *next* trading day, at the opening price, exactly like a real order would take a moment to fill. It also pays a small fee and accepts a slightly worse price on every trade — and a noticeably worse price during a fear spike, because in real life everyone else is trying to sell at the same moment too, so prices get harder to trade cleanly right when it matters most.

### An important note on the word "cash" — it means two different things in this project

Both decision-makers below have a defensive mode people casually call "going to cash." **These are not the same thing**, and it's worth keeping straight:

- **For the Triage Nurse (section 5), "Cash" is literal.** The money is genuinely uninvested — parked, holding nothing, zero market exposure. When it says "go to Cash," that's exactly what happens.
- **For the Autopilot (section 6), "cash" is a nickname, not the real holding.** Its defensive and emergency-stop modes don't actually sit in uninvested dollars — they buy a real fund called **SHY**, a short-term U.S. government bond fund (roughly 1-3 year maturities). SHY is called "cash-like" because short-term government bonds barely move day to day compared to stocks, so it *behaves* almost like sitting still in cash — but it isn't literally cash, and it can still lose a small amount of value (for example if interest rates move). One of its defensive modes blends SHY with **GLD** (a gold fund) instead of using SHY alone — gold is added because it sometimes moves in the opposite direction to stocks during a downturn, giving a bit of extra cushioning beyond just "sit still."

So "the strategy went to cash" means something meaningfully different depending on which of the two you're talking about — genuinely no exposure at all, versus a very low-volatility but still-invested bond (and sometimes gold) position. Worth confirming which one the client means whenever they use the word "cash."

## 4. Two genuinely different decision-makers

This project isn't one strategy with five dial settings — it's **two different styles of decision-making**, built at different times, sitting side by side in the same project. Giving them names makes this much easier to keep straight:

- **The Triage Nurse** — checks for the worst problem first, and only looks at smaller issues if nothing serious is going on.
- **The Autopilot with a Circuit Breaker** — steers on one simple signal, with a separate, unrelated emergency cutoff bolted on for when things have already gone badly wrong.

---

## 5. The Triage Nurse (this is the version marked as the "main" strategy)

A triage nurse in an ER doesn't check every symptom with equal weight, in a random order. They check the *life-threatening* stuff first — is this person in cardiac arrest? If yes, that dominates everything else and every other observation is irrelevant until it's handled. Only once that's ruled out do they move down to moderate concerns, and only after that to minor ones.

This strategy works exactly like that, in this order, every single day:

1. **Life-threatening check (overrides everything, checked first, always):** Is the fear gauge (VIX) above 30? Or has the price fallen below its slow 200-day trend line? **Either one, by itself, is enough** — go to cash immediately (real, uninvested dollars — see the note above), no matter what mode you were just in.
2. **Moderate concern (only checked if step 1 didn't fire):** Has the price closed below its faster 50-day trend line for two days running? If so, get defensive — reduce the position.
3. **Minor concern (only checked if steps 1 and 2 didn't fire):** Has the "overheated" gauge (RSI) climbed above 75? If so, trim back a bit as a precaution — you're not sick, just running a slight fever, so a small, reversible adjustment is enough.

### Why this exact order, and why these exact numbers?

- **Why check the fear gauge and the long-term trend line first, and why does either one alone win?** Because these are the two independent ways a real crash shows up, and they don't always show up together. A VIX spike is the market's own alarm bell — often the *fastest* available warning, sometimes firing before the price itself has fully broken down. The 200-day trend line is the backstop for the opposite failure mode: a slow, grinding bear market where nobody panics (VIX stays sleepy) but the price just keeps quietly sliding for months. Requiring *either* one (not both) means you're covered whether the danger arrives as a sudden shock or a slow leak.

- **Why does the moderate check require two days below the 50-day line, instead of pulling back on day one?** Because a single bad day is often just noise — the price dips below its average and bounces right back the next day. If the strategy reacted to every single-day dip, it would sell today and rebuy tomorrow constantly, paying a trading fee and a worse price both times for nothing. Waiting for two consecutive days filters out one-day noise while still reacting quickly to a real, sustained weakening.

- **Why does the minor "overheated" check only trim a little, rather than sell out?** Because being overheated isn't the same as being wrong — a stock that's risen fast can easily keep rising. Since there's no confirmed danger (steps 1 and 2 didn't fire), the response is proportionate: bank a little risk off the table in case this is the top, but don't abandon the position in case it isn't.

### A real worked example: the COVID crash, February–April 2020

This is the cleanest real illustration in the dataset the strategy was tested against. (Numbers below are rounded/illustrative to show the *mechanism* clearly, not a precise reprint of the exact daily backtest output.)

- **Mid-February 2020:** Markets are calm. Price is comfortably above both its 50-day and 200-day trend lines. VIX is sitting in the mid-teens. RSI is unremarkable. The strategy sits in **Fully Invested**, doing nothing, day after day — this is the boring, correct state almost all of the time.
- **Around February 24, 2020:** COVID fear starts hitting markets. VIX starts climbing sharply — within about a week it rockets from the high-teens into the 40s, then higher. The instant VIX crosses 30, **step 1 fires immediately.** It doesn't matter that the price hasn't even dropped below its 200-day line yet, and it doesn't matter that the strategy was happily Fully Invested the day before — the fear gauge alone is enough. **Result: straight to Cash**, in a single day, well before the worst of the crash had even happened.
- **Through March 2020:** The S&P proceeds to fall roughly 34% peak-to-trough over about a month, and VIX hits an all-time record above 80. The strategy is sitting in cash through the worst of it, missing the crash entirely — this is the entire point of step 1 existing.
- **Late March into April 2020:** VIX starts cooling back down as markets stabilize, and price begins recovering. But re-entry from Cash is deliberately strict: it needs the price back above its 200-day *and* 50-day trend lines, *and* VIX back below 30, **all three at once.** In practice this means the strategy does not buy back in on the very first up-days of the bounce — it waits until the recovery is confirmed on multiple fronts. This costs some of the earliest, fastest gains off the bottom, but it protects against the alternative: jumping back in during what turns out to be a "dead cat bounce" (a fake rally inside a still-ongoing crash), then getting whipsawed straight back into another loss. The strategy is choosing to be late and sure over early and wrong.

### The two-fund version works the same way, just doubled up

A more refined variant of the same nurse splits the money into two independent patients instead of one — a slice tracking the S&P 500 and a slice tracking the Nasdaq — and runs the identical triage logic on each separately. That means it's possible to be defensive on the Nasdaq slice (say, tech is looking shaky) while staying fully invested on the S&P slice, instead of an all-or-nothing call. The life-threatening check (VIX + the S&P's 200-day line) still overrides both slices at once, because a real crash doesn't spare one slice and not the other.

### What it actually made and lost

Roughly **21% a year** on average — but with brutal dips along the way, including a stretch where it lost close to **40%** peak-to-trough. That's the cost of the leveraged (3x) funds it holds while Fully Invested: both the gains and the crash-protection timing errors get amplified.

### A wiring gap worth knowing about

The live dashboard has extra sliders for this nurse — a separate trend-line length for the life-threatening check, a separate one for the moderate check, the overheated-gauge averaging period, exact trim percentages, and so on. When you play with these sliders live in the browser, they *do* change the results you see. But if the identical request instead goes through the strict, official server-side calculation, several of those specific sliders are quietly ignored, and hard-coded defaults are used no matter what the slider shows. Two copies of the same rulebook exist side by side, and they don't fully agree on which knobs actually do anything — worth pinning down which one produced any number the client shows you.

---

## 6. The Autopilot with a Circuit Breaker (three later, simplified versions)

Picture a car's cruise control: it steers on one simple input (is the road heading uphill or downhill?) and holds a steady speed accordingly. Bolted onto it, completely separately, is an emergency system that has nothing to do with steering — an accident-detection system that, once triggered, forces the car to slow to a crawl for a fixed, non-negotiable period, regardless of what the road looks like afterward.

This version works exactly like that, and — worth saying plainly — **it never looks at the fear gauge (VIX) at all.** Just two checks, in this order:

1. **The circuit breaker (checked first, always wins):** Look at the highest point your account has reached in the last 60 trading days (roughly the last 3 months, not ever — a *recent* peak, not the all-time high). If you're now more than 15% below that recent peak, trip the breaker: force a near-total pullback to safety for a **fixed 20 trading days.** It does not check whether the market has calmed down or turned around during that window — it simply waits out the full 20 days no matter what, then re-evaluates from scratch.
2. **The steering (only reached if the breaker isn't tripped):** Compare the 50-day trend line to the 200-day trend line. Short-term average above the long-term average → lean in, go **Aggressive**. Short-term below long-term → pull back, go **Defensive**. No in-between gear.

### Why these exact choices?

- **Why measure the drawdown from a *recent* (60-day) peak instead of the all-time peak?** Because if it measured against the all-time high, once the strategy has already been through a real bear market, it would stay "in violation" of that old high for a very long time — potentially locking itself out of ever cleanly re-engaging again, since it might take years to get back to the old peak. Measuring against a recent, rolling peak means the bar resets to current conditions, so the circuit breaker can trip, cool down, and become usable again within the same downturn instead of staying jammed.

- **Why a fixed 20-day cooldown instead of waiting for things to "look better"?** Because "looks better" is a judgment call that can flip back and forth during a choppy, bottoming market — the strategy could get faked out repeatedly, jumping back in and getting knocked straight back down. A fixed waiting period forces patience. The trade-off is explicit: it will sometimes sit out the first part of a genuine recovery, in exchange for not repeatedly whipsawing itself during a fake one.

- **Why no VIX at all, when the other strategy leans on it heavily?** Because this approach is deliberately built around only the most reliable, hardest-to-fake-out signal — your own price trend and your own account's real drawdown — rather than a separate market gauge that can occasionally spike on a headline scare that turns out to be nothing. It trades away the "early warning" speed of a fear gauge for consistency: it reacts to what's actually happened to the money, not to how scared the news suggests everyone is.

- **Why is the steering rule just two states (Aggressive/Defensive) instead of finer gradations?** Because it was tried the more complicated way first — in-between states meant to catch "somewhat bullish but not fully confident," or "recovering but not out of the woods yet" — and according to this project's own testing notes, every one of those in-between states lost money in every iteration they tried. Every extra state is an extra place the rule can flip back and forth (called "whipsawing"), and every flip costs a trading fee and a slightly worse execution price. Stripping it down to a plain binary call turned out to perform better than trying to be cleverer about it.

### A real worked example: the 2022 slow bleed

2022 is a useful contrast to the COVID example, because it was a slow, grinding decline rather than a single panic spike — a good test of a system that ignores the fear gauge entirely.

- **Early January 2022:** Markets are near all-time highs. The 50-day trend line is above the 200-day trend line, so the strategy is sitting **Aggressive** — no drama, no fear spike anywhere on the horizon.
- **Through the first several months of 2022:** Interest-rate worries push prices down gradually — no single crash day, no VIX spike anywhere near the COVID extreme, just a steady grind lower over months. Eventually the 50-day trend line crosses below the 200-day trend line. The moment that crossover happens, the steering rule flips the strategy to **Defensive** — it doesn't need a fear spike to notice something's wrong, because it was never watching for one in the first place. This is the whole point of building it this way: a slow bleed with no panic still gets caught.
- **If the account's own balance also happened to fall more than 15% below its own trailing 60-day peak at any point during this stretch**, the circuit breaker would trip independently of the trend call, forcing the fixed 20-day pullback described above — a second, independent layer of protection triggered by *your results*, not by the market's mood.

### The asset classes, in plain English

Everything the Autopilot ever holds is one of these four things:

- **UPRO** — a fund that aims to move **3 times** the daily move of the S&P 500. If the S&P 500 rises 1% in a day, UPRO aims to rise about 3%. If the S&P falls 1%, UPRO aims to fall about 3%. Think of it like buying the S&P 500 with a large stack of borrowed money on top of your own — you win big when it goes your way, and lose big, fast, when it doesn't. The fund itself handles the "borrowing" internally; you don't take out a loan yourself, but the effect on your gains and losses is the same as if you had.
- **TQQQ** — the exact same idea as UPRO, but tracking the Nasdaq-100 (a tech-heavy index) instead of the S&P 500, also at roughly 3x the daily move.
- **SPY** — a completely ordinary, unleveraged fund that just tracks the S&P 500 one-for-one. If the index rises 1%, SPY rises about 1%. This is the "plain" version of UPRO.
- **QQQ** — the ordinary, unleveraged version of TQQQ — tracks the Nasdaq-100 one-for-one, no borrowing effect.
- **SHY** — a fund holding short-term U.S. government bonds (roughly 1-3 years to maturity). Government-backed and short-term means its price barely moves day to day — it's the closest thing to "sitting still" while still technically being an investment. This is the fund used any time this document says the Autopilot went "cash-like" (see the note in section 3 — it is not literal cash).
- **GLD** — a fund that tracks the price of physical gold. Included as a second, different kind of safe haven alongside SHY — gold doesn't always move with the stock market, and sometimes moves the opposite way during a stock downturn, so holding a bit of it alongside SHY isn't just "more safety," it's a *different kind* of safety that doesn't rely on the same conditions holding up.

**Why the leveraged funds are riskier than "just 3 times the return"** — this is a genuinely important, easy-to-miss mechanic, not just intuition: because the 3x tracking resets every single day, a choppy, sideways market can quietly bleed value even if the underlying index ends up completely flat. For example, if the S&P rises 10% one day and then falls a little under 10% the next day (enough to land back exactly where it started), UPRO doesn't land back where it started — following the 3x math down as well as up, it ends up **lower** than where it began, despite the index going nowhere. This effect (often called "volatility decay") is a big part of why the leveraged versions of the Autopilot can lose money over a stretch where the market was merely choppy rather than in an outright crash, and why holding a leveraged fund for a long, uncertain "Aggressive" stretch is a meaningfully different bet than holding the plain index for the same stretch.

### Exact allocations, mode by mode, and the reasoning behind each split

| Version | Holds when Aggressive | Holds when Defensive | Holds when circuit-breaker is tripped | Holds during initial warm-up |
|---|---|---|---|---|
| Oldest, labeled "7-state" | 100% UPRO | 100% SHY | 100% SHY | 100% SHY |
| Leveraged | 60% UPRO + 40% TQQQ | 60% SHY + 40% GLD | 100% SHY | 100% SHY |
| Non-leveraged | 60% SPY + 40% QQQ | 60% SHY + 40% GLD | same as Defensive (60% SHY + 40% GLD) | 100% SHY |

("Warm-up" is a small technical detail worth knowing about rather than a real strategic choice: the trend check needs 200 days of price history before it can even compute its long-term average, so for the very first stretch of any backtest — before there's enough data to say anything — the Autopilot just parks everything in SHY by default, rather than guessing.)

**Why 60% UPRO / 40% TQQQ in "Aggressive," instead of 100% into one of them or a 50/50 split?** The S&P 500 (which UPRO tracks) is a broader, more diversified slice of the market — hundreds of large companies across many industries. The Nasdaq-100 (which TQQQ tracks) is narrower and much more concentrated in technology and a handful of large growth companies. Giving the S&P side the larger 60% weight means the "core" of the aggressive position is the broader, steadier index, while the 40% Nasdaq tilt is there to add extra growth exposure on top — a bet that tech-heavy growth stocks will often outperform during good times, without letting that concentration dominate the whole position. It's a deliberate tilt toward "mostly broad market, partly a growth bet," not an even split and not an all-in bet on either index alone. (Note: unlike the mode-switching thresholds covered earlier, this particular 60/40 split is not an adjustable dial anywhere on the dashboard — it's fixed in the underlying logic.)

**Why 60% SHY / 40% GLD in "Defensive," instead of just SHY alone?** SHY alone protects against one specific thing: stock market declines, by simply not being in stocks. Gold historically doesn't move in lockstep with either stocks or short-term bonds — it sometimes rises specifically *because* investors are nervous about stocks, which is exactly the environment "Defensive" mode is meant for. Blending in 40% gold means the defensive posture isn't just "sit still," it's "sit still, plus hold something that has its own chance of actually gaining ground while stocks are struggling." It's a diversified safe haven, not a single-asset one.

**Why does the leveraged version drop gold entirely (100% SHY) once the circuit breaker actually trips, instead of keeping the same 60/40 defensive blend?** This is a deliberate extra layer of caution reserved only for the leveraged version, and it reveals something about how the two later versions were designed differently. The circuit breaker only fires after a confirmed, serious drawdown has already happened — by that point, the leveraged version's own design assumption is that even gold's modest day-to-day price swings are volatility worth avoiding entirely, so it retreats one step further than its normal Defensive mode, all the way to the single steadiest asset available. The non-leveraged version, by contrast, treats its circuit-breaker mode as no different from ordinary Defensive — because its normal ups and downs are far gentler to begin with (it isn't running 3x leverage in the aggressive mode), its own defensive blend was judged safe enough to use even during a confirmed drawdown event, so there was no need to define a stricter fallback.

**Why does the oldest version skip all of this blending and just go 100% UPRO or 100% SHY, with nothing in between?** This is simply the least refined of the three designs — a single fund on, or a single fund off, no partial positions and no second safe-haven asset at all. It's a useful data point on how the strategy evolved: the later two versions (leveraged and non-leveraged) both added the SHY+GLD blend as a refinement over this all-or-nothing approach, which is one reason the oldest version also produced the worst results of everything tested.

- The oldest version: barely any return over the test period, and the single worst crash of anything tested — down almost **74%** at its lowest point.
- The leveraged version: a modest overall loss for the period tested, with a drawdown around **-46%.**
- The non-leveraged version: the gentlest ride of all five strategies tested — about **9% a year**, worst drawdown only around **-25%.** Smaller headline number, but by "return earned per unit of risk taken" (a standard, fairer way to judge this), it's arguably the best-designed of the five.

### Two catches worth knowing before you compare this against a client's description

1. **The richer version this was meant to be doesn't actually run.** This autopilot's own design notes describe a more elaborate 5-gear idea, where the "overheated" gauge and the choppiness gauge would fine-tune position size — for example, "only go fully Aggressive if RSI also confirms strength," or "detect choppy, high-volatility days and automatically downgrade out of leverage." That richer version was deliberately simplified away after testing reportedly showed the extra in-between gears kept losing money — see the reasoning above. The "overheated" and "choppiness" gauges are still calculated every single day and still appear as adjustable sliders on the dashboard, but nothing in today's actual decision logic reads them anymore. Moving those two sliders currently changes nothing. If a client describes this version using RSI- or volatility-based reasoning, that description matches an earlier design intent, not the code running today.
2. **The "7 distinct modes" label on the oldest version is a leftover, not a description of what runs.** When you actually execute that version today, it uses the exact same two-rule steering-plus-circuit-breaker logic described above, just holding one fund at a time instead of a blend. The seven evocative mode names are inherited from an earlier iteration; the saved results for that version were most likely produced by this simpler logic, not by seven distinct, individually-tuned modes.

## 7. The headline trade-off, in one paragraph

Across everything tested, one pattern holds up: **the versions using 3x-leveraged funds made more money on paper (around 21-22% a year) but also lost far more during bad stretches (down almost 40%, in one case down almost 75%). The version using plain, ordinary funds made less (around 9% a year) but never fell nearly as hard (down about 25% at worst) — and when you adjust for how much risk was actually taken, it's arguably the better strategy, not the weaker one.** Nobody should look at the big return number without the big loss number sitting right next to it.

## 8. What's built vs. what's just an idea on paper

- All five versions are fully coded and tested against several years of real historical price data (2020-2025).
- A deeper testing toolkit also exists — checking "does this still work in a different time period," "could this just be luck," "how sensitive is this to small setting changes" — but it only runs from a command line today; it isn't connected to the website.
- The live website shows the numbers, lets you tweak the dials, and shows a day-by-day log of what the strategy would have done. It does not place real trades or touch a real brokerage account.
- The live version currently runs on free, publicly available price data as a stand-in for the paid data source it was originally validated against, so live numbers may not perfectly match the "official" saved results.

## 9. What to listen for in the client meetings

- Do they describe "the fear index" / VIX as a trigger? → that's the Triage Nurse.
- Do they describe "trend plus a drawdown stop" with no mention of VIX? → that's the Autopilot.
- If they describe both in one breath, find out whether they see it as one strategy evolving over time, or two separate, parallel offerings.
- If they describe RSI or volatility actually changing position sizes in the Autopilot, or describe seven genuinely distinct, individually-behaving modes for the oldest version — that's a mismatch worth resolving; the code running today does neither.
- Ask directly about the worst-performing version (the ~74% drawdown one). Do they consider it an abandoned dead end, or something still "in play"? The answer tells you a lot about how carefully the strategy's history has been tracked on their side.
- Confirm which calculation path (the live browser version, or the strict server version) any numbers they show you actually came from — for the Triage Nurse, those two paths don't fully agree on which sliders matter.
