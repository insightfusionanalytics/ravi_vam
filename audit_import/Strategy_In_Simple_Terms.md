# The Ravi VAM Strategy — Explained Simply

## What is this, in one sentence?

It's a rules-based trading system that owns **leveraged stock market ETFs** (like a 3x-leveraged version of the S&P 500) when the market is healthy, sells down to cash when the market looks dangerous, and — in the more advanced versions — makes side bets that profit *from* a crash while it's happening.

Think of it like a smart thermostat for a portfolio: it doesn't try to predict the future, it just reacts to a small set of temperature readings (price trends, momentum, and fear levels) and moves between a handful of pre-defined settings ("states") automatically, every single day.

---

## The building blocks

Four instruments do all the work:

| Instrument | What it is | Role |
|---|---|---|
| **UPRO** | 3x leveraged S&P 500 ETF | Main growth engine |
| **TQQQ** | 3x leveraged Nasdaq-100 ETF | Secondary growth engine (adds tech exposure) |
| **SPXU** | 3x *inverse* S&P 500 ETF | Bets that go up when the market crashes |
| **SVIX** | Inverse volatility ETF | A smaller, second "crash insurance" bet |
| **SGOV** | Short-term Treasury bill ETF | Where uninvested cash sits and earns interest instead of doing nothing |

And three signals tell the system what the market is doing:

1. **The 200-day average price** — the long-term trend line. Below it = the market is in a downtrend.
2. **The 50-day average price** — the medium-term trend line. A quicker warning sign than the 200-day.
3. **RSI (a momentum gauge, 0–100)** — measures whether the market has gotten "too hot" too fast (overbought) or has cooled off.
4. **VIX (the market's "fear index")** — measures how nervous/panicked investors are. A VIX spike above 30 is treated as a fire alarm.

None of these are exotic — they're standard technical indicators. The strategy's real innovation is the *order* in which it checks them and the specific thresholds it reacts to.

---

## The decision order (this is the heart of the strategy)

Every day after the market closes, the system checks things in a strict pecking order — like a triage nurse, not a democracy:

1. **First: is there a "kill switch" emergency?** (VIX spiking / market deeply broken) → If yes, override everything else and get defensive. This check always wins, no matter what the trend or momentum say.
2. **Second: what does the trend say?** (Is price above or below the 50-day and 200-day averages?) → This decides the broad posture: fully invested, partially invested, or in cash.
3. **Third: what does momentum say?** (Is RSI overbought?) → This fine-tunes the position size — trims a bit if things have run up too fast, even in an uptrend.

The system never blends these into one score — it's a strict "if the top rule fires, stop and obey it" hierarchy. This is deliberate: it prevents the strategy from being fully invested during a genuine panic just because momentum still looked fine that morning.

---

## The "states" — like gears in a car

Rather than constantly nudging the portfolio by small amounts, the strategy sits in one of a handful of fixed **states** and only shifts gears when a signal crosses a threshold. This is called **state-based logic**, and it's intentional: it avoids constantly whipsawing the account with tiny trades every time a number wiggles by 0.1%.

The core (simplest) version — **Step 1** — has four gears:

| State | Allocation | When you're in it |
|---|---|---|
| **Full Bull** | ~99% in UPRO | Market is above its 200-day average and hasn't overheated |
| **Trimmed Bull** | 75% in UPRO | Still in an uptrend, but RSI shows it's gotten overheated (>75) |
| **Defensive** | 50% in UPRO | Price has dropped below its 50-day average — early warning |
| **Cash** | 0% (or later, short positions) | Price is below its 200-day average — trend is broken |

**Step 2** is the same idea but splits the "growth" sleeve into 75% UPRO / 25% TQQQ, which adds a few more in-between gears (e.g., "S&P is weak but Nasdaq is fine"), for six or seven states total instead of four.

**Step 3 (Predatory Short)** adds a fifth gear on top of Cash: if the market is *both* below its 200-day average **and** the fear index (VIX) is above 30, the strategy takes half of the idle cash and bets on the market falling further (via SPXU), rather than just sitting on the sidelines.

**Step 4 (Safety Valve)** layers in one more small, similar crash-insurance bet using SVIX during the same cash periods, so two smaller short bets can run side-by-side while the rest of the money sits safely in cash/SGOV earning interest.

---

## Why sizes shrink as things get worse

Notice the position sizes step down in stages — 99% → 75% → 50% → 0% — rather than snapping straight from all-in to all-out. This is intentional risk management: it means the strategy takes profits/de-risks gradually as warning signs accumulate, instead of making one big all-or-nothing bet on a single signal. It also avoids "whipsaw" — getting faked out by a one-day dip and jumping to cash, only to miss the rebound the next day.

---

## What actually happens mechanically, day to day

1. **4:00 PM (market close):** the system looks at that day's closing prices and indicators, and decides: "based on the rules, which state should I be in tomorrow?"
2. **Next morning, ~30 minutes after open:** if the target state is different from today's, it places trades to move the portfolio to the new allocation.
3. It accounts for realistic frictions: **trading costs** (a small commission per trade) and **slippage** (the fact that you don't get the exact price you saw yesterday — this is set worse on high-fear days, since fast markets have wider price gaps).

This one-day delay (decide at close, trade the next morning) is deliberate — it's meant to reflect how a real person or system would actually be able to execute, rather than assuming a magical same-second trade.

---

## How has it performed (backtested, not live)?

Using ~14 years of historical data (2011–2025):

- **Step 1 (UPRO only):** roughly **13% per year**, turning $100k into about **$574k**, but with a rough patch where it could have fallen ~51% peak-to-trough.
- **Step 2 (UPRO + TQQQ):** roughly **17% per year**, turning $100k into about **$914k** — better returns, similar (slightly worse) worst-case drawdown.
- **Step 3 (the crash-betting overlay alone):** a net **loser** on its own (about -3%/year) — it's meant to be a hedge that pays off in specific sharp crashes, not a standalone money-maker, and historically it lost more often than it won.
- **Combined portfolio (Steps 1–4 together):** about **14%/year**, turning $100k into roughly **$653k**, with the crash-bet overlays smoothing some — but not all — of the pain during the worst drawdowns.

For comparison, plain buy-and-hold S&P 500 (no leverage) returned roughly **14% CAGR** over the shorter 2020-2025 test window — so the leveraged strategy's edge over simple buy-and-hold exists, but isn't dramatic once you account for the much bigger drawdowns leverage brings.

**Important honest caveat:** these are backtests on historical data, not live trading results. Leveraged ETFs (UPRO, TQQQ, SPXU) are aggressive, high-decay instruments — they can lose value even faster than "3x the market's move" over long stretches. A -50% peak-to-trough drawdown is a real, painful possibility this strategy has already shown in testing, not just a theoretical tail risk.

---

## What's still unresolved (per the audit notes)

The current build is based on transcripts of conversations with the client (Ravi), and several exact parameters were **not explicitly confirmed** and are currently "best guess" defaults that need his sign-off:

- The exact RSI settings (what period, and the exact rebuy threshold)
- Whether the "kill switch" needs *just* a VIX spike, or VIX spike *and* a broken trend together
- The exact VIX level to re-enter the market after a crash (two different numbers were mentioned in meetings)
- What causes the crash-bet (SPXU) position to be closed
- The entire "Safety Valve" (SVIX) sub-strategy's exact rules — this was only briefly mentioned, never fully specified

In short: the framework and the big rules (trend + momentum + fear, checked in that priority order, moving through fixed allocation "gears") are solid and confirmed. Some of the fine print — the exact numeric knobs — still needs the client to confirm before this is fully locked down for real-money use.
