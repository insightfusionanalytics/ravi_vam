# Ravi VAM Strategy Platform — Project Overview

*Written after reading through all the code, the deployment setup, and the project history. This is one complete document — everything you need is here.*

## What this is

It's a **tool for testing a stock-trading idea on past data.** It does not trade real money. It does not touch a real brokerage account. It just answers the question: "if I had used this strategy over the last few years, what would have happened?" — and shows the answer on a web page with charts and a trade-by-trade log.

## The trading idea, simply

The strategy looks at three things every day:

1. **Is the market trending up or down?** (using moving averages — basically, is the price higher or lower than its recent average)
2. **Is the market "overheated" or "cooled off"?** (a momentum reading called RSI)
3. **Is there fear in the market?** (the VIX, sometimes called the "fear index")

Based on those three things, it automatically moves money between "fully invested," "half invested," and "all cash." When things look healthy, it goes big. When things look shaky, it pulls back. When things look scary, it goes to cash.

There are 5 versions of this idea saved in the project, each a little different — some use ETFs that move 3x as much as the market (bigger gains, but bigger losses too), some use plain, normal ETFs (smaller gains, but much smaller losses).

**The most important thing to know:** the versions using the 3x ETFs showed the best returns in testing (about 21-22% a year) — but also the worst losses along the way, up to -39%, and one version fell as much as -74% at its worst point. The plain, non-leveraged version made less money overall (about 9% a year) but never fell nearly as hard (-25% at worst) and actually came out looking better on a "return for the risk taken" basis. So: bigger numbers, bigger pain, both ways. Nobody should look at the 21% number without also looking at the -39% number.

There's also some solid testing tooling already built into the code (stuff that checks the strategy against different time periods, runs random simulations, checks if results are just luck) — but right now none of it is hooked up to the website. It only works if someone runs it manually from the command line.

## How it's built

It's **one single program**, not two separate ones. A lot of similar projects have a separate "frontend" (the website you see) and "backend" (the engine doing the work), talking to each other over the internet. This project doesn't — one program does both jobs at once: it runs the calculations AND serves the web page. That's simpler, and it's fewer things that can break.

It gets its stock price data from one of two places: a paid data source called DataBento (the "proper" source), or, if that's not available, it automatically downloads similar data for free from Yahoo Finance instead. **Right now, it's running on the free Yahoo Finance data**, not the paid one — worth knowing, since the numbers might not match exactly what the strategy was originally tested on.

## What's been done recently

The project existed and ran on a laptop, but wasn't properly live on the internet yet. Here's what happened to get it there:

1. Got it running and double-checked it worked correctly.
2. Wrote the full setup guide and all the server configuration needed to put it online.
3. Put it live at **https://backtestravi.insightfusionanalytics.com** — and along the way, fixed a string of real problems:
   - The server was told to look for the code in the wrong folder.
   - The program was set up to run under the wrong user account.
   - The program tried to use a "door" (technical term: port) into the server that was already being used by something else.
   - There were **two different sets of instructions** telling the server how to handle this website at the same time, left over from an earlier, unfinished attempt — they were quietly fighting each other, so part of the website worked and part didn't. Cleaned that up to just one clear set of instructions.
   - The website's address (DNS) was pointing to the wrong server entirely. Fixed.
4. As of now, it's confirmed **live and working** — the page loads, the data loads, and if it ever crashes, it's set up to automatically restart itself.
5. There's a draft proposal (not started yet) for two next steps: (a) a tool that automatically tries thousands of setting combinations to find the best one, instead of adjusting sliders by hand, and (b) ongoing hosting/maintenance so it doesn't quietly break again later the way the first deployment attempt did.

## My honest opinion

**What's good:**
- The idea itself is easy to understand — three simple checks, a handful of "modes" to move between. Nothing overly complicated or mysterious.
- There's genuinely good testing work already built in (even if it's not hooked up to the website yet).
- Making it one single program instead of two was a smart move — it's simpler to run and already avoided a bunch of headaches the earlier, split-into-two-pieces version ran into.
- Adding a brand-new version of the strategy is easy — it's just a settings file, no need to touch the code.

**What's not so good, and worth fixing:**
- **There's no automatic testing at all.** Every bug that's been found so far was found by a person manually clicking around and noticing something was wrong. For a tool whose whole job is producing trustworthy numbers, that's a gap.
- **Two of the calculation scripts are almost exact copies of each other.** If a mistake is found and fixed in one, it's easy to forget to fix the same mistake in the other.
- **When something goes wrong, the website currently shows the raw technical error to whoever's looking.** Not dangerous, but sloppy — worth cleaning up.
- **It's running on the free backup data source, not the intended paid one** — so the live numbers might not perfectly match the "official" test results saved in the settings files.
- **It's all running on one single server with no backup.** If that server has a problem, the whole site goes down until someone fixes it — nothing catches that automatically or notifies anyone.
- **The strategy that's marked as the "main" one uses 3x leveraged ETFs**, which historically saw drops of about -39%. Anyone making real decisions from this should be shown that number right next to any return number, not just the return number.

## Where things stand right now

It's live, it's working, and it'll restart itself if it crashes. Nobody gets notified if it goes down, though — someone has to check and notice. There's no automatic testing and no backup server. The two proposed next steps (auto-tuning, ongoing maintenance) are written up but haven't started yet. That's the real, current state — not a best-case description of it.
