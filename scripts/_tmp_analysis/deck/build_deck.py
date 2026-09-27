"""Generates every slide .html file for the deck, plus deck.json."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SLIDES = ROOT / "project" / "slides"
SLIDES.mkdir(parents=True, exist_ok=True)
SVG = ROOT / "svgs"

# ---- palette / type ----
BG = "#0A0B0F"
SURFACE = "#14161B"
SURFACE2 = "#1B1E24"
BORDER = "#262A31"
TEXT = "#EDEEF0"
DIM = "#9098A3"
MUTED = "#5C6470"
ACCENT = "#4C7DFF"
ACCENT_DIM = "#1B2340"
GREEN = "#1FAD5C"
GREEN_DIM = "#123322"
RED = "#DC4646"
RED_DIM = "#331616"
YELLOW = "#C99A2E"
YELLOW_DIM = "#332A12"

SANS = "'IBM Plex Sans', Arial, sans-serif"
MONO = "'JetBrains Mono', 'Courier New', monospace"

def svg(name):
    return (SVG / f"{name}.svg").read_text()

def footer(label, page):
    return f'''<p style="position:absolute; left:128px; bottom:64px; font-size:24px; color:{MUTED}; font-family:{SANS}">{label}</p>
<p style="position:absolute; right:128px; bottom:64px; font-size:24px; color:{MUTED}; font-family:{MONO}">{page}</p>'''

def eyebrow(text, color=ACCENT):
    return f'<p style="font-size:26px; font-weight:700; letter-spacing:2px; text-transform:uppercase; color:{color}; margin:0">{text}</p>'

def write(slide_id, body, data_section=None, transition="fade"):
    attrs = f'id="{slide_id}"'
    if data_section:
        attrs += f' data-section="{data_section}"'
    attrs += f' data-transition="{transition}"'
    content = f'<section {attrs} style="{body["style"]}">\n{body["inner"]}\n</section>\n'
    (SLIDES / f"{slide_id}.html").write_text(content)

BASE = f"background:{BG}; color:{TEXT}; font-family:{SANS}; display:flex; flex-direction:column; padding:128px 128px 160px; gap:40px"

# ============================================================ 1. COVER
write("cover", {
    "style": f"background:linear-gradient(135deg, {BG} 0%, #0d1220 100%); color:{TEXT}; font-family:{SANS}; display:flex; flex-direction:column; justify-content:center; padding:160px",
    "inner": f'''
<p style="font-size:28px; font-weight:700; letter-spacing:3px; text-transform:uppercase; color:{ACCENT}; margin:0 0 32px">RAVI VAM Strategy Platform</p>
<h1 style="font-size:88px; font-weight:700; line-height:1.12; margin:0; max-width:1500px">Parameter Optimization &amp;<br>Whipsaw Investigation</h1>
<p style="font-size:34px; color:{DIM}; margin:48px 0 0; max-width:1300px; line-height:1.5">What was tuned, what was tried to fix the drawdown trade-off, what held up under honest testing &mdash; and what's left to decide.</p>
<div style="position:absolute; left:160px; bottom:120px; display:flex; flex-direction:column; gap:8px">
<p style="font-size:24px; color:{MUTED}; margin:0">Prepared for Ravi Mareedu &amp; Sudhir Vyakaranam</p>
<p style="font-size:24px; color:{MUTED}; margin:0; font-family:{MONO}">Insight Fusion Analytics &middot; September 26, 2026</p>
</div>
<div style="position:absolute; right:0; top:0; width:640px; height:1080px; opacity:0.5">
<svg width="640" height="1080" viewBox="0 0 640 1080" xmlns="http://www.w3.org/2000/svg" aria-label="Decorative equity curve">
<path d="M 40,820 L 90,790 L 140,860 L 190,760 L 240,700 L 290,730 L 340,600 L 390,640 L 440,480 L 490,520 L 540,360 L 590,300" fill="none" stroke="{ACCENT}" stroke-width="4" opacity="0.55"/>
<path d="M 40,900 L 90,880 L 140,910 L 190,860 L 240,840 L 290,850 L 340,780 L 390,800 L 440,700 L 490,720 L 540,640 L 590,600" fill="none" stroke="{GREEN}" stroke-width="4" opacity="0.4"/>
</svg>
</div>'''
})

# ============================================================ 2. EXEC SUMMARY
write("exec-summary", {
    "style": BASE,
    "inner": f'''
{eyebrow("Executive Summary")}
<h2 style="font-size:60px; font-weight:700; margin:0; line-height:1.15; max-width:1600px">Six tuning passes were run. One survived honest testing. Three more attempts were made to fix its remaining flaw &mdash; none of them held up either.</h2>
<div style="display:flex; gap:32px; margin-top:16px">
<div style="flex:1; background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:40px; display:flex; flex-direction:column; gap:12px">
<p style="font-family:{MONO}; font-size:88px; font-weight:700; color:{GREEN}; margin:0">1 / 6</p>
<p style="font-size:26px; color:{DIM}; margin:0">tuning passes beat doing nothing, on data never seen during search</p>
</div>
<div style="flex:1; background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:40px; display:flex; flex-direction:column; gap:12px">
<p style="font-family:{MONO}; font-size:88px; font-weight:700; color:{RED}; margin:0">0 / 3</p>
<p style="font-size:26px; color:{DIM}; margin:0">whipsaw-mechanism fixes improved on that one result</p>
</div>
<div style="flex:1; background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:40px; display:flex; flex-direction:column; gap:12px">
<p style="font-family:{MONO}; font-size:88px; font-weight:700; color:{ACCENT}; margin:0">21.2%</p>
<p style="font-size:26px; color:{DIM}; margin:0">CAGR of the one adopted candidate, vs. 15.5% today</p>
</div>
</div>
<p style="font-size:26px; color:{MUTED}; margin-top:8px; max-width:1500px">That is a real, useful outcome, not a failed exercise &mdash; a search (or a fix) that only ever reports flattering numbers isn't one you can trust.</p>
{footer("Executive Summary", "01")}'''
}, data_section="Six passes run, one real result")

# ============================================================ 3. METHODOLOGY
write("methodology", {
    "style": BASE,
    "inner": f'''
{eyebrow("Methodology")}
<h2 style="font-size:56px; font-weight:700; margin:0">What Was Tested, and How</h2>
<div style="display:grid; grid-template-columns:1fr 1fr 1fr 1fr; gap:28px; margin-top:8px">
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; flex-direction:column; gap:12px">
<p style="font-size:22px; text-transform:uppercase; letter-spacing:1px; color:{MUTED}; margin:0">Search / tune period</p>
<p style="font-size:30px; font-weight:600; font-family:{MONO}; margin:0">2011&ndash;2021</p>
</div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; flex-direction:column; gap:12px">
<p style="font-size:22px; text-transform:uppercase; letter-spacing:1px; color:{MUTED}; margin:0">Sealed holdout</p>
<p style="font-size:30px; font-weight:600; font-family:{MONO}; margin:0">2022&ndash;2025</p>
</div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; flex-direction:column; gap:12px">
<p style="font-size:22px; text-transform:uppercase; letter-spacing:1px; color:{MUTED}; margin:0">Scoring</p>
<p style="font-size:26px; font-weight:600; margin:0">Median Calmar across 4 blocks, minus half the spread</p>
</div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; flex-direction:column; gap:12px">
<p style="font-size:22px; text-transform:uppercase; letter-spacing:1px; color:{MUTED}; margin:0">Guardrails</p>
<p style="font-size:26px; font-weight:600; margin:0">Max drawdown, min trades, defensive-state usage, cash-time ceiling</p>
</div>
</div>
<div style="background:{SURFACE2}; border-left:6px solid {ACCENT}; border-radius:12px; padding:32px 40px; margin-top:8px">
<p style="font-size:27px; color:{TEXT}; margin:0; line-height:1.5">Every number that matters in this deck is the <b>sealed</b> result &mdash; performance on data the search never touched while picking anything. Reported once per candidate on principle: a "sealed" period checked repeatedly stops being sealed.</p>
</div>
{footer("Methodology", "02")}'''
}, data_section="Search vs. sealed-holdout discipline")

# ============================================================ 4. HEADLINE RESULTS TABLE
def result_row(strategy, cand, cagr, calmar, maxdd, verdict, is_baseline=False, is_adopt=False):
    # Table cells hold plain text only in this format (no nested tags) --
    # so the verdict is plain colored/bold text on the td itself, not a pill.
    vlabel, vcolor = "", MUTED
    if verdict == "adopt":
        vlabel, vcolor = "ADOPT", GREEN
    elif verdict == "reject":
        vlabel, vcolor = "reject", MUTED
    style_row = f'background:{GREEN_DIM if is_adopt else ("transparent" if not is_baseline else SURFACE2)};'
    txt_color = MUTED if is_baseline else TEXT
    fs = "italic" if is_baseline else "normal"
    return f'''<tr style="{style_row}">
<td style="width:38%; padding:18px 20px; font-size:26px; color:{txt_color}; font-style:{fs}">{cand}</td>
<td style="width:15%; padding:18px 20px; font-size:26px; font-family:{MONO}; color:{txt_color}; text-align:right">{cagr}</td>
<td style="width:15%; padding:18px 20px; font-size:26px; font-family:{MONO}; color:{txt_color}; text-align:right">{calmar}</td>
<td style="width:17%; padding:18px 20px; font-size:26px; font-family:{MONO}; color:{txt_color}; text-align:right">{maxdd}</td>
<td style="width:15%; padding:18px 20px; font-size:24px; font-weight:700; color:{vcolor}; text-align:center">{vlabel}</td>
</tr>'''

write("headline-results", {
    "style": BASE,
    "inner": f'''
{eyebrow("Headline Findings")}
<h2 style="font-size:52px; font-weight:700; margin:0">Sealed-Data Results, All Three Strategies</h2>
<table style="width:1664px; font-family:{SANS}; border-collapse:collapse; margin-top:4px">
<tr style="background:{ACCENT_DIM}">
<th style="width:38%; padding:16px 20px; text-align:left; font-size:22px; color:{ACCENT}">STRATEGY / CANDIDATE</th>
<th style="width:15%; padding:16px 20px; text-align:right; font-size:22px; color:{ACCENT}">CAGR</th>
<th style="width:15%; padding:16px 20px; text-align:right; font-size:22px; color:{ACCENT}">CALMAR</th>
<th style="width:17%; padding:16px 20px; text-align:right; font-size:22px; color:{ACCENT}">MAX DD</th>
<th style="width:15%; padding:16px 20px; text-align:center; font-size:22px; color:{ACCENT}">VERDICT</th>
</tr>
{result_row("s2","UPRO + TQQQ &mdash; current settings","+15.5%","0.511","&minus;30.4%","", is_baseline=True)}
{result_row("s2","UPRO + TQQQ &mdash; tuned, confirmed values locked","+21.2%","0.522","&minus;40.7%","adopt", is_adopt=True)}
{result_row("s2","UPRO + TQQQ &mdash; tuned, confirmed values also opened up","+19.8%","0.483","&minus;41.0%","reject")}
{result_row("s1","UPRO-Only &mdash; current settings","+14.5%","0.508","&minus;28.5%","", is_baseline=True)}
{result_row("s1","UPRO-Only &mdash; tuned, confirmed values locked","+9.5%","0.542","&minus;17.6%","reject")}
{result_row("s3","SPXU Hedge Overlay &mdash; current settings","&minus;3.0%","&minus;0.168","&minus;17.8%","", is_baseline=True)}
{result_row("s3","SPXU Hedge Overlay &mdash; tuned, confirmed values locked","&minus;8.2%","&minus;0.269","&minus;30.6%","reject")}
</table>
<p style="font-size:23px; color:{MUTED}; margin:0">SVIX Safety Valve: not run &mdash; live only since 2022-03-30, too little independent history to validate without look-ahead risk.</p>
{footer("Headline Findings", "03")}'''
}, data_section="One adopted candidate across three strategies")

# ============================================================ 5. FLAGSHIP TRADE-OFF
write("flagship-tradeoff", {
    "style": BASE,
    "inner": f'''
{eyebrow("The One Real Finding")}
<h2 style="font-size:56px; font-weight:700; margin:0">A Real Gain, With a Real Cost</h2>
<div style="display:flex; gap:56px; margin-top:16px; align-items:stretch">
<div style="flex:1; display:flex; flex-direction:column; gap:28px">
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:36px; display:flex; justify-content:space-between; align-items:center">
<p style="font-size:28px; color:{DIM}; margin:0">CAGR</p>
<p style="font-family:{MONO}; font-size:34px; margin:0"><span style="color:{MUTED}">15.5%</span> <span style="color:{DIM}">&rarr;</span> <b style="color:{GREEN}">21.2%</b></p>
</div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:36px; display:flex; justify-content:space-between; align-items:center">
<p style="font-size:28px; color:{DIM}; margin:0">Sharpe</p>
<p style="font-family:{MONO}; font-size:34px; margin:0"><span style="color:{MUTED}">0.625</span> <span style="color:{DIM}">&rarr;</span> <b style="color:{GREEN}">0.696</b></p>
</div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:36px; display:flex; justify-content:space-between; align-items:center">
<p style="font-size:28px; color:{DIM}; margin:0">Max Drawdown</p>
<p style="font-family:{MONO}; font-size:34px; margin:0"><span style="color:{MUTED}">&minus;30.4%</span> <span style="color:{DIM}">&rarr;</span> <b style="color:{RED}">&minus;40.7%</b></p>
</div>
</div>
<div style="flex:1; background:{YELLOW_DIM}; border:1px solid #4a3a15; border-radius:20px; padding:44px; display:flex; flex-direction:column; justify-content:center; gap:20px">
<p style="font-size:24px; font-weight:700; text-transform:uppercase; letter-spacing:1px; color:{YELLOW}; margin:0">The honest catch</p>
<p style="font-size:29px; line-height:1.55; margin:0">The return gain didn't come free. Two independent scoring methods &mdash; including one built specifically to reward shallower drawdowns &mdash; agree this trade-off is structural to the strategy's parameter space, not an artifact of how the search was scored.</p>
<p style="font-size:29px; font-weight:700; margin:0; color:{TEXT}">This is a preference call, not a technical one.</p>
</div>
</div>
{footer("The One Real Finding", "04")}'''
}, data_section="+5.7pp CAGR for materially deeper drawdown")

# ============================================================ 6. DIVIDER: REAL MONEY
write("divider-realmoney", {
    "style": f"background:linear-gradient(135deg, #0d1220 0%, {BG} 100%); color:{TEXT}; font-family:{SANS}; display:flex; flex-direction:column; justify-content:center; padding:160px",
    "inner": f'''
<p style="font-size:28px; font-weight:700; letter-spacing:3px; text-transform:uppercase; color:{ACCENT}; margin:0 0 32px">Part Two</p>
<h1 style="font-size:88px; font-weight:700; line-height:1.15; margin:0; max-width:1500px">Beyond Tuning:<br>Is This Ready for Real Money?</h1>
<p style="font-size:32px; color:{DIM}; margin:40px 0 0; max-width:1300px; line-height:1.5">Not just the headline CAGR &mdash; the strategy's actual win/loss pattern, its behavior, and whether that behavior looks built to hold real capital.</p>'''
}, data_section="Diagnosing the strategy's real-money behavior", transition="push")

# ============================================================ 7. WHIPSAW DOMINANT
write("whipsaw-dominant", {
    "style": BASE,
    "inner": f'''
{eyebrow("Finding", RED)}
<h2 style="font-size:52px; font-weight:700; margin:0">Whipsaw Is the Dominant Loss Pattern</h2>
<p style="font-size:27px; color:{DIM}; margin:0; max-width:1600px; line-height:1.5">Most short defensive/cash episodes reverse almost immediately &mdash; and when they do, the market had usually kept rising the whole time. The <b style="color:{TEXT}">kill switch (zero-day confirmation)</b> is the larger source of false alarms in both strategies, not the 50-day trim.</p>
<div style="margin-top:8px">{svg("whipsaw_split")}</div>
{footer("Strategy Behavior", "05")}'''
}, data_section="Kill switch: larger false-alarm source than the trim")

# ============================================================ 8. NO ASYMMETRIC PROTECTION
write("no-asymmetric", {
    "style": f"background:{BG}; color:{TEXT}; font-family:{SANS}; display:flex; flex-direction:row; padding:128px 128px 160px; gap:64px",
    "inner": f'''
<div style="flex:1; display:flex; flex-direction:column; justify-content:center; gap:28px">
{eyebrow("Finding", YELLOW)}
<h2 style="font-size:52px; font-weight:700; margin:0; line-height:1.15">No Real Asymmetric Protection</h2>
<p style="font-size:27px; color:{DIM}; margin:0; line-height:1.55">A genuinely protective timing system should catch more of the downside than it gives up on the upside.</p>
<p style="font-size:27px; color:{TEXT}; margin:0; line-height:1.55">Here, upside and downside capture vs. raw UPRO are <b>nearly identical</b> in both strategies. The risk reduction is coming mostly from averaging ~25% time in cash, not from smart crash-avoidance.</p>
</div>
<div style="flex:1; display:flex; align-items:center; justify-content:center">{svg("capture")}</div>
{footer("Strategy Behavior", "06")}'''
}, data_section="Upside/downside capture nearly equal")

# ============================================================ 9. DEEP DRAWDOWN
write("deep-drawdown", {
    "style": BASE,
    "inner": f'''
{eyebrow("Finding", MUTED)}
<h2 style="font-size:52px; font-weight:700; margin:0">Still a Deep, Long Drawdown Profile</h2>
<p style="font-size:27px; color:{DIM}; margin:0; max-width:1650px; line-height:1.5">The state machine roughly halves raw leveraged buy-and-hold's drawdown (&minus;75.7% UPRO / &minus;81.7% TQQQ uncontrolled, vs. &minus;44&ndash;49% here) &mdash; real protection. But the flagship still spends long stretches meaningfully underwater:</p>
<div style="margin-top:4px; display:flex; justify-content:center">{svg("underwater")}</div>
<p style="font-size:24px; color:{MUTED}; margin:0">UPRO + TQQQ, full history. Three separate drawdowns exceeded 35&ndash;44%, each taking roughly a year or more to fully recover from.</p>
{footer("Strategy Behavior", "07")}'''
}, data_section="Drawdown halved vs. buy-and-hold, still deep")

# ============================================================ 10. CONCRETE EXAMPLES
write("concrete-examples", {
    "style": BASE,
    "inner": f'''
{eyebrow("Real Episodes, Not Illustrations")}
<h2 style="font-size:52px; font-weight:700; margin:0">Two Concrete Examples of the Pattern</h2>
<div style="display:flex; gap:48px; margin-top:8px">
<div style="flex:1; display:flex; flex-direction:column; gap:16px">
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:24px">{svg("example1")}</div>
<p style="font-size:25px; color:{DIM}; margin:0; text-align:center">UPRO-Only &mdash; exited 2024-08-05, re-entered 2024-08-16 (kill switch)</p>
</div>
<div style="flex:1; display:flex; flex-direction:column; gap:16px">
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:24px">{svg("example2")}</div>
<p style="font-size:25px; color:{DIM}; margin:0; text-align:center">UPRO + TQQQ &mdash; exited 2013-06-24, re-entered 2013-07-09 (defensive trim)</p>
</div>
</div>
{footer("Strategy Behavior", "08")}'''
}, data_section="August 2024 and mid-2013, both real, both false alarms")

# ============================================================ 11. ROOT CAUSE
write("root-cause", {
    "style": BASE,
    "inner": f'''
{eyebrow("Root Cause, Traced to the Trade Log", RED)}
<h2 style="font-size:50px; font-weight:700; margin:0">The Kill Switch Has Zero Confirmation &mdash; and It Shows</h2>
<p style="font-size:26px; color:{DIM}; margin:0; max-width:1650px">Worst drawdown's actual exits (2021-11-22 &rarr; 2023-03-10). All seven were the kill switch. None were the trim.</p>
<table style="width:1500px; font-family:{MONO}; border-collapse:collapse; margin-top:4px">
<tr style="background:{ACCENT_DIM}"><th style="padding:12px 18px; text-align:left; font-size:21px; font-family:{SANS}; color:{ACCENT}">DATE</th><th style="padding:12px 18px; text-align:left; font-size:21px; font-family:{SANS}; color:{ACCENT}">TRANSITION</th><th style="padding:12px 18px; text-align:left; font-size:21px; font-family:{SANS}; color:{ACCENT}">NOTE</th></tr>
<tr><td style="padding:10px 18px; font-size:24px; color:{DIM}">2022-03-23</td><td style="padding:10px 18px; font-size:24px; color:{GREEN}">CASH &rarr; BULL_FULL</td><td style="padding:10px 18px; font-size:22px; color:{MUTED}; font-family:{SANS}">re-entry</td></tr>
<tr style="background:{RED_DIM}"><td style="padding:10px 18px; font-size:24px; color:{DIM}">2022-03-24</td><td style="padding:10px 18px; font-size:24px; color:{RED}">BULL_FULL &rarr; CASH</td><td style="padding:10px 18px; font-size:22px; color:{RED}; font-family:{SANS}; font-weight:700">killed the VERY NEXT DAY</td></tr>
<tr><td style="padding:10px 18px; font-size:24px; color:{DIM}">2023-01-17</td><td style="padding:10px 18px; font-size:24px; color:{GREEN}">CASH &rarr; BULL_FULL</td><td style="padding:10px 18px; font-size:22px; color:{MUTED}; font-family:{SANS}">re-entry</td></tr>
<tr style="background:{RED_DIM}"><td style="padding:10px 18px; font-size:24px; color:{DIM}">2023-01-18</td><td style="padding:10px 18px; font-size:24px; color:{RED}">BULL_FULL &rarr; CASH</td><td style="padding:10px 18px; font-size:22px; color:{RED}; font-family:{SANS}; font-weight:700">killed the VERY NEXT DAY</td></tr>
</table>
<p style="font-size:25px; color:{TEXT}; margin-top:4px; max-width:1650px; line-height:1.5">The trim has a 60-day post-re-entry immunity to stop exactly this. <b>The kill switch has none</b> &mdash; by explicit design, "it always fires." That asymmetry, not a vague bug, is the traceable root cause.</p>
{footer("Root Cause", "09")}'''
}, data_section="Kill switch lacks the trim's own re-entry protection")

# ============================================================ 12. DIVIDER: FIXES
write("divider-fixes", {
    "style": f"background:linear-gradient(135deg, #0d1220 0%, {BG} 100%); color:{TEXT}; font-family:{SANS}; display:flex; flex-direction:column; justify-content:center; padding:160px",
    "inner": f'''
<p style="font-size:28px; font-weight:700; letter-spacing:3px; text-transform:uppercase; color:{ACCENT}; margin:0 0 32px">Part Three</p>
<h1 style="font-size:88px; font-weight:700; line-height:1.15; margin:0; max-width:1500px">Three Attempts<br>to Fix It</h1>
<p style="font-size:32px; color:{DIM}; margin:40px 0 0; max-width:1300px; line-height:1.5">Each built as its own isolated variant, verified byte-identical to the confirmed engine at default, then run through the real optimizer &mdash; same guardrails, same 4-fold scoring, same discipline as every other candidate.</p>'''
}, data_section="Kill-switch buffer, boolean re-entry, continuous re-entry", transition="push")

# ============================================================ 13. ATTEMPT: KILL SWITCH
write("attempt-killswitch", {
    "style": BASE,
    "inner": f'''
{eyebrow("Attempt 1 of 3")}
<h2 style="font-size:50px; font-weight:700; margin:0">Kill-Switch Buffer + Confirmation</h2>
<p style="font-size:26px; color:{DIM}; margin:0; max-width:1650px">Require SPY to close a defined % below its 200-SMA, sustained for N days, instead of firing on a single close. VIX leg untouched.</p>
<div style="display:flex; gap:32px; margin-top:8px">
<div style="flex:1; background:{GREEN_DIM}; border-radius:20px; padding:36px; display:flex; flex-direction:column; gap:14px">
<p style="font-size:24px; font-weight:700; text-transform:uppercase; color:{GREEN}; margin:0">Dev period (2011&ndash;2021)</p>
<p style="font-size:28px; margin:0">Every single fold improved. 0 guardrail rejections. Tight cluster: all 17 top trials agreed on confirm=4 days exactly.</p>
<p style="font-family:{MONO}; font-size:30px; margin:0">Calmar <span style="color:{MUTED}">0.889</span> &rarr; <b style="color:{GREEN}">1.094</b></p>
</div>
<div style="flex:1; background:{RED_DIM}; border-radius:20px; padding:36px; display:flex; flex-direction:column; gap:14px">
<p style="font-size:24px; font-weight:700; text-transform:uppercase; color:{RED}; margin:0">Sealed holdout (2022&ndash;2025)</p>
<p style="font-size:28px; margin:0">Whipsaws genuinely dropped (false alarms 4&rarr;2). Return didn't follow &mdash; the slower trigger also absorbed more of a real decline (SVB, March 2023).</p>
<p style="font-family:{MONO}; font-size:30px; margin:0">CAGR <span style="color:{MUTED}">21.24%</span> &rarr; <b style="color:{RED}">20.61%</b></p>
</div>
</div>
<p style="font-size:25px; color:{TEXT}; margin:0">Mechanism verified working exactly as designed. Net effect on this window: slightly negative. <b>Not adopted.</b></p>
{footer("Fixes Attempted", "10")}'''
}, data_section="Clean dev result, failed the sealed holdout")

# ============================================================ 14. ATTEMPT: RE-ENTRY
write("attempt-reentry", {
    "style": BASE,
    "inner": f'''
{eyebrow("Attempts 2 &amp; 3 of 3")}
<h2 style="font-size:50px; font-weight:700; margin:0">Loosening Re-entry Instead of Tightening Exit</h2>
<p style="font-size:26px; color:{DIM}; margin:0; max-width:1650px">The market bottoms in the first 15&ndash;20% of a whipsaw episode, then recovers while the strategy waits for re-entry. Two versions tried:</p>
<div style="display:flex; gap:32px; margin-top:4px">
<div style="flex:1; background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:36px; display:flex; flex-direction:column; gap:12px">
<p style="font-size:25px; font-weight:700; color:{TEXT}; margin:0">Boolean: "N of 4 conditions"</p>
<p style="font-size:25px; color:{DIM}; margin:0; line-height:1.5">Tested the full range, 1-of-4 through 4-of-4. 1-of-4 blew out to <b style="color:{RED}">&minus;61.8% MaxDD</b> and 538 trades. Best point (3-of-4): one fold worse, trades <b>up</b> 40%, improvement driven by reduced fold variance, not higher typical return.</p>
</div>
<div style="flex:1; background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:36px; display:flex; flex-direction:column; gap:12px">
<p style="font-size:25px; font-weight:700; color:{TEXT}; margin:0">Continuous: signed-distance tolerance</p>
<p style="font-size:25px; color:{DIM}; margin:0; line-height:1.5">Better-built (a true continuous relaxation, verified identical to today's rule at zero tolerance) &mdash; but the robust, density-picked region scored <b style="color:{RED}">worse than baseline</b> (0.756 vs. 0.905), deeper drawdown, 35% more trades.</p>
</div>
</div>
<p style="font-size:25px; color:{TEXT}; margin:0">Same bear-market fold got worse under every version tried. <b>Neither adopted.</b></p>
{footer("Fixes Attempted", "11")}'''
}, data_section="Boolean and continuous versions, both negative")

# ============================================================ 15. WHY NONE WORKED
write("why-none-worked", {
    "style": BASE,
    "inner": f'''
{eyebrow("Synthesis")}
<h2 style="font-size:54px; font-weight:700; margin:0">Why None of the Three Held Up</h2>
<div style="display:flex; align-items:center; gap:0; margin-top:24px">
<div style="flex:1; background:{SURFACE}; border:2px solid {RED}; border-radius:20px 0 0 20px; padding:44px; display:flex; flex-direction:column; gap:16px">
<p style="font-size:26px; font-weight:700; text-transform:uppercase; color:{RED}; margin:0">Cost of whipsaw</p>
<p style="font-size:28px; margin:0; line-height:1.5">False alarms during choppy, sideways stretches. Real, and this is what every fix targeted.</p>
</div>
<div style="width:120px; height:120px; border-radius:60px; background:{ACCENT_DIM}; border:2px solid {ACCENT}; display:flex; align-items:center; justify-content:center; flex-shrink:0">
<p style="font-size:44px; font-weight:700; color:{ACCENT}; margin:0">vs</p>
</div>
<div style="flex:1; background:{SURFACE}; border:2px solid {YELLOW}; border-radius:0 20px 20px 0; padding:44px; display:flex; flex-direction:column; gap:16px">
<p style="font-size:26px; font-weight:700; text-transform:uppercase; color:{YELLOW}; margin:0">Cost of slower reaction</p>
<p style="font-size:28px; margin:0; line-height:1.5">Every fix that resists whipsaw also reacts slower to genuine declines. That cost showed up every time, too.</p>
</div>
</div>
<p style="font-size:29px; color:{TEXT}; margin-top:16px; max-width:1650px; line-height:1.6">Three independent, properly-tested attempts, all landing flat-to-negative, is a real signal: <b>whipsaw resistance and crash-reactivity are in tension</b> in this state-machine design on 3x leveraged instruments. That's consistent with what trend-following systems generally look like &mdash; not a bug waiting to be patched.</p>
{footer("Synthesis", "12")}'''
}, data_section="Whipsaw-resistance vs. crash-reactivity, a real tension")

# ============================================================ 16. AUDIT / CONFIDENCE
write("audit-confidence", {
    "style": BASE,
    "inner": f'''
{eyebrow("Verification")}
<h2 style="font-size:52px; font-weight:700; margin:0">Independently Re-checked Before Reporting</h2>
<div style="display:grid; grid-template-columns:1fr 1fr; gap:24px; margin-top:8px">
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; gap:20px; align-items:flex-start"><x-icon name="CheckCircle" style="color:{GREEN}; width:36px; height:36px"></x-icon><p style="font-size:25px; margin:0; line-height:1.5">All 3 variant engines re-verified byte-identical to the confirmed engine at neutral defaults, fresh, not just trusted from earlier</p></div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; gap:20px; align-items:flex-start"><x-icon name="CheckCircle" style="color:{GREEN}; width:36px; height:36px"></x-icon><p style="font-size:25px; margin:0; line-height:1.5">Baseline params cross-checked against both OPTIMIZATION_REPORT.md and the live results.json independently</p></div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; gap:20px; align-items:flex-start"><x-icon name="CheckCircle" style="color:{GREEN}; width:36px; height:36px"></x-icon><p style="font-size:25px; margin:0; line-height:1.5">A CAGR rounding discrepancy was traced to a shared, non-differential detail in the metrics function &mdash; not a bug</p></div>
<div style="background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px; display:flex; gap:20px; align-items:flex-start"><x-icon name="CheckCircle" style="color:{GREEN}; width:36px; height:36px"></x-icon><p style="font-size:25px; margin:0; line-height:1.5">Guardrails confirmed genuinely live via synthetic breach tests (drawdown, min-trades, defensive-floor), not silently bypassed</p></div>
</div>
<div style="background:{ACCENT_DIM}; border-radius:16px; padding:32px 40px; margin-top:4px">
<p style="font-size:26px; margin:0; line-height:1.5">The kill-switch holdout result was traced to the actual trade log: both instant re-kill whipsaws are confirmed gone, and the portfolio-value gap is traced to a specific real event (SVB, March 2023) the slower trigger rode through instead of exiting.</p>
</div>
{footer("Verification", "13")}'''
}, data_section="Byte-identical checks, baseline cross-checks, live guardrail tests")

# ============================================================ 17. RECOMMENDATION
write("recommendation", {
    "style": BASE,
    "inner": f'''
{eyebrow("Where This Leaves Things")}
<h2 style="font-size:52px; font-weight:700; margin:0">Current Recommendation</h2>
<div style="display:flex; flex-direction:column; gap:20px; margin-top:8px">
<div style="display:flex; gap:24px; align-items:center; background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px 36px">
<p style="font-family:{MONO}; font-size:32px; font-weight:700; color:{GREEN}; margin:0; width:64px">01</p>
<p style="font-size:28px; margin:0; line-height:1.5"><b>Adopt</b> UPRO + TQQQ tuned, confirmed values locked &mdash; <i>contingent on accepting the larger drawdown</i> as the price of the return gain. Still the only candidate, across nine total attempts, that survived sealed-data testing.</p>
</div>
<div style="display:flex; gap:24px; align-items:center; background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px 36px">
<p style="font-family:{MONO}; font-size:32px; font-weight:700; color:{DIM}; margin:0; width:64px">02</p>
<p style="font-size:28px; margin:0; line-height:1.5"><b>Leave UPRO-Only and SPXU Hedge Overlay exactly as they are.</b> Nothing beats today's settings once tested honestly.</p>
</div>
<div style="display:flex; gap:24px; align-items:center; background:{SURFACE}; border:1px solid {BORDER}; border-radius:16px; padding:32px 36px">
<p style="font-family:{MONO}; font-size:32px; font-weight:700; color:{DIM}; margin:0; width:64px">03</p>
<p style="font-size:28px; margin:0; line-height:1.5"><b>Leave the whipsaw mechanism as-is.</b> Three properly-tested fixes, none improved on it. Treat it as a structural property of this design, not an unsolved bug.</p>
</div>
</div>
{footer("Recommendation", "14")}'''
}, data_section="Adopt the flagship trade-off; leave the mechanism as-is")

# ============================================================ 18. NEXT STEPS
write("next-steps", {
    "style": BASE,
    "inner": f'''
{eyebrow("What's Left")}
<h2 style="font-size:52px; font-weight:700; margin:0">Next Steps</h2>
<div style="display:flex; gap:28px; margin-top:8px">
<div style="flex:1; background:{ACCENT_DIM}; border-radius:20px; padding:40px; display:flex; flex-direction:column; gap:16px">
<p style="font-size:24px; font-weight:700; text-transform:uppercase; letter-spacing:1px; color:{ACCENT}; margin:0">Your call</p>
<p style="font-size:28px; margin:0; line-height:1.5">Decide whether the &minus;30.4%&rarr;&minus;40.7% drawdown trade-off is acceptable for +5.7pp of CAGR. If yes, ready to load into the live dashboard immediately.</p>
</div>
<div style="flex:1; background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:40px; display:flex; flex-direction:column; gap:16px">
<p style="font-size:24px; font-weight:700; text-transform:uppercase; letter-spacing:1px; color:{MUTED}; margin:0">Free, no holdout needed</p>
<p style="font-size:28px; margin:0; line-height:1.5">Sweep idle cash (~24.5% average allocation) into a yield-bearing instrument. Every number shown so far understates real returns by this amount.</p>
</div>
<div style="flex:1; background:{SURFACE}; border:1px solid {BORDER}; border-radius:20px; padding:40px; display:flex; flex-direction:column; gap:16px">
<p style="font-size:24px; font-weight:700; text-transform:uppercase; letter-spacing:1px; color:{YELLOW}; margin:0">The bigger conversation</p>
<p style="font-size:28px; margin:0; line-height:1.5">A mean-reversion arm designed to profit from the same chop that costs the trend-following sleeve &mdash; a structural answer, not a parameter patch.</p>
</div>
</div>
{footer("Next Steps", "15")}'''
}, data_section="Your call, a free win, and the structural option")

# ============================================================ 19. CLOSING
write("closing", {
    "style": f"background:linear-gradient(135deg, {BG} 0%, #0d1220 100%); color:{TEXT}; font-family:{SANS}; display:flex; flex-direction:column; justify-content:center; align-items:center; padding:160px; text-align:center",
    "inner": f'''
<h1 style="font-size:76px; font-weight:700; margin:0; max-width:1400px">One decision on the table.<br>Everything else, answered.</h1>
<p style="font-size:30px; color:{DIM}; margin:40px 0 0; max-width:1100px; line-height:1.6">Accept the flagship's drawdown trade-off or don't &mdash; that's the only open question left after nine independently tested candidates.</p>
<p style="position:absolute; bottom:120px; font-size:24px; color:{MUTED}; font-family:{MONO}">RAVI VAM Strategy Platform &middot; Insight Fusion Analytics</p>'''
}, data_section="Closing")

print("All slides written.")
