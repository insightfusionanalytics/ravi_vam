"""Generate authentic project progress report PDF for Ravi VAM project."""

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether, ListFlowable, ListItem,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

OUTPUT = Path(__file__).resolve().parent.parent / "results" / "step1_step2_databento_final" / "VAM_Project_Progress_Report.pdf"

# Colors
BLUE = HexColor("#1B4F72")
GREEN = HexColor("#16A34A")
RED = HexColor("#DC2626")
AMBER = HexColor("#D97706")
GREY = HexColor("#888888")
GREY_LIGHT = HexColor("#F5F5F5")
BLACK = HexColor("#222222")
WHITE = HexColor("#FFFFFF")


def S() -> dict:
    """All styles."""
    return {
        "title": ParagraphStyle("t", fontSize=22, textColor=BLUE, fontName="Helvetica-Bold",
                                spaceAfter=14, alignment=TA_CENTER),
        "sub": ParagraphStyle("s", fontSize=12, textColor=GREY, fontName="Helvetica",
                              spaceBefore=0, spaceAfter=6, alignment=TA_CENTER),
        "prep": ParagraphStyle("p", fontSize=9, textColor=GREY, fontName="Helvetica-Oblique",
                               spaceAfter=10, alignment=TA_CENTER),
        "h1": ParagraphStyle("h1", fontSize=14, textColor=BLUE, fontName="Helvetica-Bold",
                             spaceBefore=14, spaceAfter=6),
        "h2": ParagraphStyle("h2", fontSize=10, textColor=BLUE, fontName="Helvetica-Bold",
                             spaceBefore=10, spaceAfter=4),
        "body": ParagraphStyle("b", fontSize=9, textColor=BLACK, fontName="Helvetica",
                               leading=13, spaceAfter=4),
        "body_bold": ParagraphStyle("bb", fontSize=9, textColor=BLACK, fontName="Helvetica-Bold",
                                    leading=13, spaceAfter=4),
        "small": ParagraphStyle("sm", fontSize=7.5, textColor=GREY, fontName="Helvetica",
                                leading=10),
        "bullet": ParagraphStyle("bu", fontSize=9, textColor=BLACK, fontName="Helvetica",
                                 leading=13, leftIndent=16, spaceAfter=2),
        "footer": ParagraphStyle("f", fontSize=7, textColor=GREY, fontName="Helvetica",
                                 alignment=TA_CENTER),
    }


def hr():
    return HRFlowable(width="100%", thickness=0.5, color=HexColor("#CCCCCC"),
                       spaceBefore=4, spaceAfter=4)


CELL_STYLE = ParagraphStyle("cell", fontSize=8, textColor=BLACK, fontName="Helvetica", leading=10)
CELL_BOLD = ParagraphStyle("cellb", fontSize=8, textColor=BLACK, fontName="Helvetica-Bold", leading=10)
CELL_HEAD = ParagraphStyle("cellh", fontSize=8, textColor=WHITE, fontName="Helvetica-Bold", leading=10)


def P(text: str, bold: bool = False) -> Paragraph:
    """Wrap text in a Paragraph for proper table cell wrapping."""
    return Paragraph(str(text), CELL_BOLD if bold else CELL_STYLE)


def PH(text: str) -> Paragraph:
    """Header cell paragraph."""
    return Paragraph(str(text), CELL_HEAD)


def wrap_row(row: list, header: bool = False) -> list:
    """Wrap all cells in a row with Paragraphs."""
    if header:
        return [PH(c) for c in row]
    return [P(c) for c in row]


def wrap_data(data: list[list]) -> list[list]:
    """Wrap all rows, treating first row as header."""
    result = [wrap_row(data[0], header=True)]
    for row in data[1:]:
        result.append(wrap_row(row))
    return result


def tbl(data, widths):
    wrapped = wrap_data(data)
    t = Table(wrapped, colWidths=widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY_LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.4, HexColor("#DDDDDD")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t


def make_status_cell(text: str) -> Paragraph:
    """Create a colored status cell paragraph."""
    upper = text.upper()
    if "COMPLETE" in upper or "DONE" in upper or "FIXED" in upper:
        color = GREEN
    elif "BLOCKED" in upper or "PENDING" in upper or "NOT DOWNLOADED" in upper:
        color = RED
    elif "IN PROGRESS" in upper:
        color = AMBER
    elif "DISCLOSED" in upper:
        color = AMBER
    else:
        color = BLACK
    st = ParagraphStyle("status", fontSize=8, textColor=color, fontName="Helvetica-Bold", leading=10)
    return Paragraph(text, st)


def status_tbl(data, widths):
    """Table with colored status in the last column."""
    wrapped = [wrap_row(data[0], header=True)]
    for row in data[1:]:
        cells = [P(c) for c in row[:-1]]
        cells.append(make_status_cell(row[-1]))
        wrapped.append(cells)

    t = Table(wrapped, colWidths=widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BLUE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY_LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.4, HexColor("#DDDDDD")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t


def bullets(s, items):
    """Return list of bullet paragraphs."""
    return [Paragraph("\u2022  " + item, s["bullet"]) for item in items]


def build() -> None:
    doc = SimpleDocTemplate(str(OUTPUT), pagesize=letter,
                            topMargin=0.5*inch, bottomMargin=0.5*inch,
                            leftMargin=0.75*inch, rightMargin=0.75*inch)
    s = S()
    st = []
    W = 6.5 * inch

    # ══════════════════════════════════════════════════════════
    # PAGE 1: Cover + Project Status
    # ══════════════════════════════════════════════════════════

    st.append(Spacer(1, 12))
    st.append(Paragraph("VAM Split Strategy", s["title"]))
    st.append(Paragraph("Project Progress Report", s["sub"]))
    st.append(Paragraph(
        "Prepared for Ravi Mareedu &amp; Sudhir Vyakaranam  |  "
        "Insight Fusion Analytics  |  March 29, 2026", s["prep"]))
    st.append(hr())

    # Project Overview
    st.append(Paragraph("Project Overview", s["h1"]))
    st.append(Paragraph(
        "This report documents all work completed on the VAM Split Strategy project, "
        "including the step-by-step build process, issues discovered and fixed, "
        "quality audits performed, and what remains to be done. "
        "This is a transparent, unedited account of the engineering process.", s["body"]))

    # Roadmap Status
    st.append(Paragraph("Roadmap Status", s["h1"]))
    roadmap = [
        ["Step", "Scope", "Hours", "Price", "Status"],
        ["Step 0", "UPRO-only backtest (yfinance prototype)", "20", "$1,100", "COMPLETE"],
        ["Step 1", "Rebuild on DataBento (production data)", "8", "$440", "COMPLETE"],
        ["Step 2", "Add TQQQ 25% sleeve (6-state machine)", "12", "$660", "COMPLETE"],
        ["Step 3", "Predatory Short (SPXU)", "10", "$550", "BLOCKED \u2014 data"],
        ["Step 4", "Safety Valve (SVIX + SGOV)", "10", "$550", "BLOCKED \u2014 data"],
        ["Step 5", "Excel Parameter Configurator", "6\u201312", "$330\u2013$660", "PENDING"],
    ]
    st.append(status_tbl(roadmap, [W*0.09, W*0.38, W*0.08, W*0.1, W*0.35]))

    st.append(Spacer(1, 6))
    st.append(Paragraph(
        "<b>Steps 1 &amp; 2 are fully built, audited, and ready for delivery.</b> "
        "Steps 3\u20134 are blocked because SPXU, SVIX, and SGOV daily data has not been "
        "downloaded from DataBento yet. Step 5 depends on Steps 3\u20134.", s["body"]))

    # ══════════════════════════════════════════════════════════
    # PAGE 2: What Was Built
    # ══════════════════════════════════════════════════════════

    st.append(PageBreak())
    st.append(Paragraph("What Was Built", s["h1"]))

    st.append(Paragraph("Step 1 \u2014 UPRO-Only Backtest on DataBento", s["h2"]))
    st.extend(bullets(s, [
        "Loaded SPY, UPRO, VIX daily data from DataBento (institutional-grade)",
        "Calculated SMA-50, SMA-200, RSI-14 indicators on SPY",
        "Implemented 4-state machine: BULL_100 \u2192 BULL_TRIMMED \u2192 DEFENSIVE \u2192 CASH",
        "Signal priority hierarchy: Kill Switch (VIX>30 or SPY&lt;200SMA) > Defensive (SMA-50) > RSI Trim",
        "T+1 execution: signal at market close, trade at next-day open price",
        "Dynamic slippage: 5 bps normal, 20 bps on stress days (VIX > 25)",
        "67 trades over 5.2 years, $100K \u2192 $274,821 (+174.8%, CAGR 21.4%)",
    ]))

    st.append(Paragraph("Step 2 \u2014 UPRO + TQQQ Backtest", s["h2"]))
    st.extend(bullets(s, [
        "Added QQQ signals and TQQQ position tracking",
        "Expanded to 6-state machine with independent SPY/QQQ defensive triggers",
        "States: BULL_FULL, BULL_TRIMMED, DEF_SPY, DEF_QQQ, DEF_BOTH, CASH",
        "SPY controls UPRO sleeve (75%), QQQ controls TQQQ sleeve (25%)",
        "180 trades (90 UPRO + 90 TQQQ), $100K \u2192 $278,744 (+178.7%, CAGR 21.8%)",
    ]))

    st.append(Paragraph("Matrix Cross-Check (Internal Validation)", s["h2"]))
    st.extend(bullets(s, [
        "Built a separate YAML-driven Markov transition engine (10 states x 24 scenarios)",
        "Ran the same data through both engines independently",
        "95.8% directional agreement \u2014 confirms the state machine logic is correct",
        "Differences are from the matrix's 5-day cooldown chain (more conservative by design)",
    ]))

    st.append(Paragraph("Interactive Chart", s["h2"]))
    st.extend(bullets(s, [
        "TradingView-style browser chart with SPY price, SMA-50, SMA-200 overlays",
        "VIX panel with kill switch threshold line at 30",
        "RSI panel with overbought (75) and rebuy (60) lines",
        "Equity curve panel showing portfolio growth",
        "Buy/sell trade markers with hover tooltips showing full signal details",
        "Toggle buttons to show/hide states, signals, and trade types",
    ]))

    # ══════════════════════════════════════════════════════════
    # PAGE 3: Issues Found & Fixed
    # ══════════════════════════════════════════════════════════

    st.append(PageBreak())
    st.append(Paragraph("Issues Discovered &amp; Fixed", s["h1"]))
    st.append(Paragraph(
        "During the build process, we ran 3 rounds of audits (cross-checks, devil's advocate "
        "reviews, and debug verification). Every issue found was fixed and re-verified. "
        "Below is the complete, unedited list.", s["body"]))

    issues = [
        ["#", "Issue", "Severity", "Resolution", "Status"],
        ["1", "DataBento data not split-adjusted (UPRO showed -52% in 1 day)",
         "CRITICAL", "Identified 2 UPRO splits + 3 TQQQ splits. Applied in-memory adjustment.", "FIXED"],
        ["2", "Look-ahead bias: trades at same-day close instead of next-day open",
         "CRITICAL", "Implemented T+1 pending-trade queue. Execute at next day's open price.", "FIXED"],
        ["3", "T+2 double-shift bug: execution price was 2 days late",
         "CRITICAL", "Removed shift(-1) from open column. Pending queue provides the 1-bar delay.", "FIXED"],
        ["4", "Sharpe ratio not risk-free adjusted (overstated by ~0.15)",
         "HIGH", "Subtracted 4% annualized risk-free rate from daily returns before Sharpe calc.", "FIXED"],
        ["5", "Slippage too low on panic days (5 bps flat)",
         "HIGH", "Dynamic slippage: 5 bps normal, 20 bps when VIX > 25 or kill switch.", "FIXED"],
        ["6", "Trade log state_from column showed destination, not origin",
         "MEDIUM", "Captured old state value before update. All 67 trades now correct.", "FIXED"],
        ["7", "Negative cash balances on some days (implicit margin)",
         "MEDIUM", "Capped BULL_100 allocation at 99% to reserve cash for costs.", "FIXED"],
        ["8", "Kill switch whipsaw: 4 round trips in Mar-Apr 2022",
         "KNOWN", "Documented as disclosure. 200-SMA leg has no confirmation period (by design).", "DISCLOSED"],
    ]
    st.append(status_tbl(issues, [W*0.04, W*0.24, W*0.09, W*0.42, W*0.10]))

    # ══════════════════════════════════════════════════════════
    # PAGE 4: Audit Trail
    # ══════════════════════════════════════════════════════════

    st.append(PageBreak())
    st.append(Paragraph("Audit Trail", s["h1"]))
    st.append(Paragraph(
        "Every backtest result was verified through multiple independent checks "
        "before being declared ready for delivery.", s["body"]))

    audits = [
        ["Round", "What Was Checked", "Findings"],
        ["1. Cross-Check (3 parallel agents)",
         "Step 1 trade log math (67 trades), Step 2 trade log math (180 trades), Matrix vs normal directional agreement",
         "Split data bug found (CRITICAL). All portfolio math correct to $0.01. 95.8% agreement confirmed."],
        ["2. Devil's Advocate Round 1",
         "15 attack vectors: look-ahead bias, split correctness, survivorship bias, execution assumptions, Sharpe inflation",
         "Look-ahead bias (CRITICAL). Sharpe inflation (HIGH). Slippage too low (HIGH). Step 2 worse than Step 1 (fixed after T+1)."],
        ["3. Debug + Devil's Advocate Round 2",
         "T+1 execution price verification, pending-trade state logic, trade log state_from correctness",
         "T+2 double-shift bug (CRITICAL). state_from logging bug (MEDIUM). All other checks passed."],
        ["4. Final Verification (post all fixes)",
         "5 trades: exec price vs raw UPRO open. 5 trades: signal conditions on T-1. 5 trades: state_from/to. 3 trades: portfolio math. 3 Step 2: UPRO+TQQQ pairing.",
         "ALL 21 CHECKS PASSED. Zero mismatches. GO for client delivery."],
    ]
    st.append(tbl(audits, [W*0.18, W*0.40, W*0.42]))

    # ══════════════════════════════════════════════════════════
    # PAGE 5: Deliverables + What's Next
    # ══════════════════════════════════════════════════════════

    st.append(PageBreak())
    st.append(Paragraph("Deliverables Ready for Review", s["h1"]))

    deliverables = [
        ["File", "Description"],
        ["VAM_Backtest_Executive_Summary.pdf", "Performance results for Step 1 & 2 with methodology and disclosures"],
        ["step1_databento_trade_log.csv", "67 trades with 25 columns: signal date, exec price, reason, math checks"],
        ["step1_databento_portfolio_values.csv", "1,308 daily rows with 30+ columns: every signal, threshold, state"],
        ["step2_databento_trade_log.csv", "180 trades (90 UPRO + 90 TQQQ) with full audit trail"],
        ["step2_databento_portfolio_values.csv", "1,308 daily rows with SPY + QQQ signals, dual allocations"],
        ["vam_step1_interactive_chart.html", "Browser-based interactive chart with toggles for states and signals"],
    ]
    st.append(tbl(deliverables, [W*0.42, W*0.58]))

    st.append(Paragraph("What's Next", s["h1"]))
    next_steps = [
        ["Step", "What Needs to Happen", "Estimated Time", "Depends On"],
        ["Step 0 Payment", "Invoice $1,100 on Upwork for completed Step 0 work", "\u2014", "Client approval"],
        ["Advance Deposit", "$1,100 advance for Steps 1\u20134 per proposal", "\u2014", "Step 0 paid"],
        ["Step 1\u20132 Delivery", "Present results + interactive chart to client", "1 call", "Payment confirmed"],
        ["Step 3: SPXU Short", "Download SPXU + VIX data, build predatory short state", "5\u20137 days", "Step 2 approved"],
        ["Step 4: SVIX Safety", "Download SVIX + SGOV data, build safety valve", "3\u20135 days", "Step 3 complete"],
        ["Step 5: Configurator", "Excel parameter file for client to tweak thresholds", "3\u20137 days", "Step 4 complete"],
    ]
    st.append(status_tbl(next_steps, [W*0.16, W*0.40, W*0.16, W*0.20]))

    st.append(Spacer(1, 12))
    st.append(Paragraph("Data Needed for Steps 3\u20134 (Not Yet Downloaded)", s["h2"]))
    data_needed = [
        ["Ticker", "Purpose", "Source", "Status"],
        ["SPXU", "Inverse S&P 500 ETF for predatory short strategy", "DataBento", "NOT DOWNLOADED"],
        ["SVIX", "Short VIX ETF for volatility crush recovery", "DataBento", "NOT DOWNLOADED"],
        ["SGOV", "Ultra-short Treasury ETF (cash equivalent)", "DataBento", "NOT DOWNLOADED"],
    ]
    st.append(status_tbl(data_needed, [W*0.12, W*0.42, W*0.16, W*0.25]))
    st.append(Paragraph(
        "DataBento data costs $199/month, invoiced to client per proposal terms.", s["small"]))

    # Footer
    st.append(Spacer(1, 24))
    st.append(hr())
    st.append(Paragraph(
        "CONFIDENTIAL \u2014 Insight Fusion Analytics  |  www.insightfusionanalytics.com",
        s["footer"]))

    doc.build(st)
    print(f"Saved: {OUTPUT}")


if __name__ == "__main__":
    build()
