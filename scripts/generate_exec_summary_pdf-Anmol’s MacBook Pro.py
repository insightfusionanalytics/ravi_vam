"""Generate professional PDF executive summary for Ravi VAM backtest."""

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "results" / "step1_step2_databento_final"

# IFA Brand Colors
IFA_BLUE = HexColor("#1B4F72")
IFA_GREEN = HexColor("#22C55E")
IFA_RED = HexColor("#DC2626")
GREY_LIGHT = HexColor("#F5F5F5")
GREY_MID = HexColor("#888888")
WHITE = HexColor("#FFFFFF")
BLACK = HexColor("#222222")


def build_styles() -> dict:
    """Create all paragraph styles."""
    return {
        "title": ParagraphStyle(
            "title", fontSize=24, textColor=IFA_BLUE,
            fontName="Helvetica-Bold", spaceAfter=8, alignment=TA_CENTER,
        ),
        "subtitle": ParagraphStyle(
            "subtitle", fontSize=12, textColor=GREY_MID,
            fontName="Helvetica", spaceBefore=4, spaceAfter=6, alignment=TA_CENTER,
        ),
        "prepared": ParagraphStyle(
            "prepared", fontSize=9, textColor=GREY_MID,
            fontName="Helvetica-Oblique", spaceAfter=12, alignment=TA_CENTER,
        ),
        "h1": ParagraphStyle(
            "h1", fontSize=14, textColor=IFA_BLUE,
            fontName="Helvetica-Bold", spaceBefore=14, spaceAfter=6,
        ),
        "h2": ParagraphStyle(
            "h2", fontSize=10, textColor=IFA_BLUE,
            fontName="Helvetica-Bold", spaceBefore=10, spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "body", fontSize=9, textColor=BLACK,
            fontName="Helvetica", leading=13, spaceAfter=4,
        ),
        "small": ParagraphStyle(
            "small", fontSize=7.5, textColor=GREY_MID,
            fontName="Helvetica", leading=10, spaceAfter=2,
        ),
        "footer": ParagraphStyle(
            "footer", fontSize=7, textColor=GREY_MID,
            fontName="Helvetica", alignment=TA_CENTER,
        ),
    }


def hr() -> HRFlowable:
    """Thin horizontal rule."""
    return HRFlowable(
        width="100%", thickness=0.5, color=HexColor("#CCCCCC"),
        spaceBefore=4, spaceAfter=4,
    )


def styled_table(data: list[list], col_widths: list) -> Table:
    """Create a consistently styled table."""
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        # Header row
        ("BACKGROUND", (0, 0), (-1, 0), IFA_BLUE),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8.5),
        # Body rows
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 8.5),
        ("TEXTCOLOR", (0, 1), (-1, -1), BLACK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GREY_LIGHT]),
        # Borders and padding
        ("GRID", (0, 0), (-1, -1), 0.4, HexColor("#DDDDDD")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


def build_step_section(
    s: dict,
    title: str,
    states_desc: str,
    metrics: list[list],
    annual: list[list],
    annual_note: str = "",
) -> list:
    """Build a complete step section (title + metrics + annual returns)."""
    elements = []
    elements.append(Paragraph(title, s["h1"]))
    elements.append(Paragraph(states_desc, s["body"]))
    elements.append(Spacer(1, 4))

    w = 6.5 * inch  # Full usable width
    elements.append(styled_table(metrics, [w * 0.24, w * 0.24, w * 0.28, w * 0.24]))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Annual Returns", s["h2"]))
    aw = w / 7
    elements.append(styled_table(annual, [aw] * 7))
    if annual_note:
        elements.append(Paragraph(annual_note, s["small"]))

    return elements


def build_pdf() -> None:
    """Build the executive summary PDF."""
    output_path = OUTPUT_DIR / "VAM_Backtest_Executive_Summary.pdf"
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=letter,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
    )

    s = build_styles()
    story = []
    W = 6.5 * inch  # Usable width

    # ════════════════════════════════════════════════════════════════
    # PAGE 1: Header + Methodology + Step 1
    # ════════════════════════════════════════════════════════════════

    story.append(Spacer(1, 12))
    story.append(Paragraph("VAM Split Strategy", s["title"]))
    story.append(Paragraph("DataBento Production Backtest Results", s["subtitle"]))
    story.append(Paragraph(
        "Prepared for Ravi Mareedu &amp; Sudhir Vyakaranam  |  "
        "Insight Fusion Analytics  |  March 2026",
        s["prepared"],
    ))
    story.append(hr())

    # Methodology
    story.append(Paragraph("Methodology", s["h1"]))
    method_data = [
        ["Parameter", "Value"],
        ["Data Source", "DataBento (DBEQ.BASIC) + CBOE VIX, split-adjusted"],
        ["Period", "Oct 16, 2020 \u2013 Dec 30, 2025 (5.2 years)"],
        ["Execution", "T+1 (signal at market close, execute at next-day open)"],
        ["Slippage", "5 bps normal  /  20 bps on stress days (VIX > 25)"],
        ["Commission", "$1.00 per trade (IBKR flat rate)"],
        ["Risk Metrics", "Sharpe & Sortino adjusted for 4% risk-free rate"],
    ]
    story.append(styled_table(method_data, [W * 0.25, W * 0.75]))
    story.append(Spacer(1, 4))

    # Step 1 — keep together so it doesn't split
    step1 = build_step_section(
        s,
        title="Step 1 \u2014 UPRO Only (4-State Machine)",
        states_desc="States:  BULL_100 (99% UPRO)  |  BULL_TRIMMED (75%)  |  DEFENSIVE (50%)  |  CASH",
        metrics=[
            ["Metric", "Value", "Metric", "Value"],
            ["Initial Capital", "$100,000", "SPY B&H CAGR", "+14.1%"],
            ["Final Value", "$274,821", "Alpha vs SPY", "+7.4%"],
            ["Total Return", "+174.8%", "Total Trades", "67"],
            ["CAGR", "+21.4%", "Total Costs", "$6,421"],
            ["Sharpe Ratio", "0.676", "Max Drawdown", "\u201338.3%"],
            ["Sortino Ratio", "0.880", "Max DD Date", "2023-03-10"],
            ["Calmar Ratio", "0.559", "", ""],
        ],
        annual=[
            ["Year", "2020*", "2021", "2022", "2023", "2024", "2025"],
            ["Return", "+18.0%", "+60.3%", "\u201329.0%", "+28.5%", "+27.8%", "+32.2%"],
        ],
        annual_note="* 2020 is partial (Oct\u2013Dec only)",
    )
    story.append(KeepTogether(step1))

    # ════════════════════════════════════════════════════════════════
    # PAGE 2: Step 2 + Comparison
    # ════════════════════════════════════════════════════════════════

    story.append(PageBreak())

    step2 = build_step_section(
        s,
        title="Step 2 \u2014 UPRO + TQQQ (6-State Machine)",
        states_desc=(
            "States:  BULL_FULL (74.25% UPRO + 24.75% TQQQ)  |  BULL_TRIMMED  |  "
            "DEF_SPY  |  DEF_QQQ  |  DEF_BOTH  |  CASH"
        ),
        metrics=[
            ["Metric", "Value", "Metric", "Value"],
            ["Initial Capital", "$100,000", "SPY B&H CAGR", "+14.1%"],
            ["Final Value", "$278,744", "Alpha vs SPY", "+7.7%"],
            ["Total Return", "+178.7%", "Total Trades", "180 (90+90)"],
            ["CAGR", "+21.8%", "Total Costs", "$6,362"],
            ["Sharpe Ratio", "0.663", "Max Drawdown", "\u201339.2%"],
            ["Sortino Ratio", "0.861", "Max DD Date", "2023-03-10"],
            ["Calmar Ratio", "0.555", "", ""],
        ],
        annual=[
            ["Year", "2020*", "2021", "2022", "2023", "2024", "2025"],
            ["Return", "+16.9%", "+52.7%", "\u201331.6%", "+40.3%", "+32.7%", "+31.2%"],
        ],
    )
    story.extend(step2)
    story.append(Spacer(1, 8))

    # Comparison
    story.append(Paragraph("Step 1 vs Step 2 Comparison", s["h1"]))
    comp_data = [
        ["Metric", "Step 1", "Step 2", "Winner"],
        ["Final Value", "$274,821", "$278,744", "Step 2 (+$3,923)"],
        ["CAGR", "21.4%", "21.8%", "Step 2"],
        ["Sharpe", "0.676", "0.663", "Step 1 (marginal)"],
        ["Max Drawdown", "\u201338.3%", "\u201339.2%", "Step 1 (less risk)"],
        ["Alpha vs SPY", "+7.4%", "+7.7%", "Step 2"],
    ]
    story.append(styled_table(comp_data, [W * 0.2, W * 0.2, W * 0.2, W * 0.4]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "Both steps deliver strong results. Step 2 has slightly higher returns; "
        "Step 1 has slightly lower drawdown. The difference is small enough that "
        "either is defensible.",
        s["body"],
    ))

    # ════════════════════════════════════════════════════════════════
    # PAGE 3: Disclosures
    # ════════════════════════════════════════════════════════════════

    story.append(PageBreak())
    story.append(Paragraph("Important Disclosures", s["h1"]))
    story.append(Spacer(1, 4))

    disclosures = [
        ("1. Data Window",
         "5.2 years (2020\u20132025) \u2014 mostly bull market. The strategy has not been "
         "tested through multi-year bears (2000\u20132003, 2007\u20132009). Results may not "
         "generalize to all market conditions."),
        ("2. Max Drawdown",
         "\u201338% to \u201339% is significant. A $100K portfolio would have dropped to "
         "~$62K at the worst point. SPY\u2019s max drawdown over the same period was "
         "approximately \u201325%."),
        ("3. 2022 Performance",
         "The strategy lost 29\u201332% in 2022. The kill switch prevented catastrophic "
         "losses (UPRO unprotected lost ~65%), but still underperformed SPY (\u201319%) "
         "in the crash year."),
        ("4. Kill Switch Whipsaw",
         "In Mar\u2013Apr 2022, the strategy executed 4 round trips in 20 days as SPY "
         "oscillated around the 200-day SMA. This generated ~$700 in unnecessary "
         "transaction costs. The kill switch has no confirmation period on the "
         "200-SMA leg."),
        ("5. Statistical Significance",
         "Alpha of 7\u20138% over 5.2 years is not conclusive. The confidence interval "
         "is wide enough that the true alpha could be zero. This is historical "
         "outperformance, not a guarantee of future results."),
    ]
    for title, body in disclosures:
        story.append(Paragraph(title, s["h2"]))
        story.append(Paragraph(body, s["body"]))
        story.append(Spacer(1, 4))

    # Footer
    story.append(Spacer(1, 30))
    story.append(hr())
    story.append(Paragraph(
        "CONFIDENTIAL \u2014 Prepared by Insight Fusion Analytics  |  "
        "www.insightfusionanalytics.com",
        s["footer"],
    ))

    doc.build(story)
    print(f"  Saved: {output_path}")


if __name__ == "__main__":
    build_pdf()
