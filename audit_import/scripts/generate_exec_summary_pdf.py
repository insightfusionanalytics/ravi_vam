"""Generate professional PDF executive summary for Ravi VAM backtest."""

import json
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
    PageBreak,
    KeepTogether,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
DELIVERY_DIR = Path(__file__).resolve().parent.parent / "delivery" / "backtest_results"
DELIVERY_DIR.mkdir(parents=True, exist_ok=True)

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
            "title",
            fontSize=24,
            textColor=IFA_BLUE,
            fontName="Helvetica-Bold",
            spaceAfter=8,
            alignment=TA_CENTER,
        ),
        "subtitle": ParagraphStyle(
            "subtitle",
            fontSize=12,
            textColor=GREY_MID,
            fontName="Helvetica",
            spaceBefore=4,
            spaceAfter=6,
            alignment=TA_CENTER,
        ),
        "prepared": ParagraphStyle(
            "prepared",
            fontSize=9,
            textColor=GREY_MID,
            fontName="Helvetica-Oblique",
            spaceAfter=12,
            alignment=TA_CENTER,
        ),
        "h1": ParagraphStyle(
            "h1",
            fontSize=14,
            textColor=IFA_BLUE,
            fontName="Helvetica-Bold",
            spaceBefore=14,
            spaceAfter=6,
        ),
        "h2": ParagraphStyle(
            "h2",
            fontSize=10,
            textColor=IFA_BLUE,
            fontName="Helvetica-Bold",
            spaceBefore=10,
            spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "body",
            fontSize=9,
            textColor=BLACK,
            fontName="Helvetica",
            leading=13,
            spaceAfter=4,
        ),
        "small": ParagraphStyle(
            "small",
            fontSize=7.5,
            textColor=GREY_MID,
            fontName="Helvetica",
            leading=10,
            spaceAfter=2,
        ),
        "footer": ParagraphStyle(
            "footer",
            fontSize=7,
            textColor=GREY_MID,
            fontName="Helvetica",
            alignment=TA_CENTER,
        ),
    }


def hr() -> HRFlowable:
    """Thin horizontal rule."""
    return HRFlowable(
        width="100%",
        thickness=0.5,
        color=HexColor("#CCCCCC"),
        spaceBefore=4,
        spaceAfter=4,
    )


def styled_table(data: list[list], col_widths: list) -> Table:
    """Create a consistently styled table."""
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
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
            ]
        )
    )
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
    ncols = len(annual[0])
    aw = w / ncols
    elements.append(styled_table(annual, [aw] * ncols))
    if annual_note:
        elements.append(Paragraph(annual_note, s["small"]))

    return elements


def _fp(v: float) -> str:
    """Format percentage with sign."""
    return f"+{v:.1f}%" if v >= 0 else f"\u2013{abs(v):.1f}%"


def _fd(v: float) -> str:
    """Format dollar value."""
    return f"${v:,.0f}"


def build_pdf() -> None:
    """Build the executive summary PDF from metrics JSON files."""
    with open(RESULTS_DIR / "step1_databento_metrics.json") as f:
        m1 = json.load(f)
    with open(RESULTS_DIR / "step2_databento_metrics.json") as f:
        m2 = json.load(f)
    output_path = DELIVERY_DIR / "VAM_Backtest_Executive_Summary.pdf"
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
    story.append(
        Paragraph(
            "Prepared for Ravi Mareedu &amp; Sudhir Vyakaranam  |  "
            "Insight Fusion Analytics  |  March 2026",
            s["prepared"],
        )
    )
    story.append(hr())

    # Methodology
    story.append(Paragraph("Methodology", s["h1"]))
    method_data = [
        ["Parameter", "Value"],
        ["Data Source", "DataBento (XNAS.ITCH) + CBOE VIX, split-adjusted in-memory"],
        ["Period", f"{m1['start_date']} \u2013 {m1['end_date']} ({m1['years']} years)"],
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
            [
                "Initial Capital",
                _fd(m1["initial_capital"]),
                "SPY B&H CAGR",
                _fp(m1["benchmark_spy_cagr_pct"]),
            ],
            ["Final Value", _fd(m1["final_value"]), "Alpha vs SPY", _fp(m1["alpha_vs_spy_pct"])],
            ["Total Return", _fp(m1["total_return_pct"]), "Total Trades", str(m1["total_trades"])],
            [
                "CAGR",
                _fp(m1["cagr_pct"]),
                "Total Costs",
                _fd(m1["total_commissions"] + m1["total_slippage"]),
            ],
            ["Sharpe Ratio", f"{m1['sharpe']:.3f}", "Max Drawdown", _fp(m1["max_drawdown_pct"])],
            ["Sortino Ratio", f"{m1['sortino']:.3f}", "Max DD Date", m1["max_drawdown_date"]],
            ["Calmar Ratio", f"{m1['calmar']:.3f}", "", ""],
        ],
        annual=[
            ["Year"] + sorted(m1["annual_returns"].keys()),
            ["Return"]
            + [_fp(m1["annual_returns"][y]) for y in sorted(m1["annual_returns"].keys())],
        ],
        annual_note=f"* {sorted(m1['annual_returns'].keys())[0]} may be partial year",
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
            [
                "Initial Capital",
                _fd(m2["initial_capital"]),
                "SPY B&H CAGR",
                _fp(m2["benchmark_spy_cagr_pct"]),
            ],
            ["Final Value", _fd(m2["final_value"]), "Alpha vs SPY", _fp(m2["alpha_vs_spy_pct"])],
            [
                "Total Return",
                _fp(m2["total_return_pct"]),
                "Total Trades",
                f"{m2['total_trades']} ({m2['upro_trades']}+{m2['tqqq_trades']})",
            ],
            [
                "CAGR",
                _fp(m2["cagr_pct"]),
                "Total Costs",
                _fd(m2["total_commissions"] + m2["total_slippage"]),
            ],
            ["Sharpe Ratio", f"{m2['sharpe']:.3f}", "Max Drawdown", _fp(m2["max_drawdown_pct"])],
            ["Sortino Ratio", f"{m2['sortino']:.3f}", "Max DD Date", m2["max_drawdown_date"]],
            ["Calmar Ratio", f"{m2['calmar']:.3f}", "", ""],
        ],
        annual=[
            ["Year"] + sorted(m2["annual_returns"].keys()),
            ["Return"]
            + [_fp(m2["annual_returns"][y]) for y in sorted(m2["annual_returns"].keys())],
        ],
    )
    story.extend(step2)
    story.append(Spacer(1, 8))

    # Comparison
    story.append(Paragraph("Step 1 vs Step 2 Comparison", s["h1"]))
    dv = m2["final_value"] - m1["final_value"]
    comp_data = [
        ["Metric", "Step 1", "Step 2", "Winner"],
        ["Final Value", _fd(m1["final_value"]), _fd(m2["final_value"]), f"Step 2 (+{_fd(dv)})"],
        ["CAGR", f"{m1['cagr_pct']:.1f}%", f"{m2['cagr_pct']:.1f}%", "Step 2"],
        [
            "Sharpe",
            f"{m1['sharpe']:.3f}",
            f"{m2['sharpe']:.3f}",
            "Step 1" if m1["sharpe"] > m2["sharpe"] else "Step 2",
        ],
        [
            "Max Drawdown",
            _fp(m1["max_drawdown_pct"]),
            _fp(m2["max_drawdown_pct"]),
            "Step 1 (less risk)",
        ],
        ["Alpha vs SPY", _fp(m1["alpha_vs_spy_pct"]), _fp(m2["alpha_vs_spy_pct"]), "Step 2"],
    ]
    story.append(styled_table(comp_data, [W * 0.2, W * 0.2, W * 0.2, W * 0.4]))
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "Both steps deliver strong results. Step 2 has slightly higher returns; "
            "Step 1 has slightly lower drawdown. The difference is small enough that "
            "either is defensible.",
            s["body"],
        )
    )

    # ════════════════════════════════════════════════════════════════
    # PAGE 3: Disclosures
    # ════════════════════════════════════════════════════════════════

    story.append(PageBreak())
    story.append(Paragraph("Important Disclosures", s["h1"]))
    story.append(Spacer(1, 4))

    disclosures = [
        (
            "1. Data Window",
            f"{m1['years']} years ({m1['start_date']} to {m1['end_date']}). "
            "The strategy has not been tested through multi-year bears "
            "(2000\u20132003, 2007\u20132009). Results may not generalize to all market conditions.",
        ),
        (
            "2. Max Drawdown",
            "\u201338% to \u201339% is significant. A $100K portfolio would have dropped to "
            "~$62K at the worst point. SPY\u2019s max drawdown over the same period was "
            "approximately \u201325%.",
        ),
        (
            "3. 2022 Performance",
            "The strategy lost 29\u201332% in 2022. The kill switch prevented catastrophic "
            "losses (UPRO unprotected lost ~65%), but still underperformed SPY (\u201319%) "
            "in the crash year.",
        ),
        (
            "4. Kill Switch Whipsaw",
            "In Mar\u2013Apr 2022, the strategy executed 4 round trips in 20 days as SPY "
            "oscillated around the 200-day SMA. This generated ~$700 in unnecessary "
            "transaction costs. The kill switch has no confirmation period on the "
            "200-SMA leg.",
        ),
        (
            "5. Statistical Significance",
            f"Alpha of {m1['alpha_vs_spy_pct']:.0f}\u2013{m2['alpha_vs_spy_pct']:.0f}% over {m1['years']} years is not conclusive. The confidence interval "
            "is wide enough that the true alpha could be zero. This is historical "
            "outperformance, not a guarantee of future results.",
        ),
    ]
    for title, body in disclosures:
        story.append(Paragraph(title, s["h2"]))
        story.append(Paragraph(body, s["body"]))
        story.append(Spacer(1, 4))

    # Footer
    story.append(Spacer(1, 30))
    story.append(hr())
    story.append(
        Paragraph(
            "CONFIDENTIAL \u2014 Prepared by Insight Fusion Analytics  |  "
            "www.insightfusionanalytics.com",
            s["footer"],
        )
    )

    doc.build(story)
    print(f"  Saved: {output_path}")


if __name__ == "__main__":
    build_pdf()
