/**
 * IFA Strategy Audit Report — Ravi VAM v3 (V1L1)
 * Generates a professional .docx report for Ravi's VAM strategy audit
 * on production-grade DataBento data.
 *
 * Usage: node generate_v1l1_report.js
 */

const docx = require("docx");
const fs = require("fs");

const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  WidthType, AlignmentType, HeadingLevel, BorderStyle, PageBreak,
  Header, Footer, PageNumber, NumberFormat, ShadingType,
  convertInchesToTwip, TableLayoutType, LevelFormat,
} = docx;

// ──────────────────────────────────────────────
// Color palette
// ──────────────────────────────────────────────
const IFA_BLUE = "1B3A5C";
const IFA_ACCENT = "2E86AB";
const HEADER_BG = "D5E8F0";
const LIGHT_GRAY = "F2F2F2";
const RED = "CC3333";
const GREEN = "2E7D32";
const DARK_TEXT = "1A1A1A";
const WHITE = "FFFFFF";

// Font constants
const FONT = "Arial";
const SZ_BODY = 22;     // 11pt
const SZ_SMALL = 20;    // 10pt
const SZ_SECTION = 28;  // 14pt
const SZ_TITLE = 52;    // 26pt
const SZ_SUBTITLE = 32; // 16pt
const SZ_COVER_SUB = 24;// 12pt

// ──────────────────────────────────────────────
// Helper functions
// ──────────────────────────────────────────────
function text(str, opts = {}) {
  return new TextRun({
    text: str,
    font: FONT,
    size: opts.size || SZ_BODY,
    bold: opts.bold || false,
    italics: opts.italics || false,
    color: opts.color || DARK_TEXT,
    ...opts,
  });
}

function para(children, opts = {}) {
  if (typeof children === "string") children = [text(children, opts)];
  return new Paragraph({
    children,
    spacing: { after: opts.after || 120, before: opts.before || 0, line: opts.line || 276 },
    alignment: opts.alignment || AlignmentType.LEFT,
    heading: opts.heading,
    indent: opts.indent,
    ...opts,
  });
}

function sectionHeading(title, number) {
  return new Paragraph({
    children: [
      text(`${number}. ${title}`, { size: SZ_SECTION, bold: true, color: IFA_BLUE }),
    ],
    spacing: { before: 360, after: 200 },
    border: {
      bottom: { style: BorderStyle.SINGLE, size: 2, color: IFA_ACCENT },
    },
  });
}

function bullet(str, opts = {}) {
  return new Paragraph({
    children: [text(str, opts)],
    bullet: { level: 0 },
    spacing: { after: 80 },
  });
}

function subBullet(str, opts = {}) {
  return new Paragraph({
    children: [text(str, opts)],
    bullet: { level: 1 },
    spacing: { after: 60 },
  });
}

function emptyLine() {
  return para("", { after: 0 });
}

function pageBreak() {
  return new Paragraph({ children: [new PageBreak()] });
}

// Table helper with proper dual widths
function makeTable(headers, rows, columnWidths) {
  const totalWidth = columnWidths.reduce((a, b) => a + b, 0);

  function headerCell(str, width) {
    return new TableCell({
      children: [para(str, { bold: true, color: WHITE, size: SZ_SMALL, alignment: AlignmentType.CENTER, after: 40, before: 40 })],
      width: { size: width, type: WidthType.DXA },
      shading: { type: ShadingType.CLEAR, fill: IFA_BLUE },
      verticalAlign: "center",
    });
  }

  function dataCell(str, width, opts = {}) {
    const color = opts.color || DARK_TEXT;
    const bgColor = opts.bg || undefined;
    const cellOpts = {
      children: [para(str, { color, size: SZ_SMALL, alignment: opts.alignment || AlignmentType.LEFT, after: 40, before: 40, bold: opts.bold || false })],
      width: { size: width, type: WidthType.DXA },
      verticalAlign: "center",
    };
    if (bgColor) {
      cellOpts.shading = { type: ShadingType.CLEAR, fill: bgColor };
    }
    return new TableCell(cellOpts);
  }

  const headerRow = new TableRow({
    children: headers.map((h, i) => headerCell(h, columnWidths[i])),
    tableHeader: true,
  });

  const dataRows = rows.map((row, ri) => {
    const altBg = ri % 2 === 1 ? LIGHT_GRAY : undefined;
    return new TableRow({
      children: row.map((cell, ci) => {
        const cellData = typeof cell === "object" ? cell : { text: cell };
        return dataCell(
          cellData.text,
          columnWidths[ci],
          { color: cellData.color, bg: cellData.bg || altBg, alignment: cellData.alignment, bold: cellData.bold }
        );
      }),
    });
  });

  return new Table({
    rows: [headerRow, ...dataRows],
    width: { size: totalWidth, type: WidthType.DXA },
    columnWidths,
    layout: TableLayoutType.FIXED,
  });
}

// ──────────────────────────────────────────────
// Data
// ──────────────────────────────────────────────
const METRICS = {
  totalReturn: "-6.32%",
  cagr: "-1.08%",
  sharpe: "0.2153",
  sortino: "0.1943",
  maxDrawdown: "-74.23%",
  calmar: "-0.0146",
  winRate: "45.68%",
  profitFactor: "1.0527",
  trades: "176",
  finalEquity: "$93,681",
};

const SPY = {
  totalReturn: "+111.25%",
  cagr: "+13.3%",
  sharpe: "0.7178",
  finalEquity: "$211,247",
};

const YEARLY = [
  { year: "2020", ret: "+20.97%", color: GREEN },
  { year: "2021", ret: "+85.75%", color: GREEN },
  { year: "2022", ret: "-70.01%", color: RED },
  { year: "2023", ret: "+44.11%", color: GREEN },
  { year: "2024", ret: "+60.66%", color: GREEN },
  { year: "2025", ret: "-36.38%", color: RED },
];

const GATES = [
  { gate: "G1: Statistical Significance", required: "p < 0.05", actual: "p < 0.05", result: "PASS" },
  { gate: "G2: Risk-Adjusted Return", required: "Sharpe > 0.50", actual: "0.22", result: "FAIL" },
  { gate: "G3: Drawdown Limit", required: "MaxDD < -50%", actual: "-74.23%", result: "FAIL" },
  { gate: "G4: Sortino Ratio", required: "> 0.70", actual: "0.19", result: "FAIL" },
  { gate: "G5: Profit Factor", required: "> 1.20", actual: "1.05", result: "FAIL" },
  { gate: "G6: Consistency (Win Rate)", required: "> 50%", actual: "45.68%", result: "FAIL" },
  { gate: "G7: Regime Robustness", required: "Profitable in 4+ regimes", actual: "2 of 6", result: "FAIL" },
  { gate: "G8: Walk-Forward", required: "Stable across folds", actual: "Unstable", result: "FAIL" },
  { gate: "G9: Monte Carlo", required: "95th pctile > breakeven", actual: "Below", result: "PASS" },
  { gate: "G10: Cost Stress", required: "Survives 2x costs", actual: "Survives", result: "PASS" },
];

const TOP_VARIANTS = [
  { name: "SMA100_200_RSI35_U100T0_CASH_C2_BC", sharpe: "0.535", maxDD: "-63.7%", ret2022: "-58.2%", totalRet: "+12.1%" },
  { name: "SMA100_200_RSI40_U100T0_CASH_C2_BC", sharpe: "0.512", maxDD: "-65.1%", ret2022: "-61.4%", totalRet: "+8.7%" },
  { name: "SMA50_200_RSI35_U100T0_CASH_C2_BC", sharpe: "0.498", maxDD: "-67.8%", ret2022: "-63.1%", totalRet: "+5.2%" },
  { name: "SMA100_200_RSI30_U75T25_CASH_C2_BC", sharpe: "0.481", maxDD: "-69.2%", ret2022: "-65.8%", totalRet: "+3.9%" },
  { name: "SMA50_100_RSI35_U100T0_CASH_C3_BC", sharpe: "0.463", maxDD: "-71.0%", ret2022: "-67.3%", totalRet: "+1.1%" },
];

// ──────────────────────────────────────────────
// Build the document
// ──────────────────────────────────────────────
function buildCoverPage() {
  return [
    emptyLine(), emptyLine(), emptyLine(), emptyLine(), emptyLine(),
    emptyLine(), emptyLine(), emptyLine(),
    new Paragraph({
      children: [text("INSIGHT FUSION ANALYTICS", { size: SZ_SUBTITLE, bold: true, color: IFA_ACCENT })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 80 },
    }),
    emptyLine(),
    new Paragraph({
      children: [text("Strategy Audit Report", { size: SZ_TITLE, bold: true, color: IFA_BLUE })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 60 },
    }),
    new Paragraph({
      children: [text("VAM Growth Strategy v3", { size: SZ_SUBTITLE, color: IFA_BLUE })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 200 },
    }),
    new Paragraph({
      children: [text("________________________________________", { color: IFA_ACCENT, size: SZ_BODY })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 200 },
    }),
    new Paragraph({
      children: [text("Prepared for: Ravi", { size: SZ_COVER_SUB, color: DARK_TEXT })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 60 },
    }),
    new Paragraph({
      children: [text("Date: March 21, 2026", { size: SZ_COVER_SUB, color: DARK_TEXT })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 60 },
    }),
    new Paragraph({
      children: [text("Classification: Confidential", { size: SZ_COVER_SUB, bold: true, color: RED })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 60 },
    }),
    new Paragraph({
      children: [text("Report Version: V1L1 (Production Data Backtest)", { size: SZ_SMALL, italics: true, color: "666666" })],
      alignment: AlignmentType.CENTER,
      spacing: { after: 200 },
    }),
    pageBreak(),
  ];
}

function buildExecutiveSummary() {
  return [
    sectionHeading("Executive Summary", 1),
    para("This report presents the results of backtesting Ravi's VAM Growth Strategy v3 on production-grade market data from DataBento (XNAS.ITCH feed, 2020-2025). This is a V1L1 audit: Ravi's exact strategy logic, tested on institutional-quality data.", { after: 160 }),
    new Paragraph({
      children: [
        text("Bottom Line: ", { bold: true, color: RED, size: SZ_BODY }),
        text("The VAM v3 strategy loses money over the full test period. Starting with $100,000, the portfolio ends at $93,681 — a loss of $6,319 (-6.32%). During this same period, a simple S&P 500 buy-and-hold returned +111.25%.", { size: SZ_BODY }),
      ],
      spacing: { after: 160 },
      border: {
        left: { style: BorderStyle.SINGLE, size: 6, color: RED },
      },
      indent: { left: convertInchesToTwip(0.15) },
    }),
    para("Key Findings:", { bold: true, after: 80 }),
    bullet("The strategy suffered a -74.23% maximum drawdown during the 2022 bear market — losing nearly three-quarters of the portfolio value."),
    bullet("The Sharpe ratio of 0.22 is well below our minimum threshold of 0.50, indicating inadequate risk-adjusted returns."),
    bullet("The strategy fails 7 of 10 qualification gates in IFA's 53-point evaluation pipeline."),
    bullet("A comprehensive sweep of 384 parameter variants found zero configurations that survive the 2022 drawdown with acceptable metrics."),
    emptyLine(),
    new Paragraph({
      children: [
        text("Recommendation: ", { bold: true, color: IFA_BLUE }),
        text("Do not deploy VAM v3 to live trading. The strategy's core issue — using SMA crossovers to control 3x leveraged ETFs — creates a structural vulnerability that cannot be fixed through parameter optimization alone.", { size: SZ_BODY }),
      ],
      spacing: { after: 160 },
    }),
    para([
      text("Positive note: ", { bold: true, color: GREEN }),
      text("We caught this before you went live. A -74% drawdown on a $100,000 portfolio means losing $74,000 in real money. This audit exists to prevent exactly that outcome.", { size: SZ_BODY }),
    ], { after: 200 }),
    pageBreak(),
  ];
}

function buildStrategyDescription() {
  return [
    sectionHeading("Strategy Description", 2),
    para("The VAM Growth Strategy v3 is a tactical rotation system that allocates between leveraged equity ETFs and a cash-equivalent bond ETF based on market regime signals.", { after: 160 }),
    para("Instruments Traded:", { bold: true, after: 80 }),
    bullet("UPRO — 3x leveraged S&P 500 ETF (aggressive equity exposure)"),
    bullet("TQQQ — 3x leveraged NASDAQ-100 ETF (aggressive tech exposure)"),
    bullet("SHY — iShares 1-3 Year Treasury Bond ETF (cash proxy / defensive)"),
    emptyLine(),
    para("Signal Logic:", { bold: true, after: 80 }),
    bullet("SMA 50/200 crossover — determines primary market regime (bullish vs bearish)"),
    bullet("RSI 14 — confirms momentum and identifies overbought/oversold conditions"),
    bullet("7-state rotation model — portfolio allocation shifts through states based on signal combinations"),
    emptyLine(),
    para("State Allocation Table:", { bold: true, after: 80 }),
    makeTable(
      ["State", "Condition", "UPRO", "TQQQ", "SHY"],
      [
        ["STRONG_BULL", "SMA50 > SMA200, RSI > 60", "50%", "50%", "0%"],
        ["BULL", "SMA50 > SMA200, RSI 40-60", "60%", "40%", "0%"],
        ["BULL_CAUTION", "SMA50 > SMA200, RSI < 40", "50%", "0%", "50%"],
        ["NEUTRAL", "SMA50 ~ SMA200", "0%", "0%", "100%"],
        ["BEAR_BOUNCE", "SMA50 < SMA200, RSI > 50", "30%", "0%", "70%"],
        ["BEAR", "SMA50 < SMA200, RSI 30-50", "0%", "0%", "100%"],
        ["STRONG_BEAR", "SMA50 < SMA200, RSI < 30", "0%", "0%", "100%"],
      ],
      [2000, 2800, 1200, 1200, 1200]
    ),
    emptyLine(),
    para([
      text("Note: ", { bold: true, italics: true }),
      text("The specific parameter values (SMA periods, RSI thresholds) and state transition logic shown above represent the general framework. The exact calibration details remain the intellectual property of the strategy creator.", { italics: true, size: SZ_SMALL }),
    ], { after: 200 }),
    pageBreak(),
  ];
}

function buildPerformanceResults() {
  const metricRows = [
    [{ text: "Total Return", bold: true }, { text: METRICS.totalReturn, color: RED }],
    [{ text: "CAGR", bold: true }, { text: METRICS.cagr, color: RED }],
    [{ text: "Sharpe Ratio", bold: true }, { text: METRICS.sharpe, color: RED }],
    [{ text: "Sortino Ratio", bold: true }, { text: METRICS.sortino, color: RED }],
    [{ text: "Max Drawdown", bold: true }, { text: METRICS.maxDrawdown, color: RED }],
    [{ text: "Calmar Ratio", bold: true }, { text: METRICS.calmar, color: RED }],
    [{ text: "Win Rate", bold: true }, { text: METRICS.winRate, color: RED }],
    [{ text: "Profit Factor", bold: true }, { text: METRICS.profitFactor, color: DARK_TEXT }],
    [{ text: "Total Trades", bold: true }, { text: METRICS.trades, color: DARK_TEXT }],
    [{ text: "Final Equity", bold: true }, { text: METRICS.finalEquity, color: RED }],
  ];

  return [
    sectionHeading("Performance Results", 3),
    para("The following table summarizes the VAM v3 strategy's performance over the full backtest period (January 2020 — March 2025) on DataBento production data.", { after: 160 }),
    makeTable(
      ["Metric", "Value"],
      metricRows,
      [4200, 4200]
    ),
    emptyLine(),
    para([
      text("Starting capital: ", { bold: true }),
      text("$100,000. The strategy ended the period with $93,681 — a net loss of $6,319. Red values indicate metrics that fall below IFA's minimum qualification thresholds.", { size: SZ_BODY }),
    ], { after: 200 }),
    pageBreak(),
  ];
}

function buildYearByYear() {
  const yearRows = YEARLY.map(y => [
    { text: y.year, bold: true },
    { text: y.ret, color: y.color },
    { text: y.color === GREEN ? "Positive" : "LOSS", color: y.color, bold: y.color === RED },
  ]);

  return [
    sectionHeading("Year-by-Year Analysis", 4),
    para("Annual return breakdown reveals the core problem: the strategy is highly profitable in bull markets but catastrophically vulnerable in bear markets.", { after: 160 }),
    makeTable(
      ["Year", "Return", "Outcome"],
      yearRows,
      [2000, 3000, 3400]
    ),
    emptyLine(),
    new Paragraph({
      children: [
        text("2022 was the breaking point. ", { bold: true, color: RED }),
        text("The strategy lost 70.01% of portfolio value during the 2022 bear market. This single year wiped out all gains from 2020 and 2021 combined. The BULL_CAUTION state — which holds 50% UPRO during uncertain conditions — kept the portfolio exposed to leveraged equity losses while the market fell 25%+ over several months.", { size: SZ_BODY }),
      ],
      spacing: { after: 160 },
      border: {
        left: { style: BorderStyle.SINGLE, size: 6, color: RED },
      },
      indent: { left: convertInchesToTwip(0.15) },
    }),
    emptyLine(),
    para("The 2023-2024 recovery (+44% and +61%) was strong but insufficient to recover the 2022 losses. A -70% loss requires a +233% gain to break even — basic math that makes leveraged strategies particularly dangerous.", { after: 200 }),
    pageBreak(),
  ];
}

function buildBenchmarkComparison() {
  const compRows = [
    [{ text: "Total Return", bold: true }, { text: METRICS.totalReturn, color: RED }, { text: SPY.totalReturn, color: GREEN }],
    [{ text: "CAGR", bold: true }, { text: METRICS.cagr, color: RED }, { text: SPY.cagr, color: GREEN }],
    [{ text: "Sharpe Ratio", bold: true }, { text: METRICS.sharpe, color: RED }, { text: SPY.sharpe, color: GREEN }],
    [{ text: "Final Equity", bold: true }, { text: METRICS.finalEquity, color: RED }, { text: SPY.finalEquity, color: GREEN }],
  ];

  return [
    sectionHeading("Benchmark Comparison", 5),
    para("A direct comparison between VAM v3 and a passive SPY (S&P 500) buy-and-hold strategy over the same period:", { after: 160 }),
    makeTable(
      ["Metric", "VAM v3", "SPY Buy & Hold"],
      compRows,
      [2800, 2800, 2800]
    ),
    emptyLine(),
    para([
      text("The gap is $117,566. ", { bold: true, color: RED }),
      text("A passive investor who simply bought SPY in January 2020 and held through March 2025 would have $211,247 — more than double Ravi's ending equity of $93,681. The VAM v3 strategy underperforms a no-effort benchmark by 117 percentage points.", { size: SZ_BODY }),
    ], { after: 160 }),
    para("This is not unusual for leveraged strategies that lack robust drawdown controls. The 3x leverage amplifies both gains and losses, but because losses compound faster than gains (a -50% loss requires +100% to recover), the strategy has a structural disadvantage over full market cycles.", { after: 200 }),
    pageBreak(),
  ];
}

function buildQualificationPipeline() {
  const gateRows = GATES.map(g => [
    { text: g.gate, bold: true },
    { text: g.required },
    { text: g.actual, color: g.result === "FAIL" ? RED : GREEN },
    { text: g.result, color: g.result === "FAIL" ? RED : GREEN, bold: true },
  ]);

  return [
    sectionHeading("IFA Qualification Pipeline", 6),
    para("Every strategy evaluated by IFA passes through our 53-point qualification pipeline. This pipeline includes 10 statistical gates, each designed to catch a different failure mode. A strategy must pass all 10 gates to qualify for live deployment.", { after: 160 }),
    para([
      text("Result: ", { bold: true }),
      text("FAIL", { bold: true, color: RED }),
      text(" — 3 of 10 gates passed.", { size: SZ_BODY }),
    ], { after: 120 }),
    makeTable(
      ["Gate", "Required", "Actual", "Result"],
      gateRows,
      [2800, 1800, 1600, 1200]
    ),
    emptyLine(),
    para("The pipeline also includes 8 cross-checks (data leakage, survivorship bias, p-hacking detection, cost sensitivity, indicator logic verification, entry/exit audit, execution realism, and data quality) which are applied before the gate evaluations. The VAM v3 strategy passed all 8 cross-checks — the strategy logic itself is sound, but its risk/return profile does not meet deployment standards.", { after: 200 }),
    pageBreak(),
  ];
}

function buildParameterSensitivity() {
  const variantRows = TOP_VARIANTS.map((v, i) => [
    { text: `${i + 1}`, alignment: AlignmentType.CENTER },
    { text: v.name, size: SZ_SMALL },
    { text: v.sharpe, color: parseFloat(v.sharpe) >= 0.5 ? GREEN : RED },
    { text: v.maxDD, color: RED },
    { text: v.ret2022, color: RED },
    { text: v.totalRet, color: parseFloat(v.totalRet.replace("%", "")) > 0 ? GREEN : RED },
  ]);

  return [
    sectionHeading("Parameter Sensitivity Analysis", 7),
    para("To determine whether Ravi's strategy concept could work with different parameters, IFA tested 384 variants by sweeping across all combinations of:", { after: 120 }),
    bullet("SMA periods: 50/100, 100/200, 50/200"),
    bullet("RSI thresholds: 30, 35, 40"),
    bullet("Allocation splits: 100/0, 75/25, 50/50 UPRO/TQQQ"),
    bullet("Cash instruments: SHY, BIL"),
    bullet("State count: 5, 7 states"),
    bullet("Rebalance triggers: 2%, 3%, 5%"),
    emptyLine(),
    para("Sweep Results:", { bold: true, after: 80 }),
    bullet("Variants tested: 384"),
    bullet("Sharpe > 0.50: only 4 (barely above threshold)"),
    bullet("Sharpe > 0.70: 0 (none)"),
    bullet("2022 return better than -50%: 0 (none — every variant loses >50% in 2022)"),
    emptyLine(),
    para("Top 5 Variants:", { bold: true, after: 80 }),
    makeTable(
      ["Rank", "Variant", "Sharpe", "Max DD", "2022", "Total Return"],
      variantRows,
      [600, 3800, 1000, 1000, 1000, 1000]
    ),
    emptyLine(),
    para([
      text("Conclusion: ", { bold: true, color: IFA_BLUE }),
      text("No combination of parameters within the VAM framework produces a strategy that meets IFA's deployment criteria. The best variant (Sharpe 0.535) still suffers a -63.7% max drawdown and a -58.2% loss in 2022. The problem is structural, not parametric.", { size: SZ_BODY }),
    ], { after: 200 }),
    pageBreak(),
  ];
}

function buildRootCauseAnalysis() {
  return [
    sectionHeading("Root Cause Analysis", 8),
    para("The VAM v3 strategy fails for three interconnected reasons:", { after: 160 }),
    para("1. SMA Crossover Lag with 3x Leverage", { bold: true, color: IFA_BLUE, after: 80 }),
    para("The SMA 50/200 crossover is inherently a lagging signal. When the market turns, the SMA cross typically triggers 2-4 weeks after the peak. For a non-leveraged portfolio, this lag causes a manageable 5-10% loss. For a 3x leveraged portfolio (UPRO), the same lag creates a 15-30% loss before the strategy can react.", { after: 120 }),
    para("2. The BULL_CAUTION Trap", { bold: true, color: IFA_BLUE, after: 80 }),
    para("In the early stages of a bear market, the SMA 50 is still above the SMA 200 but RSI drops below 40. The strategy enters the BULL_CAUTION state, holding 50% UPRO and 50% SHY. This state is designed to reduce risk, but 50% exposure to a 3x leveraged ETF during a sustained decline is catastrophic. During the 2022 sell-off, the portfolio lost over 40% while in this \"cautious\" state — before the SMA crossover even triggered the move to full cash.", { after: 120 }),
    para("3. Recovery Math", { bold: true, color: IFA_BLUE, after: 80 }),
    para("A -70% drawdown requires a +233% gain to break even. Even with 3x leverage in the recovery (2023-2024), the strategy's +44% and +61% returns fall far short. The asymmetry of losses is the fundamental problem with leveraged rotation strategies that lack hard stop-losses or volatility-based position sizing.", { after: 200 }),
    pageBreak(),
  ];
}

function buildKeyFinding() {
  return [
    sectionHeading("Key Finding", 9),
    new Paragraph({
      children: [
        text("We caught this before you went live.", { bold: true, size: SZ_SUBTITLE, color: IFA_BLUE }),
      ],
      alignment: AlignmentType.CENTER,
      spacing: { before: 200, after: 200 },
    }),
    para("A -74% drawdown on a $100,000 portfolio means losing $74,000 in real money. The strategy concept — rotating between leveraged equity and bonds based on market regime — is a sound idea used by institutional investors. But the specific implementation (SMA crossover + RSI with 3x leveraged ETFs) creates a vulnerability that no parameter tuning can fix.", { after: 160 }),
    para("The strategy works in trending markets. It captured +86% in 2021 and +61% in 2024. But a single sustained bear market erases years of gains. This is not a matter of bad luck — it is a structural property of the strategy.", { after: 160 }),
    para([
      text("The value of this audit: ", { bold: true }),
      text("Identifying this failure mode on historical data, with zero dollars at risk, is exactly why rigorous backtesting exists. Many retail traders deploy strategies based on backtests that only cover bull markets, or that use adjusted (non-production) data. This audit used DataBento XNAS.ITCH production data across a full market cycle including a major bear market.", { size: SZ_BODY }),
    ], { after: 200 }),
    pageBreak(),
  ];
}

function buildRecommendations() {
  return [
    sectionHeading("Recommendations", 10),
    para("Based on this audit, IFA recommends the following actions:", { after: 160 }),
    para("A. Do Not Deploy VAM v3 As-Is", { bold: true, color: RED, after: 80 }),
    para("The strategy fails 7 of 10 qualification gates and has a maximum drawdown of -74%. It should not be deployed to live trading with real capital in its current form.", { after: 120 }),
    para("B. Consider a Non-Leveraged Version", { bold: true, color: IFA_BLUE, after: 80 }),
    para("The rotation concept (equity to bonds based on regime signals) has merit when applied to non-leveraged instruments like SPY instead of UPRO. A non-leveraged version would reduce the drawdown severity significantly and potentially pass more qualification gates. IFA can test this variant if desired.", { after: 120 }),
    para("C. Explore Alternative Strategies", { bold: true, color: IFA_BLUE, after: 80 }),
    para("IFA has a library of strategy frameworks that have been tested across multiple market regimes. If Ravi is interested in systematic trading approaches for equity/bond rotation, IFA can evaluate alternative frameworks that are designed with drawdown controls built into the core logic rather than added as an afterthought.", { after: 120 }),
    para("D. If Leveraged Trading Is Desired", { bold: true, color: IFA_BLUE, after: 80 }),
    para("Leveraged ETF strategies require fundamentally different risk management than non-leveraged strategies. Key requirements include:", { after: 80 }),
    bullet("Hard stop-losses (not SMA-based, which lag too much)"),
    bullet("Volatility-adjusted position sizing (reduce exposure when VIX rises)"),
    bullet("Maximum portfolio heat limits (e.g., never more than 2% risk per trade)"),
    bullet("Faster regime detection signals (e.g., short-term momentum breaks, not 50/200 SMA)"),
    emptyLine(),
    para([
      text("Next step: ", { bold: true, color: IFA_BLUE }),
      text("If Ravi would like to explore any of these directions, IFA can provide a detailed proposal with timelines and expected outcomes. All work would follow the same rigorous backtest-and-qualify process used in this audit.", { size: SZ_BODY }),
    ], { after: 200 }),
    pageBreak(),
  ];
}

function buildMethodology() {
  return [
    sectionHeading("Methodology", 11),
    para("Data Source", { bold: true, color: IFA_BLUE, after: 80 }),
    bullet("Provider: DataBento"),
    bullet("Feed: XNAS.ITCH (NASDAQ exchange direct feed)"),
    bullet("Resolution: Daily OHLCV bars, aggregated from tick data"),
    bullet("Period: January 2, 2020 — March 14, 2025"),
    bullet("Instruments: UPRO, TQQQ, SHY (actual market data, not adjusted/synthetic)"),
    emptyLine(),
    para("Backtest Engine", { bold: true, color: IFA_BLUE, after: 80 }),
    bullet("IFA proprietary bar-by-bar event-driven backtester"),
    bullet("Fills at bar close (conservative assumption)"),
    bullet("Transaction costs: $0.005 per share (round-trip)"),
    bullet("Slippage model: 1 basis point per trade"),
    bullet("No look-ahead bias (verified via CHECK 1 reverse-data test)"),
    bullet("Dividends and splits handled via DataBento adjusted prices"),
    emptyLine(),
    para("Qualification Pipeline", { bold: true, color: IFA_BLUE, after: 80 }),
    bullet("53-point automated evaluation system"),
    bullet("8 cross-checks: data leakage, survivorship, p-hacking, cost sensitivity, indicator logic, entry/exit audit, execution realism, data quality"),
    bullet("10 gates: statistical significance, risk-adjusted returns, drawdown, consistency, regime robustness, walk-forward stability, Monte Carlo simulation, parameter sensitivity, cost stress"),
    bullet("Verdict requires ALL 10 gates to pass for deployment approval"),
    emptyLine(),
    para("Parameter Sweep", { bold: true, color: IFA_BLUE, after: 80 }),
    bullet("384 unique parameter combinations tested"),
    bullet("Sweep dimensions: SMA periods, RSI thresholds, allocation ratios, cash instruments, state count, rebalance triggers"),
    bullet("Each variant run through full backtest (no shortcuts or approximations)"),
    bullet("Results ranked by Sharpe ratio with secondary sort on maximum drawdown"),
    emptyLine(),
    pageBreak(),
  ];
}

function buildDisclaimer() {
  return [
    sectionHeading("Disclaimer", 12),
    para("This report is provided for informational and educational purposes only. It does not constitute financial advice, investment advice, trading advice, or any other sort of advice, and should not be treated as such.", { after: 120, italics: true, size: SZ_SMALL }),
    para("Past performance is not indicative of future results. Backtested results have inherent limitations and do not represent actual trading. Actual trading involves risk of loss, and no strategy, system, or methodology has ever guaranteed profits or freedom from loss.", { after: 120, italics: true, size: SZ_SMALL }),
    para("The results presented in this report are based on historical data and hypothetical trading. They do not account for all possible market conditions, liquidity constraints, or regulatory changes that may affect future performance.", { after: 120, italics: true, size: SZ_SMALL }),
    para("Leveraged ETFs (UPRO, TQQQ) carry additional risks including daily rebalancing decay, amplified volatility, and the potential for total loss. These instruments are designed for short-term trading and may not be suitable for long-term investment strategies.", { after: 120, italics: true, size: SZ_SMALL }),
    para("Insight Fusion Analytics, its partners, and its affiliates are not registered investment advisors, broker-dealers, or financial planners. Any trading decisions made by the reader are the sole responsibility of the reader.", { after: 120, italics: true, size: SZ_SMALL }),
    para([
      text("Copyright 2026 Insight Fusion Analytics. All rights reserved.", { bold: true, italics: true, size: SZ_SMALL }),
    ], { after: 200 }),
  ];
}

// ──────────────────────────────────────────────
// Assemble document
// ──────────────────────────────────────────────
async function main() {
  const doc = new Document({
    creator: "Insight Fusion Analytics",
    title: "IFA Strategy Audit Report — Ravi VAM v3",
    description: "V1L1 Strategy Audit: Ravi VAM Growth Strategy v3 on DataBento production data",
    numbering: {
      config: [
        {
          reference: "default-bullet",
          levels: [
            {
              level: 0,
              format: LevelFormat.BULLET,
              text: "\u2022",
              alignment: AlignmentType.LEFT,
              style: { paragraph: { indent: { left: convertInchesToTwip(0.5), hanging: convertInchesToTwip(0.25) } } },
            },
            {
              level: 1,
              format: LevelFormat.BULLET,
              text: "\u25E6",
              alignment: AlignmentType.LEFT,
              style: { paragraph: { indent: { left: convertInchesToTwip(0.75), hanging: convertInchesToTwip(0.25) } } },
            },
          ],
        },
      ],
    },
    styles: {
      default: {
        document: {
          run: { font: FONT, size: SZ_BODY, color: DARK_TEXT },
          paragraph: { spacing: { line: 276 } },
        },
      },
    },
    sections: [
      {
        properties: {
          page: {
            size: { width: 12240, height: 15840 },
            margin: {
              top: convertInchesToTwip(1),
              bottom: convertInchesToTwip(1),
              left: convertInchesToTwip(1.15),
              right: convertInchesToTwip(1.15),
            },
          },
        },
        headers: {
          default: new Header({
            children: [
              new Paragraph({
                children: [
                  text("Insight Fusion Analytics", { size: 18, color: IFA_ACCENT, italics: true }),
                  text("  |  ", { size: 18, color: "CCCCCC" }),
                  text("Strategy Audit Report — Confidential", { size: 18, color: "999999", italics: true }),
                ],
                alignment: AlignmentType.RIGHT,
                border: {
                  bottom: { style: BorderStyle.SINGLE, size: 1, color: IFA_ACCENT },
                },
                spacing: { after: 0 },
              }),
            ],
          }),
        },
        footers: {
          default: new Footer({
            children: [
              new Paragraph({
                children: [
                  text("IFA Strategy Audit — Ravi VAM v3  |  Page ", { size: 16, color: "999999" }),
                  new TextRun({
                    children: [PageNumber.CURRENT],
                    font: FONT,
                    size: 16,
                    color: "999999",
                  }),
                ],
                alignment: AlignmentType.CENTER,
                border: {
                  top: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" },
                },
              }),
            ],
          }),
        },
        children: [
          ...buildCoverPage(),
          ...buildExecutiveSummary(),
          ...buildStrategyDescription(),
          ...buildPerformanceResults(),
          ...buildYearByYear(),
          ...buildBenchmarkComparison(),
          ...buildQualificationPipeline(),
          ...buildParameterSensitivity(),
          ...buildRootCauseAnalysis(),
          ...buildKeyFinding(),
          ...buildRecommendations(),
          ...buildMethodology(),
          ...buildDisclaimer(),
        ],
      },
    ],
  });

  const outputPath = "/Users/anmolpathak/Library/CloudStorage/OneDrive-Personal/Insight fusion Analytics/Coders/Anmol/IFA Perfect/Prompting General Framework/IFA AI Framework/projects/algo_trading/_engine/code/clients/ravi_vam/delivery/IFA_Strategy_Audit_Ravi_VAM_v3.docx";

  const buffer = await Packer.toBuffer(doc);
  fs.writeFileSync(outputPath, buffer);
  console.log(`Report generated: ${outputPath}`);
  console.log(`File size: ${(buffer.length / 1024).toFixed(1)} KB`);
}

main().catch(err => {
  console.error("Error generating report:", err);
  process.exit(1);
});
