const docx = require("docx");
const fs = require("fs");

const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  WidthType, AlignmentType, HeadingLevel, BorderStyle, PageBreak,
  Header, Footer, PageNumber, NumberFormat, ShadingType, TabStopPosition,
  TabStopType, convertInchesToTwip, TableLayoutType,
} = docx;

// Color palette
const IFA_BLUE = "1B3A5C";
const IFA_ACCENT = "2E86AB";
const LIGHT_GRAY = "F2F2F2";
const MED_GRAY = "D9D9D9";
const DARK_TEXT = "1A1A1A";
const WHITE = "FFFFFF";

// Reusable styles
const FONT = "Arial";
const FONT_SIZE_BODY = 22; // 11pt in half-points
const FONT_SIZE_SMALL = 20; // 10pt
const FONT_SIZE_SECTION = 28; // 14pt
const FONT_SIZE_TITLE = 48; // 24pt
const FONT_SIZE_SUBTITLE = 32; // 16pt

function text(str, opts = {}) {
  return new TextRun({
    text: str,
    font: FONT,
    size: opts.size || FONT_SIZE_BODY,
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
      text(`${number}. ${title}`, { size: FONT_SIZE_SECTION, bold: true, color: IFA_BLUE }),
    ],
    spacing: { before: 360, after: 200 },
    border: {
      bottom: { style: BorderStyle.SINGLE, size: 2, color: IFA_ACCENT },
    },
  });
}

function bulletPoint(str, opts = {}) {
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

// Table helpers
function headerCell(str, width) {
  return new TableCell({
    children: [para([text(str, { bold: true, color: WHITE, size: FONT_SIZE_SMALL })], { alignment: AlignmentType.LEFT, after: 40 })],
    width: { size: width, type: WidthType.PERCENTAGE },
    shading: { type: ShadingType.SOLID, color: IFA_BLUE, fill: IFA_BLUE },
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
  });
}

function bodyCell(children, width, shaded = false) {
  if (typeof children === "string") children = [para([text(children, { size: FONT_SIZE_SMALL })], { after: 40 })];
  return new TableCell({
    children,
    width: { size: width, type: WidthType.PERCENTAGE },
    shading: shaded ? { type: ShadingType.SOLID, color: LIGHT_GRAY, fill: LIGHT_GRAY } : undefined,
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
  });
}

// ─── COVER PAGE ────────────────────────────────────────────
const coverPage = [
  emptyLine(), emptyLine(), emptyLine(), emptyLine(), emptyLine(),
  emptyLine(), emptyLine(), emptyLine(),
  new Paragraph({
    children: [
      text("INSIGHT FUSION ANALYTICS", { size: 20, bold: true, color: IFA_ACCENT, characterSpacing: 200 }),
    ],
    alignment: AlignmentType.CENTER,
    spacing: { after: 200 },
  }),
  new Paragraph({
    children: [new TextRun({ text: "______________________________", font: FONT, size: 24, color: IFA_ACCENT })],
    alignment: AlignmentType.CENTER,
    spacing: { after: 400 },
  }),
  new Paragraph({
    children: [text("Strategy Backtesting &\nOptimization Proposal", { size: FONT_SIZE_TITLE, bold: true, color: IFA_BLUE })],
    alignment: AlignmentType.CENTER,
    spacing: { after: 300 },
  }),
  new Paragraph({
    children: [text("VAM Split Strategy + Predatory Short Strategy", { size: FONT_SIZE_SUBTITLE, color: IFA_ACCENT })],
    alignment: AlignmentType.CENTER,
    spacing: { after: 600 },
  }),
  new Paragraph({
    children: [new TextRun({ text: "______________________________", font: FONT, size: 24, color: MED_GRAY })],
    alignment: AlignmentType.CENTER,
    spacing: { after: 400 },
  }),
  para([text("Prepared for: ", { size: FONT_SIZE_BODY, color: "666666" }), text("Ravi Mareedu", { size: FONT_SIZE_BODY, bold: true, color: IFA_BLUE })], { alignment: AlignmentType.CENTER, after: 100 }),
  para([text("Prepared by: ", { size: FONT_SIZE_BODY, color: "666666" }), text("Insight Fusion Analytics", { size: FONT_SIZE_BODY, bold: true, color: IFA_BLUE })], { alignment: AlignmentType.CENTER, after: 100 }),
  para([text("Date: ", { size: FONT_SIZE_BODY, color: "666666" }), text("March 2026", { size: FONT_SIZE_BODY, bold: true, color: IFA_BLUE })], { alignment: AlignmentType.CENTER, after: 400 }),
  new Paragraph({
    children: [text("CONFIDENTIAL", { size: 18, bold: true, color: "999999", characterSpacing: 300 })],
    alignment: AlignmentType.CENTER,
    spacing: { after: 0 },
  }),
  new Paragraph({ children: [new PageBreak()] }),
];

// ─── EXECUTIVE SUMMARY ────────────────────────────────────
const execSummary = [
  sectionHeading("Executive Summary", 1),
  para("Ravi Mareedu (in partnership with Sudhir) has developed a sophisticated dual-strategy system designed for the US equity markets, utilizing leveraged ETFs including UPRO, TQQQ, and SPXU."),
  emptyLine(),
  para([text("Strategy 1 — VAM Split: ", { bold: true }), text("A momentum-based allocation framework with independent sleeve management, defensive trim logic, and a kill switch mechanism for capital preservation during extreme market conditions.")]),
  emptyLine(),
  para([text("Strategy 2 — Predatory Short: ", { bold: true }), text("A counter-trend positioning strategy designed to profit from market crashes and severe downturns by entering short exposure via inverse ETFs at precisely defined trigger points.")]),
  emptyLine(),
  para("Insight Fusion Analytics (IFA) will backtest both strategies across 14+ years of historical data (2011 to present), covering multiple complete market cycles including the 2018 correction, COVID crash, 2022 bear market, and subsequent recoveries."),
  emptyLine(),
  para("The engagement will validate the strategy rules, stress-test parameters, identify structural weaknesses, and deliver production-ready results with full statistical rigor through IFA's 53-point strategy qualification pipeline."),
  new Paragraph({ children: [new PageBreak()] }),
];

// ─── SCOPE OF WORK ────────────────────────────────────────
const scopeOfWork = [
  sectionHeading("Scope of Work", 2),

  // Phase 1
  para([text("Phase 1: Strategy Implementation & Backtest", { size: 24, bold: true, color: IFA_BLUE })], { before: 200, after: 120 }),
  bulletPoint("Translate Ravi's strategy rules into a rigorous state-machine model (7 states identified from strategy meeting)"),
  bulletPoint("Independent sleeve management — UPRO follows SPY signals, TQQQ follows QQQ signals"),
  bulletPoint("Signal priority hierarchy: Kill Switch > SMA Breach > RSI triggers"),
  bulletPoint("Implement on historical data using split-adjusted daily prices"),
  bulletPoint("Transaction costs included: commission + slippage modeling"),
  para([text("Deliverables: ", { bold: true }), text("Complete backtest results, full trade log, equity curve with drawdown overlay")]),
  emptyLine(),

  // Phase 2
  para([text("Phase 2: Validation & Cross-Checks", { size: 24, bold: true, color: IFA_BLUE })], { before: 200, after: 120 }),
  bulletPoint("Year-by-year performance breakdown (2011-2025)"),
  bulletPoint("Maximum drawdown analysis with recovery period calculations"),
  bulletPoint("Critical event verification: COVID crash (Mar 2020), Christmas correction (Dec 2018), 2022 bear market"),
  bulletPoint("Benchmark comparison: VAM system vs. SPY buy-and-hold vs. UPRO buy-and-hold"),
  bulletPoint("Regime analysis: performance in bull, bear, and sideways market conditions"),
  bulletPoint("Trade frequency and holding period analysis"),
  emptyLine(),

  // Phase 3
  para([text("Phase 3: Optimization & Recommendations", { size: 24, bold: true, color: IFA_BLUE })], { before: 200, after: 120 }),
  bulletPoint("Identify structural weaknesses in current parameter set"),
  bulletPoint("Parameter sensitivity testing (VIX thresholds, SMA periods, trim percentages, RSI levels)"),
  bulletPoint("Reduce churn in Predatory Short strategy — minimize false signals"),
  bulletPoint("Deliver optimized parameter set with before/after comparison"),
  bulletPoint("Risk-adjusted performance metrics: Sharpe ratio, Sortino ratio, Calmar ratio, max drawdown"),
  bulletPoint("Walk-forward analysis to validate optimization is not curve-fit"),
  emptyLine(),

  // Phase 4
  para([text("Phase 4: Production Report & Dashboard", { size: 24, bold: true, color: IFA_BLUE, italics: true }), text("  (Optional)", { size: 22, italics: true, color: "999999" })], { before: 200, after: 120 }),
  bulletPoint("Professional performance report with publication-quality charts"),
  bulletPoint("Interactive web dashboard where Ravi can adjust parameters and visualize results in real-time"),
  bulletPoint("No strategy code revealed — black-box results only"),
  bulletPoint("Secure, private access with authentication"),
  new Paragraph({ children: [new PageBreak()] }),
];

// ─── DELIVERABLES ─────────────────────────────────────────
const deliverables = [
  sectionHeading("Deliverables", 3),
  new Table({
    width: { size: 100, type: WidthType.PERCENTAGE },
    rows: [
      new TableRow({
        children: [
          headerCell("Deliverable", 55),
          headerCell("Format", 20),
          headerCell("Phase", 25),
        ],
      }),
      ...[
        ["Summary workbook with Performance, Trade Log, Equity Curve, Year-by-Year sheets", "Excel (.xlsx)", "Phase 1-2", false],
        ["Professional performance analysis report", "PDF", "Phase 2", true],
        ["Optimization recommendations document with before/after comparisons", "PDF", "Phase 3", false],
        ["Optimized parameter set with statistical validation", "PDF / Excel", "Phase 3", true],
        ["Interactive strategy dashboard (Optional)", "Web URL", "Phase 4", false],
      ].map(([d, f, p, shaded]) =>
        new TableRow({
          children: [
            bodyCell(d, 55, shaded),
            bodyCell(f, 20, shaded),
            bodyCell(p, 25, shaded),
          ],
        })
      ),
    ],
  }),
  emptyLine(),
];

// ─── TIMELINE ─────────────────────────────────────────────
const timeline = [
  sectionHeading("Timeline", 4),
  new Table({
    width: { size: 100, type: WidthType.PERCENTAGE },
    rows: [
      new TableRow({
        children: [
          headerCell("Phase", 45),
          headerCell("Duration", 25),
          headerCell("Status", 30),
        ],
      }),
      ...[
        ["Phase 1: Strategy Implementation & Backtest", "3-5 business days", "Core", false],
        ["Phase 2: Validation & Cross-Checks", "2-3 business days", "Core", true],
        ["Phase 3: Optimization & Recommendations", "3-5 business days", "Recommended", false],
        ["Phase 4: Production Report & Dashboard", "5-7 business days", "Optional", true],
      ].map(([ph, dur, st, shaded]) =>
        new TableRow({
          children: [
            bodyCell(ph, 45, shaded),
            bodyCell(dur, 25, shaded),
            bodyCell(st, 30, shaded),
          ],
        })
      ),
      new TableRow({
        children: [
          new TableCell({
            children: [para([text("Total Estimated Duration", { bold: true, size: FONT_SIZE_SMALL })], { after: 40 })],
            width: { size: 45, type: WidthType.PERCENTAGE },
            shading: { type: ShadingType.SOLID, color: IFA_BLUE, fill: IFA_BLUE },
            margins: { top: 60, bottom: 60, left: 100, right: 100 },
          }),
          new TableCell({
            children: [para([text("2-3 weeks", { bold: true, size: FONT_SIZE_SMALL, color: WHITE })], { after: 40 })],
            width: { size: 25, type: WidthType.PERCENTAGE },
            shading: { type: ShadingType.SOLID, color: IFA_BLUE, fill: IFA_BLUE },
            margins: { top: 60, bottom: 60, left: 100, right: 100 },
            columnSpan: 2,
          }),
        ],
      }),
    ],
  }),
  emptyLine(),
  para([text("Note: ", { bold: true, italics: true }), text("Timeline begins upon project confirmation and receipt of initial payment. Phases 1 and 2 run as a single core engagement. Phase 3 is highly recommended. Phase 4 is available as an add-on.", { italics: true, color: "666666" })]),
  new Paragraph({ children: [new PageBreak()] }),
];

// ─── INVESTMENT ───────────────────────────────────────────
const investment = [
  sectionHeading("Investment", 5),
  new Table({
    width: { size: 100, type: WidthType.PERCENTAGE },
    rows: [
      new TableRow({
        children: [
          headerCell("Package", 45),
          headerCell("Includes", 35),
          headerCell("Investment", 20),
        ],
      }),
      new TableRow({
        children: [
          bodyCell([
            para([text("Core Package", { bold: true, size: FONT_SIZE_SMALL })], { after: 20 }),
            para([text("(Recommended Minimum)", { italics: true, size: 18, color: "666666" })], { after: 20 }),
          ], 45),
          bodyCell([
            para([text("Phase 1 + Phase 2", { size: FONT_SIZE_SMALL })], { after: 20 }),
            para([text("Implementation, backtest, validation, cross-checks", { size: 18, color: "666666" })], { after: 20 }),
          ], 35),
          bodyCell([
            para([text("[TO BE DISCUSSED]", { bold: true, size: FONT_SIZE_SMALL, color: IFA_ACCENT })], { after: 20 }),
          ], 20),
        ],
      }),
      new TableRow({
        children: [
          bodyCell([
            para([text("Optimization Add-On", { bold: true, size: FONT_SIZE_SMALL })], { after: 20 }),
            para([text("(Highly Recommended)", { italics: true, size: 18, color: "666666" })], { after: 20 }),
          ], 45, true),
          bodyCell([
            para([text("Phase 3", { size: FONT_SIZE_SMALL })], { after: 20 }),
            para([text("Parameter tuning, sensitivity analysis, walk-forward validation", { size: 18, color: "666666" })], { after: 20 }),
          ], 35, true),
          bodyCell([
            para([text("[TO BE DISCUSSED]", { bold: true, size: FONT_SIZE_SMALL, color: IFA_ACCENT })], { after: 20 }),
          ], 20, true),
        ],
      }),
      new TableRow({
        children: [
          bodyCell([
            para([text("Dashboard Add-On", { bold: true, size: FONT_SIZE_SMALL })], { after: 20 }),
            para([text("(Optional)", { italics: true, size: 18, color: "666666" })], { after: 20 }),
          ], 45),
          bodyCell([
            para([text("Phase 4", { size: FONT_SIZE_SMALL })], { after: 20 }),
            para([text("Interactive web dashboard with parameter controls", { size: 18, color: "666666" })], { after: 20 }),
          ], 35),
          bodyCell([
            para([text("[TO BE DISCUSSED]", { bold: true, size: FONT_SIZE_SMALL, color: IFA_ACCENT })], { after: 20 }),
          ], 20),
        ],
      }),
    ],
  }),
  emptyLine(),
];

// ─── ASSUMPTIONS ──────────────────────────────────────────
const assumptions = [
  sectionHeading("Assumptions", 6),
  bulletPoint("Client has provided strategy rules via meeting recording (completed)"),
  bulletPoint("Historical data sourced from Yahoo Finance (free) or Databento (premium — additional cost if required)"),
  bulletPoint("Backtest uses daily timeframe with close-to-close execution assumptions"),
  bulletPoint("Split-adjusted prices used for all leveraged ETF calculations"),
  bulletPoint("Results are historical simulations and do not guarantee future performance"),
  bulletPoint("No live trading, broker integration, or real-money execution is included in this scope"),
  bulletPoint("Client will be available for clarification questions during implementation (response within 24 hours)"),
  bulletPoint("Any changes to strategy rules after Phase 1 begins may require additional time and cost"),
  emptyLine(),
];

// ─── ABOUT IFA ────────────────────────────────────────────
const aboutIFA = [
  sectionHeading("About Insight Fusion Analytics", 7),
  para("Insight Fusion Analytics (IFA) is a quantitative research and data analytics firm specializing in algorithmic trading strategy development, backtesting, and optimization."),
  emptyLine(),
  para("Our core capabilities include:"),
  bulletPoint("Rigorous strategy backtesting with event-driven simulation engines"),
  bulletPoint("53-point strategy qualification pipeline — covering statistical significance, regime robustness, walk-forward validation, Monte Carlo simulation, and parameter sensitivity analysis"),
  bulletPoint("Experience across NSE (India), US equity markets, and futures markets"),
  bulletPoint("Professional reporting with institutional-grade analytics and visualizations"),
  bulletPoint("Interactive dashboards for real-time strategy monitoring and parameter exploration"),
  emptyLine(),
  para("IFA combines deep domain expertise in financial markets with advanced data science methodologies to deliver results that institutional traders rely on. Every strategy we analyze goes through the same qualification process used by professional quantitative funds."),
  new Paragraph({ children: [new PageBreak()] }),
];

// ─── TERMS ────────────────────────────────────────────────
const terms = [
  sectionHeading("Terms & Conditions", 8),

  para([text("Payment", { bold: true, size: 24, color: IFA_BLUE })], { before: 120, after: 100 }),
  bulletPoint("50% of the agreed fee due upfront upon project confirmation"),
  bulletPoint("Remaining 50% due upon delivery of final results"),
  bulletPoint("Phase 4 (if selected) is invoiced separately upon completion"),
  emptyLine(),

  para([text("Intellectual Property", { bold: true, size: 24, color: IFA_BLUE })], { before: 120, after: 100 }),
  bulletPoint("All strategy logic, rules, and parameters remain the exclusive intellectual property of the client (Ravi Mareedu and Sudhir)"),
  bulletPoint("IFA retains ownership of the backtesting framework, tools, and analytical methodologies used"),
  bulletPoint("Deliverables (reports, Excel files, dashboards) are licensed to the client for unlimited personal and business use"),
  emptyLine(),

  para([text("Confidentiality", { bold: true, size: 24, color: IFA_BLUE })], { before: 120, after: 100 }),
  bulletPoint("All strategy details, performance results, and client information are treated as strictly confidential"),
  bulletPoint("IFA will not share, publish, or disclose any results or strategy parameters to third parties"),
  bulletPoint("IFA may reference the engagement (without strategy details) for portfolio/case study purposes only with client's written consent"),
  emptyLine(),

  para([text("Disclaimer", { bold: true, size: 24, color: IFA_BLUE })], { before: 120, after: 100 }),
  bulletPoint("All results presented are based on historical backtesting and simulated trading"),
  bulletPoint("Past performance does not guarantee future results"),
  bulletPoint("IFA does not provide investment advice and is not responsible for trading decisions made based on the analysis"),
  emptyLine(), emptyLine(),

  // Signature area
  new Paragraph({
    children: [new TextRun({ text: "______________________________", font: FONT, size: 24, color: MED_GRAY })],
    spacing: { before: 600, after: 100 },
  }),
  emptyLine(),
  new Table({
    width: { size: 100, type: WidthType.PERCENTAGE },
    borders: {
      top: { style: BorderStyle.NONE },
      bottom: { style: BorderStyle.NONE },
      left: { style: BorderStyle.NONE },
      right: { style: BorderStyle.NONE },
      insideHorizontal: { style: BorderStyle.NONE },
      insideVertical: { style: BorderStyle.NONE },
    },
    rows: [
      new TableRow({
        children: [
          new TableCell({
            children: [
              para([text("Client Acceptance", { bold: true, size: 22, color: IFA_BLUE })], { after: 200 }),
              para([text("Name: ______________________", { size: FONT_SIZE_SMALL })], { after: 120 }),
              para([text("Signature: __________________", { size: FONT_SIZE_SMALL })], { after: 120 }),
              para([text("Date: ______________________", { size: FONT_SIZE_SMALL })], { after: 40 }),
            ],
            width: { size: 50, type: WidthType.PERCENTAGE },
            borders: {
              top: { style: BorderStyle.NONE }, bottom: { style: BorderStyle.NONE },
              left: { style: BorderStyle.NONE }, right: { style: BorderStyle.NONE },
            },
          }),
          new TableCell({
            children: [
              para([text("IFA Authorization", { bold: true, size: 22, color: IFA_BLUE })], { after: 200 }),
              para([text("Name: Anmol Pathak", { size: FONT_SIZE_SMALL })], { after: 120 }),
              para([text("Signature: __________________", { size: FONT_SIZE_SMALL })], { after: 120 }),
              para([text("Date: ______________________", { size: FONT_SIZE_SMALL })], { after: 40 }),
            ],
            width: { size: 50, type: WidthType.PERCENTAGE },
            borders: {
              top: { style: BorderStyle.NONE }, bottom: { style: BorderStyle.NONE },
              left: { style: BorderStyle.NONE }, right: { style: BorderStyle.NONE },
            },
          }),
        ],
      }),
    ],
  }),
];

// ─── DOCUMENT ─────────────────────────────────────────────
const doc = new Document({
  styles: {
    default: {
      document: {
        run: { font: FONT, size: FONT_SIZE_BODY, color: DARK_TEXT },
      },
    },
    paragraphStyles: [
      {
        id: "ListParagraph",
        name: "List Paragraph",
        basedOn: "Normal",
        run: { font: FONT, size: FONT_SIZE_BODY },
      },
    ],
  },
  numbering: {
    config: [{
      reference: "bullet-list",
      levels: [
        { level: 0, format: NumberFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: convertInchesToTwip(0.5), hanging: convertInchesToTwip(0.25) } } } },
        { level: 1, format: NumberFormat.BULLET, text: "\u25E6", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: convertInchesToTwip(1.0), hanging: convertInchesToTwip(0.25) } } } },
      ],
    }],
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 }, // US Letter
        margin: { top: 1440, bottom: 1200, left: 1440, right: 1440 },
      },
    },
    headers: {
      default: new Header({
        children: [
          new Paragraph({
            children: [
              text("INSIGHT FUSION ANALYTICS", { size: 16, bold: true, color: IFA_ACCENT, characterSpacing: 150 }),
              text("    |    ", { size: 16, color: MED_GRAY }),
              text("Strategy Backtesting Proposal — Ravi Mareedu", { size: 16, color: "999999" }),
            ],
            alignment: AlignmentType.LEFT,
            border: { bottom: { style: BorderStyle.SINGLE, size: 1, color: MED_GRAY, space: 4 } },
          }),
        ],
      }),
      first: new Header({ children: [new Paragraph({ children: [] })] }),
    },
    footers: {
      default: new Footer({
        children: [
          new Paragraph({
            children: [
              text("Confidential — Insight Fusion Analytics", { size: 16, color: "999999" }),
              text("        ", {}),
              text("Page ", { size: 16, color: "999999" }),
              new TextRun({ children: [PageNumber.CURRENT], font: FONT, size: 16, color: "999999" }),
              text(" of ", { size: 16, color: "999999" }),
              new TextRun({ children: [PageNumber.TOTAL_PAGES], font: FONT, size: 16, color: "999999" }),
            ],
            alignment: AlignmentType.CENTER,
            border: { top: { style: BorderStyle.SINGLE, size: 1, color: MED_GRAY, space: 4 } },
          }),
        ],
      }),
    },
    children: [
      ...coverPage,
      ...execSummary,
      ...scopeOfWork,
      ...deliverables,
      ...timeline,
      ...investment,
      ...assumptions,
      ...aboutIFA,
      ...terms,
    ],
  }],
});

const OUTPUT = "/Users/anmolpathak/Library/CloudStorage/OneDrive-Personal/Insight fusion Analytics/Coders/Anmol/IFA Perfect/Prompting General Framework/IFA AI Framework/projects/algo_trading/_engine/code/results/IFA_Proposal_Ravi_VAM_Strategy.docx";

Packer.toBuffer(doc).then((buffer) => {
  fs.writeFileSync(OUTPUT, buffer);
  console.log("Proposal saved to:", OUTPUT);
  console.log("Size:", (buffer.length / 1024).toFixed(1), "KB");
}).catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
