"""
Generate an interactive TradingView-style chart for VAM Step 2 backtest results.

Step 2 = UPRO + TQQQ (7-state machine)

Creates a standalone HTML file with:
- SPY price chart with SMA-50 / SMA-200 overlays and UPRO trade markers
- QQQ price chart with QQQ SMA-50 overlay and TQQQ trade markers
- VIX panel with kill switch threshold line
- RSI panel with overbought/oversold zones
- Equity curve panel
- State timeline (color-coded background — 7 states)
- Toggle buttons per state and signal type
- Hover tooltips with full trade details for both baskets
"""

import json
from pathlib import Path

import pandas as pd

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
DELIVERY_DIR = Path(__file__).resolve().parent.parent / "delivery" / "backtest_results"
DELIVERY_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load Step 2 portfolio values and trade log."""
    portfolio = pd.read_csv(RESULTS_DIR / "step2_databento_portfolio_values.csv")
    trades = pd.read_csv(RESULTS_DIR / "step2_databento_trade_log.csv")
    return portfolio, trades


def build_chart_data(portfolio: pd.DataFrame, trades: pd.DataFrame) -> dict:
    """Convert DataFrames to JSON-serializable chart data."""

    # ── Price series ──
    spy  = [{"time": r["date"], "value": r["spy_close"]}  for _, r in portfolio.iterrows()]
    qqq  = [{"time": r["date"], "value": r["qqq_close"]}  for _, r in portfolio.iterrows()]
    sma50_spy  = [{"time": r["date"], "value": r["spy_sma50"]}  for _, r in portfolio.iterrows()]
    sma200_spy = [{"time": r["date"], "value": r["spy_sma200"]} for _, r in portfolio.iterrows()]
    sma50_qqq  = [{"time": r["date"], "value": r["qqq_sma50"]}  for _, r in portfolio.iterrows()]

    # ── VIX ──
    vix = [{"time": r["date"], "value": r["vix"]} for _, r in portfolio.iterrows()]

    # ── RSI (SPY) ──
    rsi = [{"time": r["date"], "value": r["spy_rsi_14"]} for _, r in portfolio.iterrows()]

    # ── Equity curve ──
    equity = [{"time": r["date"], "value": r["portfolio_value"]} for _, r in portfolio.iterrows()]

    # ── State timeline (7 states) ──
    state_colors = {
        "BULL_100":       "rgba(34, 197, 94,  0.08)",
        "BULL_TRIMMED":   "rgba(245, 158, 11, 0.08)",
        "DEFENSIVE_SPY":  "rgba(234, 179, 8,  0.10)",
        "DEFENSIVE_QQQ":  "rgba(251, 146, 60, 0.10)",
        "DEFENSIVE_BOTH": "rgba(239, 68,  68, 0.08)",
        "SMA_RECOVERY":   "rgba(56,  189, 248, 0.08)",
        "CASH":           "rgba(239, 68,  68, 0.05)",
    }
    states = [
        {
            "time":  r["date"],
            "state": r["state"],
            "color": state_colors.get(r["state"], "rgba(128,128,128,0.08)"),
        }
        for _, r in portfolio.iterrows()
    ]

    # ── Trade markers — split by instrument ──
    upro_markers  = []
    tqqq_markers  = []
    for _, t in trades.iterrows():
        is_buy     = t["action"] == "BUY"
        instrument = t.get("instrument", "UPRO")
        entry = {
            "time":         t["execution_date"],
            "position":     "belowBar" if is_buy else "aboveBar",
            "color":        "#22C55E" if is_buy else "#EF4444",
            "shape":        "arrowUp" if is_buy else "arrowDown",
            "text":         f"{'BUY' if is_buy else 'SELL'} {instrument} → {t.get('state_to','')}",
            "state_from":   t.get("state_from", ""),
            "state_to":     t.get("state_to", ""),
            "reason":       t.get("trigger_reason", ""),
            "exec_price":   float(t.get("exec_price", 0)),
            "trade_value":  float(t.get("trade_value_dollars", 0)),
            "portfolio_after": float(t.get("portfolio_value_at_close", 0)),
            "slippage_type":   t.get("slippage_type", ""),
            "total_cost":      float(t.get("total_cost_dollars", 0)),
        }
        if instrument == "TQQQ":
            tqqq_markers.append(entry)
        else:
            upro_markers.append(entry)

    # ── Daily tooltip data ──
    daily_details = []
    for _, r in portfolio.iterrows():
        daily_details.append({
            "date":          r["date"],
            "state":         r["state"],
            "spy":           r["spy_close"],
            "qqq":           r["qqq_close"],
            "upro_close":    r["upro_close"],
            "tqqq_close":    r["tqqq_close"],
            "vix":           r["vix"],
            "rsi":           r["spy_rsi_14"],
            "sma50_spy":     r["spy_sma50"],
            "sma200_spy":    r["spy_sma200"],
            "sma50_qqq":     r["qqq_sma50"],
            "spy_vs_sma50":  r["spy_vs_sma50"],
            "spy_vs_sma200": r["spy_vs_sma200"],
            "qqq_vs_sma50":  r["qqq_vs_sma50"],
            "kill_active":   r["kill_switch_active"],
            "kill_reason":   r["kill_reason"],
            "spy_def":       r["spy_defensive_trigger"],
            "qqq_def":       r["qqq_defensive_trigger"],
            "rsi_zone":      r["rsi_zone"],
            "portfolio":     r["portfolio_value"],
            "cum_return":    r["cumulative_return_pct"],
            "upro_alloc":    r["upro_allocation_pct"],
            "tqqq_alloc":    r["tqqq_allocation_pct"],
            "cash_alloc":    r["cash_allocation_pct"],
            "pending":       r["pending_signal_for_tomorrow"],
        })

    return {
        "spy":         spy,
        "qqq":         qqq,
        "sma50_spy":   sma50_spy,
        "sma200_spy":  sma200_spy,
        "sma50_qqq":   sma50_qqq,
        "vix":         vix,
        "rsi":         rsi,
        "equity":      equity,
        "states":      states,
        "upro_markers":  upro_markers,
        "tqqq_markers":  tqqq_markers,
        "daily":       daily_details,
    }


def generate_html(data: dict) -> str:
    """Generate the full interactive HTML page."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VAM Split Strategy — Step 2 UPRO + TQQQ</title>
<script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0f; color: #e0e0e0; }}
.header {{ padding: 16px 24px; background: #111118; border-bottom: 1px solid #2a2a3a; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }}
.header h1 {{ font-size: 18px; color: #fff; }}
.header .stats {{ display: flex; gap: 24px; font-size: 13px; flex-wrap: wrap; }}
.header .stat {{ text-align: center; }}
.header .stat .label {{ color: #888; font-size: 11px; }}
.header .stat .value {{ font-size: 16px; font-weight: 600; }}
.header .stat .value.green {{ color: #22C55E; }}
.header .stat .value.red {{ color: #EF4444; }}
.controls {{ padding: 10px 24px; background: #111118; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; border-bottom: 1px solid #2a2a3a; }}
.controls .group-label {{ font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 1px; margin-right: 4px; }}
.btn {{ padding: 6px 14px; border-radius: 6px; border: 1px solid #333; background: #1a1a24; color: #ccc; font-size: 12px; cursor: pointer; transition: all 0.15s; }}
.btn:hover {{ background: #2a2a3a; }}
.btn.active {{ border-color: #4a9eff; color: #4a9eff; background: rgba(74,158,255,0.1); }}
.btn.bull    {{ border-color: #22C55E; color: #22C55E; background: rgba(34,197,94,0.1); }}
.btn.trim    {{ border-color: #F59E0B; color: #F59E0B; background: rgba(245,158,11,0.1); }}
.btn.def-spy {{ border-color: #EAB308; color: #EAB308; background: rgba(234,179,8,0.1); }}
.btn.def-qqq {{ border-color: #FB923C; color: #FB923C; background: rgba(251,146,60,0.1); }}
.btn.def-both {{ border-color: #F97316; color: #F97316; background: rgba(249,115,22,0.1); }}
.btn.recovery {{ border-color: #38BDF8; color: #38BDF8; background: rgba(56,189,248,0.1); }}
.btn.cash    {{ border-color: #EF4444; color: #EF4444; background: rgba(239,68,68,0.1); }}
.sep {{ width: 1px; height: 24px; background: #333; margin: 0 8px; }}
.charts {{ padding: 0 24px 24px; }}
.chart-container {{ margin-top: 12px; border: 1px solid #1e1e2e; border-radius: 8px; overflow: hidden; position: relative; }}
.chart-label {{ position: absolute; top: 8px; left: 12px; z-index: 10; font-size: 11px; color: #666; background: rgba(10,10,15,0.8); padding: 2px 8px; border-radius: 4px; pointer-events: none; }}
.tooltip {{ position: fixed; z-index: 1000; background: #1a1a28; border: 1px solid #333; border-radius: 8px; padding: 12px; font-size: 12px; pointer-events: none; max-width: 380px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }}
.tooltip .tt-header {{ font-weight: 600; color: #fff; margin-bottom: 8px; font-size: 13px; }}
.tooltip .tt-row {{ display: flex; justify-content: space-between; gap: 16px; padding: 2px 0; }}
.tooltip .tt-label {{ color: #888; }}
.tooltip .tt-value {{ color: #e0e0e0; font-weight: 500; }}
.tooltip .tt-value.green {{ color: #22C55E; }}
.tooltip .tt-value.red {{ color: #EF4444; }}
.tooltip .tt-value.warn {{ color: #F59E0B; }}
.tooltip .tt-divider {{ border-top: 1px solid #2a2a3a; margin: 6px 0; }}
.state-badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
.state-badge.BULL_100       {{ background: rgba(34,197,94,0.2);  color: #22C55E; }}
.state-badge.BULL_TRIMMED   {{ background: rgba(245,158,11,0.2); color: #F59E0B; }}
.state-badge.DEFENSIVE_SPY  {{ background: rgba(234,179,8,0.2);  color: #EAB308; }}
.state-badge.DEFENSIVE_QQQ  {{ background: rgba(251,146,60,0.2); color: #FB923C; }}
.state-badge.DEFENSIVE_BOTH {{ background: rgba(249,115,22,0.2); color: #F97316; }}
.state-badge.SMA_RECOVERY   {{ background: rgba(56,189,248,0.2); color: #38BDF8; }}
.state-badge.CASH           {{ background: rgba(239,68,68,0.2);  color: #EF4444; }}
</style>
</head>
<body>
<div class="header">
  <h1>VAM Split Strategy — Step 2 &nbsp;<span style="color:#888;font-size:14px">UPRO + TQQQ</span></h1>
  <div class="stats">
    <div class="stat"><div class="label">Final Value</div><div class="value green" id="stat-final">—</div></div>
    <div class="stat"><div class="label">CAGR</div><div class="value green" id="stat-cagr">—</div></div>
    <div class="stat"><div class="label">Sharpe</div><div class="value" id="stat-sharpe">—</div></div>
    <div class="stat"><div class="label">Max DD</div><div class="value red" id="stat-dd">—</div></div>
    <div class="stat"><div class="label">Trades</div><div class="value" id="stat-trades">—</div></div>
    <div class="stat"><div class="label">Alpha vs SPY</div><div class="value green" id="stat-alpha">—</div></div>
  </div>
</div>

<div class="controls">
  <span class="group-label">States:</span>
  <button class="btn bull active"     data-state="BULL_100"       onclick="toggleState(this)">BULL 100%</button>
  <button class="btn trim active"     data-state="BULL_TRIMMED"   onclick="toggleState(this)">TRIMMED 75%</button>
  <button class="btn def-spy active"  data-state="DEFENSIVE_SPY"  onclick="toggleState(this)">DEF SPY</button>
  <button class="btn def-qqq active"  data-state="DEFENSIVE_QQQ"  onclick="toggleState(this)">DEF QQQ</button>
  <button class="btn def-both active" data-state="DEFENSIVE_BOTH" onclick="toggleState(this)">DEF BOTH</button>
  <button class="btn recovery active" data-state="SMA_RECOVERY"   onclick="toggleState(this)">SMA RECOVERY</button>
  <button class="btn cash active"     data-state="CASH"           onclick="toggleState(this)">CASH 0%</button>
  <div class="sep"></div>
  <span class="group-label">Signals:</span>
  <button class="btn active" id="btn-sma50spy"  onclick="toggleLine('sma50spy',this)">SPY SMA-50</button>
  <button class="btn active" id="btn-sma200spy" onclick="toggleLine('sma200spy',this)">SPY SMA-200</button>
  <button class="btn active" id="btn-sma50qqq"  onclick="toggleLine('sma50qqq',this)">QQQ SMA-50</button>
  <button class="btn active" id="btn-vix"  onclick="togglePanel('vix-container',this)">VIX Panel</button>
  <button class="btn active" id="btn-rsi"  onclick="togglePanel('rsi-container',this)">RSI Panel</button>
  <button class="btn active" id="btn-qqq"  onclick="togglePanel('qqq-container',this)">QQQ Panel</button>
  <div class="sep"></div>
  <span class="group-label">Trades:</span>
  <button class="btn active" id="btn-buys"  onclick="toggleMarkers('BUY',this)">Buys</button>
  <button class="btn active" id="btn-sells" onclick="toggleMarkers('SELL',this)">Sells</button>
  <div class="sep"></div>
  <span class="group-label">View:</span>
  <button class="btn active" id="btn-equity" onclick="togglePanel('equity-container',this)">Equity Curve</button>
</div>

<div class="charts">
  <div class="chart-container">
    <div class="chart-label">SPY + SMAs + UPRO Trade Markers</div>
    <div id="chart-spy" style="height:380px"></div>
  </div>
  <div class="chart-container" id="qqq-container">
    <div class="chart-label">QQQ + QQQ SMA-50 + TQQQ Trade Markers</div>
    <div id="chart-qqq" style="height:220px"></div>
  </div>
  <div class="chart-container" id="vix-container">
    <div class="chart-label">VIX (Kill Switch at 30)</div>
    <div id="chart-vix" style="height:150px"></div>
  </div>
  <div class="chart-container" id="rsi-container">
    <div class="chart-label">RSI-14 (Sell &gt;75, Rebuy &lt;60)</div>
    <div id="chart-rsi" style="height:150px"></div>
  </div>
  <div class="chart-container" id="equity-container">
    <div class="chart-label">Portfolio Value ($)</div>
    <div id="chart-equity" style="height:200px"></div>
  </div>
</div>

<div class="tooltip" id="tooltip" style="display:none"></div>

<script>
const DATA = {json.dumps(data)};

// ── Header stats ──
const lastDay  = DATA.daily[DATA.daily.length - 1];
const firstDay = DATA.daily[0];
document.getElementById('stat-final').textContent = '$' + lastDay.portfolio.toLocaleString(undefined, {{maximumFractionDigits: 0}});
const years = (new Date(lastDay.date) - new Date(firstDay.date)) / (365.25 * 24 * 3600 * 1000);
const cagr  = (Math.pow(lastDay.portfolio / 100000, 1 / years) - 1) * 100;
document.getElementById('stat-cagr').textContent = (cagr >= 0 ? '+' : '') + cagr.toFixed(1) + '%';
document.getElementById('stat-sharpe').textContent = '—';
document.getElementById('stat-dd').textContent = '—';
const totalTrades = DATA.upro_markers.length + DATA.tqqq_markers.length;
document.getElementById('stat-trades').textContent = totalTrades + ' (' + DATA.upro_markers.length + ' UPRO + ' + DATA.tqqq_markers.length + ' TQQQ)';
const spyCagr = (Math.pow(lastDay.spy / firstDay.spy, 1 / years) - 1) * 100;
const alpha   = cagr - spyCagr;
document.getElementById('stat-alpha').textContent = (alpha >= 0 ? '+' : '') + alpha.toFixed(1) + '%';

// ── State visibility ──
let visibleStates = new Set(['BULL_100','BULL_TRIMMED','DEFENSIVE_SPY','DEFENSIVE_QQQ','DEFENSIVE_BOTH','SMA_RECOVERY','CASH']);
let showBuys = true, showSells = true;

// ── Shared chart options ──
const chartOpts = {{
  layout:     {{ background: {{ color: '#0a0a0f' }}, textColor: '#888' }},
  grid:       {{ vertLines: {{ color: '#1a1a2a' }}, horzLines: {{ color: '#1a1a2a' }} }},
  crosshair:  {{ mode: 0 }},
  timeScale:  {{ timeVisible: false, borderColor: '#2a2a3a' }},
  rightPriceScale: {{ borderColor: '#2a2a3a' }},
}};

// ── SPY chart ──
const spyEl    = document.getElementById('chart-spy');
const spyChart = LightweightCharts.createChart(spyEl, chartOpts);

const spySeries   = spyChart.addLineSeries({{ color: '#4a9eff', lineWidth: 2, title: 'SPY' }});
spySeries.setData(DATA.spy);

const sma50SpySeries  = spyChart.addLineSeries({{ color: '#F59E0B', lineWidth: 1, lineStyle: 2, title: 'SMA-50' }});
sma50SpySeries.setData(DATA.sma50_spy);

const sma200SpySeries = spyChart.addLineSeries({{ color: '#EF4444', lineWidth: 1, lineStyle: 2, title: 'SMA-200' }});
sma200SpySeries.setData(DATA.sma200_spy);

function updateUproMarkers() {{
  const filtered = DATA.upro_markers.filter(m => {{
    const isBuy = m.text.includes('BUY');
    if (isBuy  && !showBuys)  return false;
    if (!isBuy && !showSells) return false;
    return visibleStates.has(m.state_from) || visibleStates.has(m.state_to);
  }});
  spySeries.setMarkers(filtered.map(m => ({{
    time: m.time, position: m.position, color: m.color, shape: m.shape, text: m.text,
  }})));
}}
updateUproMarkers();

// ── QQQ chart ──
const qqqEl    = document.getElementById('chart-qqq');
const qqqChart = LightweightCharts.createChart(qqqEl, chartOpts);

const qqqSeries      = qqqChart.addLineSeries({{ color: '#A78BFA', lineWidth: 2, title: 'QQQ' }});
qqqSeries.setData(DATA.qqq);

const sma50QqqSeries = qqqChart.addLineSeries({{ color: '#F59E0B', lineWidth: 1, lineStyle: 2, title: 'QQQ SMA-50' }});
sma50QqqSeries.setData(DATA.sma50_qqq);

function updateTqqqMarkers() {{
  const filtered = DATA.tqqq_markers.filter(m => {{
    const isBuy = m.text.includes('BUY');
    if (isBuy  && !showBuys)  return false;
    if (!isBuy && !showSells) return false;
    return visibleStates.has(m.state_from) || visibleStates.has(m.state_to);
  }});
  qqqSeries.setMarkers(filtered.map(m => ({{
    time: m.time, position: m.position, color: m.color, shape: m.shape, text: m.text,
  }})));
}}
updateTqqqMarkers();

// ── VIX chart ──
const vixEl    = document.getElementById('chart-vix');
const vixChart = LightweightCharts.createChart(vixEl, chartOpts);
const vixSeries = vixChart.addLineSeries({{ color: '#A855F7', lineWidth: 1.5, title: 'VIX' }});
vixSeries.setData(DATA.vix);
const vixThreshold = vixChart.addLineSeries({{ color: '#EF4444', lineWidth: 1, lineStyle: 2, title: 'Kill=30' }});
vixThreshold.setData(DATA.vix.map(v => ({{ time: v.time, value: 30 }})));

// ── RSI chart ──
const rsiEl    = document.getElementById('chart-rsi');
const rsiChart = LightweightCharts.createChart(rsiEl, chartOpts);
const rsiSeries = rsiChart.addLineSeries({{ color: '#06B6D4', lineWidth: 1.5, title: 'RSI-14' }});
rsiSeries.setData(DATA.rsi);
const rsi75 = rsiChart.addLineSeries({{ color: '#EF4444', lineWidth: 1, lineStyle: 2 }});
rsi75.setData(DATA.rsi.map(r => ({{ time: r.time, value: 75 }})));
const rsi60 = rsiChart.addLineSeries({{ color: '#22C55E', lineWidth: 1, lineStyle: 2 }});
rsi60.setData(DATA.rsi.map(r => ({{ time: r.time, value: 60 }})));

// ── Equity chart ──
const eqEl    = document.getElementById('chart-equity');
const eqChart = LightweightCharts.createChart(eqEl, chartOpts);
const eqSeries = eqChart.addAreaSeries({{
  topColor:    'rgba(34,197,94,0.3)',
  bottomColor: 'rgba(34,197,94,0.0)',
  lineColor:   '#22C55E',
  lineWidth:   2,
  title:       'Portfolio',
}});
eqSeries.setData(DATA.equity);

// ── Sync all time scales ──
const allCharts = [spyChart, qqqChart, vixChart, rsiChart, eqChart];
allCharts.forEach((c, i) => {{
  c.timeScale().subscribeVisibleTimeRangeChange((range) => {{
    if (!range) return;
    allCharts.forEach((other, j) => {{
      if (i !== j) other.timeScale().setVisibleRange(range);
    }});
  }});
}});

// ── Toggle functions ──
function toggleState(btn) {{
  const state = btn.dataset.state;
  btn.classList.toggle('active');
  if (visibleStates.has(state)) visibleStates.delete(state);
  else visibleStates.add(state);
  updateUproMarkers();
  updateTqqqMarkers();
}}

function toggleLine(which, btn) {{
  btn.classList.toggle('active');
  const on = btn.classList.contains('active');
  if (which === 'sma50spy')  sma50SpySeries.applyOptions({{ visible: on }});
  if (which === 'sma200spy') sma200SpySeries.applyOptions({{ visible: on }});
  if (which === 'sma50qqq')  sma50QqqSeries.applyOptions({{ visible: on }});
}}

function togglePanel(id, btn) {{
  btn.classList.toggle('active');
  document.getElementById(id).style.display = btn.classList.contains('active') ? 'block' : 'none';
}}

function toggleMarkers(type, btn) {{
  btn.classList.toggle('active');
  if (type === 'BUY')  showBuys  = btn.classList.contains('active');
  if (type === 'SELL') showSells = btn.classList.contains('active');
  updateUproMarkers();
  updateTqqqMarkers();
}}

// ── Tooltip on SPY crosshair ──
const tooltip = document.getElementById('tooltip');

function buildTooltip(day, trade) {{
  const allocStr = day.upro_alloc.toFixed(0) + '% UPRO + ' + day.tqqq_alloc.toFixed(0) + '% TQQQ + ' + day.cash_alloc.toFixed(0) + '% Cash';
  let html = `<div class="tt-header">${{day.date}} <span class="state-badge ${{day.state}}">${{day.state.replace('_',' ')}}</span></div>`;

  html += `<div class="tt-row"><span class="tt-label">SPY</span><span class="tt-value">${{day.spy.toFixed(2)}} <span style="color:${{day.spy_vs_sma50==='ABOVE'?'#22C55E':'#EF4444'}};font-size:10px">vs 50SMA: ${{day.spy_vs_sma50}}</span></span></div>`;
  html += `<div class="tt-row"><span class="tt-label">QQQ</span><span class="tt-value">${{day.qqq.toFixed(2)}} <span style="color:${{day.qqq_vs_sma50==='ABOVE'?'#22C55E':'#EF4444'}};font-size:10px">vs 50SMA: ${{day.qqq_vs_sma50}}</span></span></div>`;
  html += `<div class="tt-row"><span class="tt-label">SPY SMA-200</span><span class="tt-value">${{day.sma200_spy.toFixed(1)}} (${{day.spy_vs_sma200}})</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">VIX</span><span class="tt-value ${{day.vix > 30 ? 'red' : ''}}">${{day.vix.toFixed(1)}}${{day.kill_active === 'YES' ? ' ⚠ KILL' : ''}}</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">RSI</span><span class="tt-value">${{day.rsi.toFixed(1)}} (${{day.rsi_zone}})</span></div>`;

  if (day.spy_def && day.spy_def !== 'NO' && !day.spy_def.startsWith('NO')) {{
    html += `<div class="tt-row"><span class="tt-label">SPY Def Signal</span><span class="tt-value warn">${{day.spy_def}}</span></div>`;
  }}
  if (day.qqq_def && day.qqq_def !== 'NO' && !day.qqq_def.startsWith('NO')) {{
    html += `<div class="tt-row"><span class="tt-label">QQQ Def Signal</span><span class="tt-value warn">${{day.qqq_def}}</span></div>`;
  }}

  html += `<div class="tt-divider"></div>`;
  html += `<div class="tt-row"><span class="tt-label">Portfolio</span><span class="tt-value">$${{day.portfolio.toLocaleString(undefined,{{maximumFractionDigits:0}})}}</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">Return</span><span class="tt-value ${{day.cum_return>=0?'green':'red'}}">${{day.cum_return>=0?'+':''}}${{day.cum_return.toFixed(1)}}%</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">Allocation</span><span class="tt-value" style="font-size:11px">${{allocStr}}</span></div>`;

  if (day.pending && day.pending !== 'NONE') {{
    html += `<div class="tt-divider"></div>`;
    html += `<div class="tt-row"><span class="tt-label">⏳ Pending</span><span class="tt-value" style="color:#F59E0B;font-size:11px">${{day.pending}}</span></div>`;
  }}

  if (trade) {{
    html += `<div class="tt-divider"></div>`;
    html += `<div class="tt-row"><span class="tt-label">🔄 Trade</span><span class="tt-value" style="color:#4a9eff">${{trade.text}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Reason</span><span class="tt-value" style="font-size:10px">${{trade.reason}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Exec $</span><span class="tt-value">$${{trade.exec_price.toFixed(2)}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Trade $</span><span class="tt-value">$${{trade.trade_value.toLocaleString(undefined,{{maximumFractionDigits:0}})}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Cost</span><span class="tt-value">$${{trade.total_cost.toFixed(2)}} (${{trade.slippage_type}})</span></div>`;
  }}

  return html;
}}

spyChart.subscribeCrosshairMove((param) => {{
  if (!param.time || !param.point) {{ tooltip.style.display = 'none'; return; }}
  const day = DATA.daily.find(d => d.date === param.time);
  if (!day) {{ tooltip.style.display = 'none'; return; }}
  const trade = DATA.upro_markers.find(m => m.time === param.time)
             || DATA.tqqq_markers.find(m => m.time === param.time);

  tooltip.innerHTML = buildTooltip(day, trade);
  tooltip.style.display = 'block';

  const x = param.point.x + spyEl.getBoundingClientRect().left;
  const y = param.point.y + spyEl.getBoundingClientRect().top;
  tooltip.style.left = (x + 20) + 'px';
  tooltip.style.top  = (y - 20) + 'px';

  const rect = tooltip.getBoundingClientRect();
  if (rect.right  > window.innerWidth)  tooltip.style.left = (x - rect.width - 20) + 'px';
  if (rect.bottom > window.innerHeight) tooltip.style.top  = (y - rect.height) + 'px';
}});

spyEl.addEventListener('mouseleave', () => {{ tooltip.style.display = 'none'; }});

// Fit all charts
allCharts.forEach(c => c.timeScale().fitContent());
</script>
</body>
</html>"""


def main() -> None:
    """Generate Step 2 interactive chart HTML."""
    print("[Chart Step 2] Loading data...")
    portfolio, trades = load_data()
    upro_trades = trades[trades["instrument"] == "UPRO"]
    tqqq_trades = trades[trades["instrument"] == "TQQQ"]
    print(f"  {len(portfolio)} days, {len(upro_trades)} UPRO trades, {len(tqqq_trades)} TQQQ trades")

    print("[Chart Step 2] Building chart data...")
    data = build_chart_data(portfolio, trades)

    print("[Chart Step 2] Generating HTML...")
    html = generate_html(data)

    output_path = DELIVERY_DIR / "vam_step2_interactive_chart.html"
    with open(output_path, "w") as f:
        f.write(html)

    print(f"  Saved: {output_path}")
    print(f"  Open in browser to view")


if __name__ == "__main__":
    main()
