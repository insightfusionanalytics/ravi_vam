"""
Generate an interactive TradingView-style chart for VAM backtest results.

Creates a standalone HTML file with:
- UPRO price chart with buy/sell markers colored by state
- SPY with SMA-50 and SMA-200 overlays
- VIX panel with kill switch threshold line
- RSI panel with overbought/oversold zones
- Equity curve panel
- State timeline (color-coded background)
- Toggle buttons per state and signal type
- Hover tooltips with full trade details
"""

import json
from pathlib import Path

import pandas as pd

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
OUTPUT_DIR = RESULTS_DIR / "step1_step2_databento_final"


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load portfolio values and trade log."""
    portfolio = pd.read_csv(RESULTS_DIR / "step1_databento_portfolio_values.csv")
    trades = pd.read_csv(RESULTS_DIR / "step1_databento_trade_log.csv")
    return portfolio, trades


def build_chart_data(portfolio: pd.DataFrame, trades: pd.DataFrame) -> dict:
    """Convert DataFrames to JSON-serializable chart data."""
    # UPRO price series
    upro = [{"time": r["date"], "value": r["upro_close"]} for _, r in portfolio.iterrows()]

    # SPY price with SMAs
    spy = [{"time": r["date"], "value": r["spy_close"]} for _, r in portfolio.iterrows()]
    sma50 = [{"time": r["date"], "value": r["spy_sma50"]} for _, r in portfolio.iterrows()]
    sma200 = [{"time": r["date"], "value": r["spy_sma200"]} for _, r in portfolio.iterrows()]

    # VIX
    vix = [{"time": r["date"], "value": r["vix"]} for _, r in portfolio.iterrows()]

    # RSI
    rsi = [{"time": r["date"], "value": r["spy_rsi_14"]} for _, r in portfolio.iterrows()]

    # Equity curve
    equity = [{"time": r["date"], "value": r["portfolio_value"]} for _, r in portfolio.iterrows()]

    # State timeline
    state_colors = {
        "BULL_100": "rgba(34, 139, 34, 0.15)",
        "BULL_TRIMMED": "rgba(255, 165, 0, 0.15)",
        "DEFENSIVE": "rgba(255, 215, 0, 0.20)",
        "CASH": "rgba(220, 20, 60, 0.10)",
    }
    states = [{"time": r["date"], "state": r["state"], "color": state_colors.get(r["state"], "rgba(128,128,128,0.1)")}
              for _, r in portfolio.iterrows()]

    # Trade markers
    markers = []
    for _, t in trades.iterrows():
        is_buy = t["action"] == "BUY"
        state_to = t.get("state_to", "")
        markers.append({
            "time": t["execution_date"],
            "position": "belowBar" if is_buy else "aboveBar",
            "color": "#22C55E" if is_buy else "#EF4444",
            "shape": "arrowUp" if is_buy else "arrowDown",
            "text": f"{'BUY' if is_buy else 'SELL'} → {state_to}",
            "state_from": t.get("state_from", ""),
            "state_to": state_to,
            "reason": t.get("trigger_reason", ""),
            "exec_price": float(t.get("exec_price_upro_open", 0)),
            "trade_value": float(t.get("trade_value_dollars", 0)),
            "portfolio_after": float(t.get("portfolio_value_at_close", 0)),
            "slippage_type": t.get("slippage_type", ""),
            "total_cost": float(t.get("total_cost_dollars", 0)),
        })

    # Daily details for tooltip
    daily_details = []
    for _, r in portfolio.iterrows():
        daily_details.append({
            "date": r["date"],
            "state": r["state"],
            "spy": r["spy_close"],
            "upro_close": r["upro_close"],
            "vix": r["vix"],
            "rsi": r["spy_rsi_14"],
            "sma50": r["spy_sma50"],
            "sma200": r["spy_sma200"],
            "kill_active": r["kill_switch_active"],
            "kill_reason": r["kill_reason"],
            "def_trigger": r["defensive_trigger"],
            "rsi_zone": r["rsi_zone"],
            "portfolio": r["portfolio_value"],
            "cum_return": r["cumulative_return_pct"],
            "upro_alloc": r["upro_allocation_pct"],
            "pending": r["pending_signal_for_tomorrow"],
        })

    return {
        "upro": upro,
        "spy": spy,
        "sma50": sma50,
        "sma200": sma200,
        "vix": vix,
        "rsi": rsi,
        "equity": equity,
        "states": states,
        "markers": markers,
        "daily": daily_details,
    }


def generate_html(data: dict) -> str:
    """Generate the full interactive HTML page."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VAM Split Strategy — Interactive Backtest</title>
<script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0f; color: #e0e0e0; }}
.header {{ padding: 16px 24px; background: #111118; border-bottom: 1px solid #2a2a3a; display: flex; justify-content: space-between; align-items: center; }}
.header h1 {{ font-size: 18px; color: #fff; }}
.header .stats {{ display: flex; gap: 24px; font-size: 13px; }}
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
.btn.bull {{ border-color: #22C55E; color: #22C55E; background: rgba(34,197,94,0.1); }}
.btn.trim {{ border-color: #F59E0B; color: #F59E0B; background: rgba(245,158,11,0.1); }}
.btn.def {{ border-color: #EAB308; color: #EAB308; background: rgba(234,179,8,0.1); }}
.btn.cash {{ border-color: #EF4444; color: #EF4444; background: rgba(239,68,68,0.1); }}
.sep {{ width: 1px; height: 24px; background: #333; margin: 0 8px; }}
.charts {{ padding: 0 24px 24px; }}
.chart-container {{ margin-top: 12px; border: 1px solid #1e1e2e; border-radius: 8px; overflow: hidden; position: relative; }}
.chart-label {{ position: absolute; top: 8px; left: 12px; z-index: 10; font-size: 11px; color: #666; background: rgba(10,10,15,0.8); padding: 2px 8px; border-radius: 4px; }}
.tooltip {{ position: fixed; z-index: 1000; background: #1a1a28; border: 1px solid #333; border-radius: 8px; padding: 12px; font-size: 12px; pointer-events: none; max-width: 360px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }}
.tooltip .tt-header {{ font-weight: 600; color: #fff; margin-bottom: 8px; font-size: 13px; }}
.tooltip .tt-row {{ display: flex; justify-content: space-between; gap: 16px; padding: 2px 0; }}
.tooltip .tt-label {{ color: #888; }}
.tooltip .tt-value {{ color: #e0e0e0; font-weight: 500; }}
.tooltip .tt-value.green {{ color: #22C55E; }}
.tooltip .tt-value.red {{ color: #EF4444; }}
.tooltip .tt-divider {{ border-top: 1px solid #2a2a3a; margin: 6px 0; }}
.state-badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
.state-badge.BULL_100 {{ background: rgba(34,197,94,0.2); color: #22C55E; }}
.state-badge.BULL_TRIMMED {{ background: rgba(245,158,11,0.2); color: #F59E0B; }}
.state-badge.DEFENSIVE {{ background: rgba(234,179,8,0.2); color: #EAB308; }}
.state-badge.CASH {{ background: rgba(239,68,68,0.2); color: #EF4444; }}
</style>
</head>
<body>
<div class="header">
  <h1>VAM Split Strategy — Step 1 UPRO Only</h1>
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
  <button class="btn bull active" data-state="BULL_100" onclick="toggleState(this)">BULL 100%</button>
  <button class="btn trim active" data-state="BULL_TRIMMED" onclick="toggleState(this)">TRIMMED 75%</button>
  <button class="btn def active" data-state="DEFENSIVE" onclick="toggleState(this)">DEFENSIVE 50%</button>
  <button class="btn cash active" data-state="CASH" onclick="toggleState(this)">CASH 0%</button>
  <div class="sep"></div>
  <span class="group-label">Signals:</span>
  <button class="btn active" id="btn-sma50" onclick="toggleLine('sma50',this)">SMA-50</button>
  <button class="btn active" id="btn-sma200" onclick="toggleLine('sma200',this)">SMA-200</button>
  <button class="btn active" id="btn-vix" onclick="toggleVix(this)">VIX Panel</button>
  <button class="btn active" id="btn-rsi" onclick="toggleRsi(this)">RSI Panel</button>
  <div class="sep"></div>
  <span class="group-label">Trades:</span>
  <button class="btn active" id="btn-buys" onclick="toggleMarkers('BUY',this)">Buys</button>
  <button class="btn active" id="btn-sells" onclick="toggleMarkers('SELL',this)">Sells</button>
  <div class="sep"></div>
  <span class="group-label">View:</span>
  <button class="btn active" id="btn-equity" onclick="toggleEquity(this)">Equity Curve</button>
</div>
<div class="charts">
  <div class="chart-container"><div class="chart-label">SPY + Signals + Trade Markers</div><div id="chart-main" style="height:400px"></div></div>
  <div class="chart-container" id="vix-container"><div class="chart-label">VIX (Kill Switch at 30)</div><div id="chart-vix" style="height:150px"></div></div>
  <div class="chart-container" id="rsi-container"><div class="chart-label">RSI-14 (Sell >75, Rebuy <60)</div><div id="chart-rsi" style="height:150px"></div></div>
  <div class="chart-container" id="equity-container"><div class="chart-label">Portfolio Value ($)</div><div id="chart-equity" style="height:200px"></div></div>
</div>
<div class="tooltip" id="tooltip" style="display:none"></div>

<script>
const DATA = {json.dumps(data)};

// Stats
const lastDay = DATA.daily[DATA.daily.length - 1];
document.getElementById('stat-final').textContent = '$' + lastDay.portfolio.toLocaleString(undefined, {{maximumFractionDigits: 0}});
document.getElementById('stat-cagr').textContent = '+21.4%';
document.getElementById('stat-sharpe').textContent = '0.676';
document.getElementById('stat-dd').textContent = '-38.3%';
document.getElementById('stat-trades').textContent = DATA.markers.length;
document.getElementById('stat-alpha').textContent = '+7.4%';

// State colors for background
const STATE_COLORS = {{
  'BULL_100': 'rgba(34,197,94,0.08)',
  'BULL_TRIMMED': 'rgba(245,158,11,0.08)',
  'DEFENSIVE': 'rgba(234,179,8,0.10)',
  'CASH': 'rgba(239,68,68,0.06)',
}};

// Track visibility
let visibleStates = new Set(['BULL_100','BULL_TRIMMED','DEFENSIVE','CASH']);
let showBuys = true, showSells = true;
let allMarkers = DATA.markers;

// ── Main Chart (SPY + SMAs + Markers) ──
const mainEl = document.getElementById('chart-main');
const mainChart = LightweightCharts.createChart(mainEl, {{
  layout: {{ background: {{ color: '#0a0a0f' }}, textColor: '#888' }},
  grid: {{ vertLines: {{ color: '#1a1a2a' }}, horzLines: {{ color: '#1a1a2a' }} }},
  crosshair: {{ mode: 0 }},
  timeScale: {{ timeVisible: false, borderColor: '#2a2a3a' }},
  rightPriceScale: {{ borderColor: '#2a2a3a' }},
}});

const spySeries = mainChart.addLineSeries({{ color: '#4a9eff', lineWidth: 2, title: 'SPY' }});
spySeries.setData(DATA.spy);

const sma50Series = mainChart.addLineSeries({{ color: '#F59E0B', lineWidth: 1, lineStyle: 2, title: 'SMA-50' }});
sma50Series.setData(DATA.sma50);

const sma200Series = mainChart.addLineSeries({{ color: '#EF4444', lineWidth: 1, lineStyle: 2, title: 'SMA-200' }});
sma200Series.setData(DATA.sma200);

// Trade markers on SPY
function updateMarkers() {{
  const filtered = allMarkers.filter(m => {{
    const isBuy = m.text.startsWith('BUY');
    if (isBuy && !showBuys) return false;
    if (!isBuy && !showSells) return false;
    const targetState = isBuy ? m.state_to : m.state_from;
    // Show if either from or to state is visible
    return visibleStates.has(m.state_from) || visibleStates.has(m.state_to);
  }});
  spySeries.setMarkers(filtered.map(m => ({{
    time: m.time,
    position: m.position,
    color: m.color,
    shape: m.shape,
    text: m.text,
  }})));
}}
updateMarkers();

// ── VIX Chart ──
const vixEl = document.getElementById('chart-vix');
const vixChart = LightweightCharts.createChart(vixEl, {{
  layout: {{ background: {{ color: '#0a0a0f' }}, textColor: '#888' }},
  grid: {{ vertLines: {{ color: '#1a1a2a' }}, horzLines: {{ color: '#1a1a2a' }} }},
  crosshair: {{ mode: 0 }},
  timeScale: {{ timeVisible: false, borderColor: '#2a2a3a' }},
  rightPriceScale: {{ borderColor: '#2a2a3a' }},
}});
const vixSeries = vixChart.addLineSeries({{ color: '#A855F7', lineWidth: 1.5, title: 'VIX' }});
vixSeries.setData(DATA.vix);
// Kill switch threshold line
const vixThreshold = vixChart.addLineSeries({{ color: '#EF4444', lineWidth: 1, lineStyle: 2, title: 'Kill=30' }});
vixThreshold.setData(DATA.vix.map(v => ({{ time: v.time, value: 30 }})));

// ── RSI Chart ──
const rsiEl = document.getElementById('chart-rsi');
const rsiChart = LightweightCharts.createChart(rsiEl, {{
  layout: {{ background: {{ color: '#0a0a0f' }}, textColor: '#888' }},
  grid: {{ vertLines: {{ color: '#1a1a2a' }}, horzLines: {{ color: '#1a1a2a' }} }},
  crosshair: {{ mode: 0 }},
  timeScale: {{ timeVisible: false, borderColor: '#2a2a3a' }},
  rightPriceScale: {{ borderColor: '#2a2a3a' }},
}});
const rsiSeries = rsiChart.addLineSeries({{ color: '#06B6D4', lineWidth: 1.5, title: 'RSI-14' }});
rsiSeries.setData(DATA.rsi);
const rsi75 = rsiChart.addLineSeries({{ color: '#EF4444', lineWidth: 1, lineStyle: 2 }});
rsi75.setData(DATA.rsi.map(r => ({{ time: r.time, value: 75 }})));
const rsi60 = rsiChart.addLineSeries({{ color: '#22C55E', lineWidth: 1, lineStyle: 2 }});
rsi60.setData(DATA.rsi.map(r => ({{ time: r.time, value: 60 }})));

// ── Equity Chart ──
const eqEl = document.getElementById('chart-equity');
const eqChart = LightweightCharts.createChart(eqEl, {{
  layout: {{ background: {{ color: '#0a0a0f' }}, textColor: '#888' }},
  grid: {{ vertLines: {{ color: '#1a1a2a' }}, horzLines: {{ color: '#1a1a2a' }} }},
  crosshair: {{ mode: 0 }},
  timeScale: {{ timeVisible: false, borderColor: '#2a2a3a' }},
  rightPriceScale: {{ borderColor: '#2a2a3a' }},
}});
const eqSeries = eqChart.addAreaSeries({{
  topColor: 'rgba(34,197,94,0.3)',
  bottomColor: 'rgba(34,197,94,0.0)',
  lineColor: '#22C55E',
  lineWidth: 2,
  title: 'Portfolio',
}});
eqSeries.setData(DATA.equity);

// Sync time scales
const charts = [mainChart, vixChart, rsiChart, eqChart];
charts.forEach((c, i) => {{
  c.timeScale().subscribeVisibleTimeRangeChange((range) => {{
    if (!range) return;
    charts.forEach((other, j) => {{
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
  updateMarkers();
}}

function toggleLine(which, btn) {{
  btn.classList.toggle('active');
  if (which === 'sma50') sma50Series.applyOptions({{ visible: btn.classList.contains('active') }});
  if (which === 'sma200') sma200Series.applyOptions({{ visible: btn.classList.contains('active') }});
}}

function toggleVix(btn) {{
  btn.classList.toggle('active');
  document.getElementById('vix-container').style.display = btn.classList.contains('active') ? 'block' : 'none';
}}

function toggleRsi(btn) {{
  btn.classList.toggle('active');
  document.getElementById('rsi-container').style.display = btn.classList.contains('active') ? 'block' : 'none';
}}

function toggleMarkers(type, btn) {{
  btn.classList.toggle('active');
  if (type === 'BUY') showBuys = btn.classList.contains('active');
  if (type === 'SELL') showSells = btn.classList.contains('active');
  updateMarkers();
}}

function toggleEquity(btn) {{
  btn.classList.toggle('active');
  document.getElementById('equity-container').style.display = btn.classList.contains('active') ? 'block' : 'none';
}}

// ── Tooltip on crosshair ──
const tooltip = document.getElementById('tooltip');
mainChart.subscribeCrosshairMove((param) => {{
  if (!param.time || !param.point) {{
    tooltip.style.display = 'none';
    return;
  }}
  const day = DATA.daily.find(d => d.date === param.time);
  if (!day) {{ tooltip.style.display = 'none'; return; }}

  // Find if there's a trade on this day
  const trade = allMarkers.find(m => m.time === param.time);

  let html = `<div class="tt-header">${{day.date}} <span class="state-badge ${{day.state}}">${{day.state}}</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">SPY</span><span class="tt-value">${{day.spy.toFixed(2)}}</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">UPRO</span><span class="tt-value">${{day.upro_close.toFixed(2)}}</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">VIX</span><span class="tt-value ${{day.vix > 30 ? 'red' : ''}}">${{day.vix.toFixed(1)}} ${{day.kill_active === 'YES' ? '⚠ KILL' : ''}}</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">RSI</span><span class="tt-value">${{day.rsi.toFixed(1)}} (${{day.rsi_zone}})</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">SMA-50</span><span class="tt-value">${{day.sma50.toFixed(1)}} (${{day.spy > day.sma50 ? 'ABOVE' : 'BELOW'}})</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">SMA-200</span><span class="tt-value">${{day.sma200.toFixed(1)}} (${{day.spy > day.sma200 ? 'ABOVE' : 'BELOW'}})</span></div>`;
  html += `<div class="tt-divider"></div>`;
  html += `<div class="tt-row"><span class="tt-label">Portfolio</span><span class="tt-value">$${{day.portfolio.toLocaleString(undefined, {{maximumFractionDigits:0}})}}</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">Return</span><span class="tt-value ${{day.cum_return >= 0 ? 'green' : 'red'}}">${{day.cum_return >= 0 ? '+' : ''}}${{day.cum_return.toFixed(1)}}%</span></div>`;
  html += `<div class="tt-row"><span class="tt-label">Allocation</span><span class="tt-value">${{day.upro_alloc.toFixed(0)}}% UPRO / ${{(100-day.upro_alloc).toFixed(0)}}% Cash</span></div>`;

  if (day.pending && day.pending !== 'NONE') {{
    html += `<div class="tt-divider"></div>`;
    html += `<div class="tt-row"><span class="tt-label">⏳ Pending</span><span class="tt-value" style="color:#F59E0B;font-size:11px">${{day.pending}}</span></div>`;
  }}

  if (trade) {{
    html += `<div class="tt-divider"></div>`;
    html += `<div class="tt-row"><span class="tt-label">🔄 Trade</span><span class="tt-value" style="color:#4a9eff">${{trade.text}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Reason</span><span class="tt-value" style="font-size:10px">${{trade.reason}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Exec Price</span><span class="tt-value">$${{trade.exec_price.toFixed(2)}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Trade $</span><span class="tt-value">$${{trade.trade_value.toLocaleString(undefined, {{maximumFractionDigits:0}})}}</span></div>`;
    html += `<div class="tt-row"><span class="tt-label">Cost</span><span class="tt-value">$${{trade.total_cost.toFixed(2)}} (${{trade.slippage_type}})</span></div>`;
  }}

  tooltip.innerHTML = html;
  tooltip.style.display = 'block';

  const x = param.point.x + mainEl.getBoundingClientRect().left;
  const y = param.point.y + mainEl.getBoundingClientRect().top;
  tooltip.style.left = (x + 20) + 'px';
  tooltip.style.top = (y - 20) + 'px';

  // Keep tooltip on screen
  const rect = tooltip.getBoundingClientRect();
  if (rect.right > window.innerWidth) tooltip.style.left = (x - rect.width - 20) + 'px';
  if (rect.bottom > window.innerHeight) tooltip.style.top = (y - rect.height) + 'px';
}});

mainEl.addEventListener('mouseleave', () => {{ tooltip.style.display = 'none'; }});

// Fit all charts
charts.forEach(c => c.timeScale().fitContent());
</script>
</body>
</html>"""


def main() -> None:
    """Generate interactive chart HTML."""
    print("[Chart] Loading data...")
    portfolio, trades = load_data()
    print(f"  {len(portfolio)} days, {len(trades)} trades")

    print("[Chart] Building chart data...")
    data = build_chart_data(portfolio, trades)

    print("[Chart] Generating HTML...")
    html = generate_html(data)

    output_path = OUTPUT_DIR / "vam_step1_interactive_chart.html"
    with open(output_path, "w") as f:
        f.write(html)

    print(f"  Saved: {output_path}")
    print(f"  Open in browser to view")


if __name__ == "__main__":
    main()
