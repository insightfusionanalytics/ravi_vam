import json
from pathlib import Path

OUT = Path("/Users/chirag/ravi_vam/scripts/_tmp_analysis/deck/svgs")
OUT.mkdir(exist_ok=True, parents=True)

GREEN = "#1FAD5C"
RED = "#DC4646"
BLUE = "#4C7DFF"
MUTED = "#5C6470"
DIM = "#9098A3"
TEXT = "#EDEEF0"
BORDER = "#262A31"

# ---------- 1. Whipsaw split bar chart (kill switch vs trim), scaled for slide ----------
def whipsaw_split_chart():
    W, H = 1500, 520
    bar_h = 64
    rows = [
        ("UPRO-Only", "Kill switch (VIX / 200-SMA, 0-day confirm)", 71, 29, "24 episodes", "+125.0pp net"),
        ("UPRO-Only", "Defensive trim (50-SMA, 2-day confirm)", 81, 19, "16 episodes", "+56.6pp net"),
        ("UPRO + TQQQ", "Kill switch (VIX / 200-SMA, 0-day confirm)", 82, 18, "17 episodes", "+106.1pp net"),
        ("UPRO + TQQQ", "Defensive trim (50-SMA, 2-day confirm)", 66, 34, "41 episodes", "+52.6pp net"),
    ]
    label_w = 620
    bar_w = 620
    svg = [f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg">']
    y = 10
    prev_strategy = None
    for strategy, mech, false_pct, correct_pct, n, net in rows:
        if strategy != prev_strategy:
            svg.append(f'<text x="0" y="{y+28}" font-family="IBM Plex Sans" font-size="30" font-weight="700" fill="{BLUE}">{strategy}</text>')
            y += 54
            prev_strategy = strategy
        false_w = bar_w * false_pct / 100
        correct_w = bar_w * correct_pct / 100
        svg.append(f'<text x="0" y="{y+bar_h/2+9}" font-family="IBM Plex Sans" font-size="24" fill="{DIM}">{mech}</text>')
        bx = label_w
        svg.append(f'<rect x="{bx}" y="{y}" width="{false_w:.1f}" height="{bar_h}" fill="{RED}" rx="4"/>')
        svg.append(f'<rect x="{bx+false_w:.1f}" y="{y}" width="{correct_w:.1f}" height="{bar_h}" fill="{GREEN}" rx="4"/>')
        svg.append(f'<text x="{bx+16}" y="{y+bar_h/2+9}" font-family="JetBrains Mono" font-size="26" font-weight="600" fill="#0A0B0F">{false_pct}%</text>')
        svg.append(f'<text x="{bx+bar_w+24:.1f}" y="{y+bar_h/2+9}" font-family="JetBrains Mono" font-size="26" font-weight="700" fill="{TEXT}">{net}</text>')
        y += bar_h + 26
    svg.append('</svg>')
    return '\n'.join(svg)

# ---------- 2. Capture chart ----------
def capture_chart():
    W, H = 900, 560
    groups = [("UPRO-Only", 59.0, 61.4), ("UPRO + TQQQ", 61.9, 62.8)]
    chart_h = 380
    base_y = 40 + chart_h
    max_val = 70
    bar_w = 110
    gap_within = 30
    gap_between = 160
    x = 130
    svg = [f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg">']
    for pct in (0, 20, 40, 60):
        gy = base_y - chart_h * pct / max_val
        svg.append(f'<line x1="90" y1="{gy:.1f}" x2="{W-20}" y2="{gy:.1f}" stroke="{BORDER}" stroke-width="2"/>')
        svg.append(f'<text x="80" y="{gy+8:.1f}" font-family="JetBrains Mono" font-size="22" fill="{DIM}" text-anchor="end">{pct}%</text>')
    for name, up, down in groups:
        up_h = chart_h * up / max_val
        down_h = chart_h * down / max_val
        svg.append(f'<rect x="{x}" y="{base_y-up_h:.1f}" width="{bar_w}" height="{up_h:.1f}" fill="{GREEN}" rx="6"/>')
        svg.append(f'<text x="{x+bar_w/2:.1f}" y="{base_y-up_h-16:.1f}" font-family="JetBrains Mono" font-size="26" font-weight="700" fill="{GREEN}" text-anchor="middle">{up:.1f}%</text>')
        x2 = x + bar_w + gap_within
        svg.append(f'<rect x="{x2}" y="{base_y-down_h:.1f}" width="{bar_w}" height="{down_h:.1f}" fill="{RED}" rx="6"/>')
        svg.append(f'<text x="{x2+bar_w/2:.1f}" y="{base_y-down_h-16:.1f}" font-family="JetBrains Mono" font-size="26" font-weight="700" fill="{RED}" text-anchor="middle">{down:.1f}%</text>')
        svg.append(f'<text x="{x+bar_w+gap_within/2:.1f}" y="{base_y+42}" font-family="IBM Plex Sans" font-size="26" font-weight="600" fill="{TEXT}" text-anchor="middle">{name}</text>')
        x += bar_w*2 + gap_within + gap_between
    svg.append(f'<line x1="90" y1="{base_y}" x2="{W-20}" y2="{base_y}" stroke="{DIM}" stroke-width="2"/>')
    svg.append(f'<rect x="{W-260}" y="10" width="22" height="22" fill="{GREEN}" rx="3"/><text x="{W-228}" y="27" font-family="IBM Plex Sans" font-size="22" fill="{DIM}">Upside capture</text>')
    svg.append(f'<rect x="{W-260}" y="42" width="22" height="22" fill="{RED}" rx="3"/><text x="{W-228}" y="59" font-family="IBM Plex Sans" font-size="22" fill="{DIM}">Downside capture</text>')
    svg.append('</svg>')
    return '\n'.join(svg)

# ---------- 3. Underwater curve (reuse data from earlier report generation) ----------
def underwater_chart(pts):
    dates = [p['date'] for p in pts]
    pv = [p['pv'] for p in pts]
    running_max = []
    m = 0
    for v in pv:
        m = max(m, v)
        running_max.append(m)
    dd = [(v - rm) / rm * 100 for v, rm in zip(pv, running_max)]
    n = len(dd)
    step = max(1, n // 500)
    idx = list(range(0, n, step))
    if idx[-1] != n - 1:
        idx.append(n - 1)

    W, H = 1650, 560
    plot_w, plot_h = W - 140, H - 110
    x0, y0 = 120, 30
    min_dd = min(dd)
    def X(i): return x0 + plot_w * i / (n - 1)
    def Y(v): return y0 + plot_h * (0 - v) / (0 - min_dd)

    path = "M " + " L ".join(f"{X(i):.1f},{Y(dd[i]):.1f}" for i in idx)
    area = path + f" L {X(idx[-1]):.1f},{Y(0):.1f} L {X(idx[0]):.1f},{Y(0):.1f} Z"

    svg = [f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg">']
    for pct in (0, -10, -20, -30, -40):
        gy = Y(pct)
        svg.append(f'<line x1="{x0}" y1="{gy:.1f}" x2="{W-20}" y2="{gy:.1f}" stroke="{BORDER}" stroke-width="2"/>')
        svg.append(f'<text x="{x0-16}" y="{gy+8:.1f}" font-family="JetBrains Mono" font-size="22" fill="{DIM}" text-anchor="end">{pct}%</text>')
    svg.append(f'<path d="{area}" fill="{RED}" fill-opacity="0.14"/>')
    svg.append(f'<path d="{path}" fill="none" stroke="{RED}" stroke-width="2.5"/>')
    year_marks = {}
    for i in idx:
        yr = dates[i][:4]
        if yr not in year_marks and int(yr) % 2 == 1:
            year_marks[yr] = i
    for yr, i in sorted(year_marks.items()):
        svg.append(f'<text x="{X(i):.1f}" y="{H-14}" font-family="JetBrains Mono" font-size="22" fill="{DIM}" text-anchor="middle">{yr}</text>')
    svg.append(f'<line x1="{x0}" y1="{Y(0):.1f}" x2="{W-20}" y2="{Y(0):.1f}" stroke="{DIM}" stroke-width="2"/>')
    svg.append('</svg>')
    return '\n'.join(svg)

# ---------- 4. Concrete example mini charts ----------
def example_chart(window, def_start, def_end, missed_gain_pct, start_date, end_date, w=760, h=420):
    prices = [p['price'] for p in window]
    n = len(prices)
    plot_w, plot_h = w - 60, h - 130
    x0, y0 = 40, 30
    pmin, pmax = min(prices), max(prices)
    pad = (pmax - pmin) * 0.12 or 1
    pmin -= pad; pmax += pad
    def X(i): return x0 + plot_w * i / (n - 1)
    def Y(v): return y0 + plot_h * (pmax - v) / (pmax - pmin)
    path = "M " + " L ".join(f"{X(i):.1f},{Y(prices[i]):.1f}" for i in range(n))

    svg = [f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">']
    svg.append(f'<rect x="{X(def_start):.1f}" y="{y0}" width="{X(def_end)-X(def_start):.1f}" height="{plot_h}" fill="{RED}" fill-opacity="0.16"/>')
    svg.append(f'<line x1="{X(def_start):.1f}" y1="{y0}" x2="{X(def_start):.1f}" y2="{y0+plot_h}" stroke="{RED}" stroke-width="2" stroke-dasharray="6,5"/>')
    svg.append(f'<line x1="{X(def_end):.1f}" y1="{y0}" x2="{X(def_end):.1f}" y2="{y0+plot_h}" stroke="{RED}" stroke-width="2" stroke-dasharray="6,5"/>')
    svg.append(f'<path d="{path}" fill="none" stroke="{BLUE}" stroke-width="3"/>')
    mid_x = (X(def_start) + X(def_end)) / 2
    svg.append(f'<text x="{mid_x:.1f}" y="{y0+28}" font-family="IBM Plex Sans" font-size="22" font-weight="700" fill="{RED}" text-anchor="middle">DEFENSIVE / CASH</text>')
    svg.append(f'<text x="{X(n-1):.1f}" y="{Y(prices[-1])-18:.1f}" font-family="JetBrains Mono" font-size="24" font-weight="700" fill="{GREEN}" text-anchor="end">+{missed_gain_pct:.1f}%</text>')
    svg.append(f'<text x="{x0}" y="{h-16}" font-family="JetBrains Mono" font-size="20" fill="{DIM}">{start_date}</text>')
    svg.append(f'<text x="{X(n-1):.1f}" y="{h-16}" font-family="JetBrains Mono" font-size="20" fill="{DIM}" text-anchor="end">{end_date}</text>')
    svg.append('</svg>')
    return '\n'.join(svg)

if __name__ == '__main__':
    (OUT / 'whipsaw_split.svg').write_text(whipsaw_split_chart())
    (OUT / 'capture.svg').write_text(capture_chart())
    print("base charts written")
