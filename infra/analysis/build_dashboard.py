#!/usr/bin/env python3
"""Dashboard HTML interactif — perf trading par LLM"""
import json
from pathlib import Path
import pandas as pd

OUT_DIR = Path('/sessions/funny-elegant-dijkstra/mnt/Trader_IA/docs/audits/20260422_perf_trading_par_llm')
data = json.loads((OUT_DIR / 'metrics.json').read_text())

LLMS = ['ChatGPT 5.2', 'Gemini 3.0 Pro', 'Grok 4.1 Reasoning']
COLORS = {'ChatGPT 5.2': '#10a37f', 'Gemini 3.0 Pro': '#4285f4', 'Grok 4.1 Reasoning': '#1da1f2'}

# Prepare data
equity_series = {name: pd.DataFrame(data[name]['equity_curve']) for name in LLMS}
for name, df in equity_series.items():
    df['ts'] = pd.to_datetime(df['ts'])

# Build sector data per LLM
sector_data = {name: data[name]['sectors'] for name in LLMS}

# Build closed trades per LLM
trades_data = {name: data[name]['closed_trades_df'] for name in LLMS}

html = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Audit perf trading par LLM — Sandbox 48j</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
body { font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 24px; background: #f6f8fa; color: #1f2328; }
h1 { margin-top: 0; color: #0969da; }
h2 { border-bottom: 1px solid #d0d7de; padding-bottom: 6px; margin-top: 32px; }
.kpi-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin: 24px 0; }
.kpi { background: white; border: 1px solid #d0d7de; border-radius: 8px; padding: 16px; }
.kpi .llm { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
.kpi .ret { font-size: 28px; font-weight: 700; }
.kpi .pos { color: #1a7f37; }
.kpi .neg { color: #cf222e; }
.kpi .neu { color: #6e7781; }
.kpi .detail { font-size: 13px; color: #57606a; margin-top: 8px; line-height: 1.5; }
.chart { background: white; border: 1px solid #d0d7de; border-radius: 8px; padding: 16px; margin: 16px 0; }
.chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.caveat { background: #fff8c5; border: 1px solid #d4a72c; border-radius: 6px; padding: 12px 16px; font-size: 14px; }
.verdict { background: #ddf4ff; border-left: 4px solid #0969da; padding: 12px 16px; border-radius: 6px; margin: 16px 0; font-size: 15px; line-height: 1.6; }
table { border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; }
th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #eaeef2; font-size: 13px; }
th { background: #f6f8fa; text-align: center; font-weight: 600; }
th:first-child, td:first-child { text-align: left; }
</style>
</head>
<body>

<h1>Audit perf trading par LLM — Sandbox 48 jours</h1>
<p><b>Période</b> : 2026-03-04 21:16 → 2026-04-21 15:05 · <b>3 LLMs</b> en parallèle · <b>Capital initial</b> ≈ 100k€ chacun</p>

<div class="verdict">
  <b>TL;DR</b> — Aucun des 3 LLMs ne démontre d'alpha significatif sur 48 jours. <b>Gemini 3.0 Pro</b> a le profil le plus discipliné (profit factor 2.5, win rate 65%). <b>Grok 4.1 Reasoning</b> est disqualifié (PF 0.54, worst trade −33.8%). <b>ChatGPT 5.2</b> trop passif pour conclure (18 trades seulement). Marges trop fines pour absorber les frais live — pas de bascule broker défendable.
</div>

<h2>KPIs principaux</h2>
<div class="kpi-grid">
"""

for name in LLMS:
    d = data[name]
    ret = d['total_return_pct']
    cls = 'pos' if ret > 0.1 else ('neg' if ret < -0.1 else 'neu')
    realized = d['total_pnl'] - d['unrealized_pnl']
    pf_str = f"{d['profit_factor']:.2f}" if d['profit_factor'] else '∞'
    html += f"""
<div class="kpi" style="border-top: 4px solid {COLORS[name]};">
  <div class="llm">{name}</div>
  <div class="ret {cls}">{ret:+.2f}%</div>
  <div class="detail">
    <b>P&amp;L</b> : réalisé {realized:+,.0f}€ · latent {d['unrealized_pnl']:+,.0f}€<br/>
    <b>Max DD</b> : {d['max_drawdown_pct']:+.2f}% &nbsp; · &nbsp; <b>Trades</b> : {d['closed_trades']}<br/>
    <b>Win rate</b> : {d['win_rate']:.0f}% &nbsp; · &nbsp; <b>PF</b> : {pf_str}<br/>
    <b>Holding moyen</b> : {d['avg_holding_days']:.0f}j
  </div>
</div>
"""

html += """
</div>

<h2>Courbes d'equity (%)</h2>
<div class="chart" id="equity-chart" style="height:440px;"></div>

<h2>Drawdown</h2>
<div class="chart" id="dd-chart" style="height:340px;"></div>

<div class="chart-grid">
  <div class="chart" id="winloss-chart" style="height:380px;"></div>
  <div class="chart" id="holding-chart" style="height:380px;"></div>
</div>

<h2>Allocation sectorielle actuelle</h2>
<div class="chart-grid">
"""

for name in LLMS:
    html += f'<div class="chart" id="sector-{name.replace(" ","-").replace(".","_")}" style="height:360px;"></div>'

html += """
</div>

<h2>Trades clôturés — distribution P&L</h2>
<div class="chart" id="trades-hist" style="height:360px;"></div>

<h2>Top 10 winners / losers par LLM</h2>
<div id="top-trades"></div>

<h2>Crash commun du 23 mars 2026</h2>
<div class="caveat">
Les 3 LLMs ont touché leur max drawdown entre <b>09:01 et 11:04</b> le <b>2026-03-23</b>, à quelques minutes d'intervalle. Stress marché exogène — à croiser avec AG4 news et calendrier macro du jour.
</div>

<h2>Méthodologie & caveats</h2>
<ul>
  <li><b>Amorce filtrée</b> : runs 23/02 → 04/03 (equity ~51k, setup demi-portefeuille) exclus</li>
  <li><b>Reset filtré</b> : run <code>RUN_20260302154826_4680</code> (equity=0) exclu</li>
  <li><b>Frais / slippage</b> : non décomptés. Ajouter -0.3% à -0.8% par rotation pour un calcul live.</li>
  <li><b>Trades clôturés</b> : définition = symbole présent dans history, absent de latest. P&L réalisé = <code>(last_price - avg_price) × quantity</code> sur dernière ligne snapshot.</li>
  <li><b>Benchmark marché</b> : non croisé. À faire pour juger de l'alpha pur.</li>
</ul>

<script>
const COLORS = """ + json.dumps(COLORS) + """;
"""

# Equity traces
html += "const equityTraces = ["
for name in LLMS:
    df = equity_series[name]
    ts = df['ts'].dt.strftime('%Y-%m-%dT%H:%M:%S').tolist()
    eq_pct = df['equity_pct'].round(3).tolist()
    html += f"""
  {{x: {json.dumps(ts)}, y: {json.dumps(eq_pct)}, name: '{name}', mode: 'lines', line: {{color: COLORS['{name}'], width: 2}}}},"""
html += """
];
Plotly.newPlot('equity-chart', equityTraces, {
  margin: {t: 10, b: 40, l: 50, r: 10},
  xaxis: {title: 'Date'},
  yaxis: {title: 'Equity vs start (%)', zeroline: true, zerolinecolor: '#6e7781'},
  hovermode: 'x unified',
  legend: {orientation: 'h', y: -0.2}
});
"""

# Drawdown traces
html += "const ddTraces = ["
for name in LLMS:
    df = equity_series[name]
    ts = df['ts'].dt.strftime('%Y-%m-%dT%H:%M:%S').tolist()
    dd = df['drawdown_pct'].round(3).tolist()
    html += f"""
  {{x: {json.dumps(ts)}, y: {json.dumps(dd)}, name: '{name}', mode: 'lines', line: {{color: COLORS['{name}'], width: 1.5}}, fill: 'tozeroy'}},"""
html += """
];
Plotly.newPlot('dd-chart', ddTraces, {
  margin: {t: 10, b: 40, l: 50, r: 10},
  xaxis: {title: 'Date'},
  yaxis: {title: 'Drawdown (%)', zeroline: true},
  hovermode: 'x unified',
  legend: {orientation: 'h', y: -0.2}
});
"""

# Win/Loss distribution
html += "const winLossData = ["
for name in LLMS:
    trades = trades_data[name]
    winners = sum(1 for t in trades if t['realized_pnl'] > 0)
    losers = sum(1 for t in trades if t['realized_pnl'] <= 0)
    html += f"""
  {{x: ['Winners', 'Losers'], y: [{winners}, {losers}], name: '{name}', type: 'bar', marker: {{color: COLORS['{name}']}}}},"""
html += """
];
Plotly.newPlot('winloss-chart', winLossData, {
  title: 'Win / Loss count par LLM',
  margin: {t: 40, b: 40, l: 40, r: 10},
  barmode: 'group',
  yaxis: {title: 'Nb trades'}
});
"""

# Holding period boxplot
html += "const holdingData = ["
for name in LLMS:
    trades = trades_data[name]
    holdings = [t['holding_days'] for t in trades]
    html += f"""
  {{y: {json.dumps(holdings)}, name: '{name}', type: 'box', marker: {{color: COLORS['{name}']}}, boxpoints: 'outliers'}},"""
html += """
];
Plotly.newPlot('holding-chart', holdingData, {
  title: 'Holding period par trade (jours)',
  margin: {t: 40, b: 40, l: 40, r: 10},
  yaxis: {title: 'Jours', zeroline: false}
});
"""

# Sector pies
for name in LLMS:
    sectors = [s for s in sector_data[name] if s['sector'] and s['sector'] != '']
    labels = [s['sector'] or 'Unclassified' for s in sector_data[name]]
    values = [s['market_value'] for s in sector_data[name]]
    div_id = f"sector-{name.replace(' ','-').replace('.','_')}"
    html += f"""
Plotly.newPlot('{div_id}', [{{
  labels: {json.dumps(labels)},
  values: {json.dumps(values)},
  type: 'pie', hole: 0.4,
  textposition: 'outside', textinfo: 'label+percent'
}}], {{
  title: '{name}',
  margin: {{t: 40, b: 10, l: 10, r: 10}},
  showlegend: false
}});
"""

# Trades histogram
html += "const tradesHist = ["
for name in LLMS:
    trades = trades_data[name]
    pnls = [t['realized_pnl'] for t in trades]
    html += f"""
  {{x: {json.dumps(pnls)}, name: '{name}', type: 'histogram', opacity: 0.6, marker: {{color: COLORS['{name}']}}, nbinsx: 20}},"""
html += """
];
Plotly.newPlot('trades-hist', tradesHist, {
  title: 'Distribution P&L par trade clôturé',
  margin: {t: 40, b: 40, l: 40, r: 10},
  barmode: 'overlay',
  xaxis: {title: 'P&L réalisé (€)', zeroline: true},
  yaxis: {title: 'Nb trades'}
});
"""

# Top trades table
html += """
</script>

<script>
const topTradesHtml = [];
"""

top_html = '<div class="chart-grid">'
for name in LLMS:
    trades = sorted(trades_data[name], key=lambda t: t['realized_pnl'], reverse=True)
    top5 = trades[:5]
    bottom5 = trades[-5:][::-1]
    top_html += f'<div class="chart"><h3 style="margin-top:0;color:{COLORS[name]}">{name}</h3>'
    top_html += '<b>Top 5 winners</b><table><tr><th>Symbole</th><th>Ret %</th><th>P&L</th><th>Holding</th></tr>'
    for t in top5:
        top_html += f'<tr><td>{t["symbol"]}</td><td style="color:#1a7f37">{t["return_pct"]:+.1f}%</td><td style="color:#1a7f37">{t["realized_pnl"]:+,.0f}€</td><td>{t["holding_days"]:.0f}j</td></tr>'
    top_html += '</table><br/><b>Top 5 losers</b><table><tr><th>Symbole</th><th>Ret %</th><th>P&L</th><th>Holding</th></tr>'
    for t in bottom5:
        top_html += f'<tr><td>{t["symbol"]}</td><td style="color:#cf222e">{t["return_pct"]:+.1f}%</td><td style="color:#cf222e">{t["realized_pnl"]:+,.0f}€</td><td>{t["holding_days"]:.0f}j</td></tr>'
    top_html += '</table></div>'
top_html += '</div>'

# Replace the placeholder div#top-trades content via JS injection
html = html.replace('<div id="top-trades"></div>', f'<div id="top-trades">{top_html}</div>')

html += """
</script>

</body>
</html>
"""

(OUT_DIR / 'dashboard.html').write_text(html, encoding='utf-8')
print(f"Wrote {OUT_DIR / 'dashboard.html'}")
