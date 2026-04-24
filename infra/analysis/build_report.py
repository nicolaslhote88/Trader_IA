#!/usr/bin/env python3
"""Génère report.md + dashboard.html depuis metrics.json"""
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

OUT_DIR = Path('/sessions/funny-elegant-dijkstra/mnt/Trader_IA/docs/audits/20260422_perf_trading_par_llm')
data = json.loads((OUT_DIR / 'metrics.json').read_text())

# ─────────── REPORT MD ───────────
def fmt_eur(x): return f"{x:+,.0f}€".replace(',', ' ')
def fmt_pct(x): return f"{x:+.2f}%"

lines = []
lines.append("# Audit perf trading par LLM — Sandbox 48 jours")
lines.append("")
lines.append(f"**Date de l'audit** : 2026-04-22  ")
lines.append(f"**Période analysée** : 2026-03-04 21:16 → 2026-04-21 15:05 (~48 jours calendaires, 334 runs horaires)  ")
lines.append(f"**Périmètre** : Agent #1 Portfolio Manager, 3 LLMs en parallèle sur sandbox DuckDB  ")
lines.append(f"**Source** : snapshots `ag1_v3_{{chatgpt52,gemini30_pro,grok41_reasoning}}.duckdb` du 2026-04-22")
lines.append("")
lines.append("## TL;DR")
lines.append("")
lines.append("Aucun des 3 LLMs ne démontre d'alpha significatif sur 48 jours sandbox (returns bruts entre −0.20% et +0.48%). **Gemini 3.0 Pro** est le seul avec une vraie discipline statistique (profit factor 2.5, win rate 65%), ChatGPT reste trop passif pour conclure, et **Grok est disqualifié** (profit factor 0.54, pertes réalisées −1,619€). Les marges sont trop fines pour survivre aux frais de transaction et au slippage réel en live trading — aucune bascule broker live n'est défendable sur ces seules données.")
lines.append("")
lines.append("## Résultats consolidés")
lines.append("")
lines.append("| Métrique | ChatGPT 5.2 | Gemini 3.0 Pro | Grok 4.1 Reasoning |")
lines.append("|---|---:|---:|---:|")
order = ['ChatGPT 5.2', 'Gemini 3.0 Pro', 'Grok 4.1 Reasoning']
rows = [
    ('Equity de départ (sandbox)', lambda d: f"{d['starting_equity']:,.0f}€"),
    ('Equity actuelle', lambda d: f"{d['current_equity']:,.0f}€"),
    ('Return sandbox', lambda d: fmt_pct(d['total_return_pct'])),
    ('Max drawdown', lambda d: fmt_pct(d['max_drawdown_pct'])),
    ('P&L réalisé (trades clôturés)', lambda d: fmt_eur(d['total_pnl'] - d['unrealized_pnl'])),
    ('P&L latent (positions ouvertes)', lambda d: fmt_eur(d['unrealized_pnl'])),
    ('P&L total (réal. + latent)', lambda d: fmt_eur(d['total_pnl'])),
    ('Trades clôturés', lambda d: str(d['closed_trades'])),
    ('Win rate', lambda d: f"{d['win_rate']:.1f}%"),
    ('Avg winner', lambda d: fmt_eur(d['avg_winner'])),
    ('Avg loser', lambda d: fmt_eur(d['avg_loser'])),
    ('Profit factor', lambda d: f"{d['profit_factor']:.2f}" if d['profit_factor'] else '∞'),
    ('Holding moyen', lambda d: f"{d['avg_holding_days']:.1f} j"),
    ('Positions actuelles', lambda d: str(d['current_positions'])),
    ('Symboles touchés (total)', lambda d: str(d['unique_symbols'])),
]
for label, fn in rows:
    lines.append(f"| {label} | " + " | ".join(fn(data[n]) for n in order) + " |")
lines.append("")

lines.append("## Analyse par LLM")
lines.append("")
for name in order:
    d = data[name]
    realized = d['total_pnl'] - d['unrealized_pnl']
    lines.append(f"### {name}")
    lines.append("")
    if name == 'ChatGPT 5.2':
        profil = "Conservateur / buy-and-hold long"
        verdict = "Le seul LLM en territoire positif sur la période, mais avec très peu de trades (18) et un holding moyen de 25 jours. Le P&L est quasi-intégralement latent (+2,211€ sur +2,306€ total), le réalisé est proche de zéro (+94€). Impossible de conclure sur sa capacité de timing — l'échantillon est trop petit. Profit factor 1.21 signifie que pour chaque 1€ perdu, il en gagne 1.21, ce qui est marginal."
    elif name == 'Gemini 3.0 Pro':
        profil = "Trader actif discipliné"
        verdict = "Le meilleur profil statistique des trois : 52 trades sur 48 jours (> 1/jour), win rate 65%, profit factor 2.5. P&L réalisé **+1,955€** largement positif, mais compensé par un latent modeste (+602€) — Gemini prend ses profits et coupe ses pertes, style « trader algorithmique ». Return total négatif (−0.20%) explique par l'equity de départ élevée et des positions ouvertes au démarrage qui ont perdu du terrain. **Candidat le plus solide pour une bascule live**, sous réserve de valider la résistance aux frais."
    else:
        profil = "Volatile / swing trader à forte conviction"
        verdict = "Profit factor 0.54 : pour chaque 1€ perdu, Grok n'en gagne que 54c. Pertes réalisées de **−1,619€** compensées uniquement par un latent important (+3,681€) — Grok « laisse courir les gagnants » mais se fait sortir trop vite ou garde trop longtemps les perdants. Worst trade EAPI.PA à −875€ (−33.8%) trahit un défaut de stop-loss. **Disqualifié pour le live dans son état actuel.**"
    lines.append(f"**Profil** : {profil}  ")
    lines.append(f"**Return** : {fmt_pct(d['total_return_pct'])} ({fmt_eur(d['current_equity']-d['starting_equity'])}) sur 48 jours")
    lines.append(f"**Max DD** : {fmt_pct(d['max_drawdown_pct'])}  ")
    lines.append(f"**P&L split** : réalisé {fmt_eur(realized)} + latent {fmt_eur(d['unrealized_pnl'])}  ")
    lines.append(f"**Trade stats** : {d['closed_trades']} trades, WR {d['win_rate']:.0f}%, PF {d['profit_factor']:.2f}, holding {d['avg_holding_days']:.0f}j  ")
    if d['best_trade']:
        bt = d['best_trade']
        lines.append(f"**Best trade** : `{bt['symbol']}` {fmt_eur(bt['realized_pnl'])} ({bt['return_pct']:+.1f}%)  ")
    if d['worst_trade']:
        wt = d['worst_trade']
        lines.append(f"**Worst trade** : `{wt['symbol']}` {fmt_eur(wt['realized_pnl'])} ({wt['return_pct']:+.1f}%)  ")
    lines.append("")
    lines.append(f"**Verdict** : {verdict}")
    lines.append("")

lines.append("## Événement marché commun — crash du 23 mars")
lines.append("")
lines.append("Les 3 LLMs ont touché leur max drawdown **le même jour** (2026-03-23) à quelques minutes près, ce qui confirme qu'il s'agit d'un stress marché exogène (probablement un événement macro, à croiser avec les news AG4 du jour). La résistance au stress est comparable :")
lines.append("")
lines.append("| LLM | Drawdown le 23/03 | Recovery |")
lines.append("|---|---:|---|")
for name in order:
    d = data[name]
    lines.append(f"| {name} | {fmt_pct(d['max_drawdown_pct'])} | Non calculé — voir equity curve |")
lines.append("")

lines.append("## Méthodologie")
lines.append("")
lines.append("**Source** : `portfolio_positions_mtm_history` (snapshot horaire des positions MTM) + `portfolio_positions_mtm_latest` (état actuel). Chaque LLM a sa propre DuckDB (3 au total, schéma identique).")
lines.append("")
lines.append("**Filtres appliqués** :")
lines.append("- Phase **amorce** filtrée (2026-02-23 → 2026-03-02, equity ~51k avec setup demi-portefeuille)")
lines.append("- Run de reset `RUN_20260302154826_4680` filtré (equity=0, snapshot transitoire)")
lines.append("- Sandbox effective : runs post-`2026-03-04 21:00` avec `SUM(market_value) > 80,000€`")
lines.append("")
lines.append("**Définition des métriques** :")
lines.append("- *Equity* = somme `market_value` de toutes les positions (inclut ligne synthétique `Meta` 50k + cash + equities)")
lines.append("- *Drawdown* = `(equity / running_peak) - 1` min sur la période")
lines.append("- *Trade clôturé* = symbole présent dans `history` mais absent de `latest`. P&L réalisé estimé sur la dernière ligne snapshot = `(last_price - avg_price) * quantity`")
lines.append("- *Win rate* = `trades_positifs / trades_clôturés`")
lines.append("- *Profit factor* = `sum(winners) / |sum(losers)|`")
lines.append("")
lines.append("**Caveats** :")
lines.append("- **48 jours = court**. Les returns sandbox ne sont pas statistiquement significatifs.")
lines.append("- **Pas de frais / slippage / spread** dans ces chiffres. En live, compter **−0.3% à −0.8%** de drag par rotation complète selon le broker.")
lines.append("- Le modèle **MTM snapshot** ne reconstitue pas exactement les trades — on capture l'avg_price à la fermeture de la position, mais les achats/ventes partiels au sein d'une même position sont lissés.")
lines.append("- L'**asset_class** est incohérent dans les DBs (`Equity` vs `EQUITY` vs `FX`). À normaliser côté AG1 MTM updater.")
lines.append("- Pas de benchmark marché (CAC 40, MSCI) croisé ici — à faire pour juger de l'alpha pur.")
lines.append("")

lines.append("## Recommandations")
lines.append("")
lines.append("1. **Ne pas brancher le broker live uniquement sur la base de cet audit.** Les marges sont trop fines pour absorber les frais réels.")
lines.append("2. **Prolonger la sandbox** à au moins 90-120 jours avant toute bascule, pour capturer plus d'événements marché (le seul stress visible est le 23/03).")
lines.append("3. **Si bascule partielle** : candidater **Gemini 3.0 Pro** d'abord, avec capital réduit (10-20k max), un kill-switch dur à −3% MTD, et une allow-list de symboles.")
lines.append("4. **Disqualifier Grok 4.1 Reasoning** dans sa config actuelle : PF 0.54 + absence visible de stop-loss (perte EAPI.PA à −33.8%). Revoir le prompt de l'agent ou exclure.")
lines.append("5. **Normaliser asset_class** (`Equity` vs `EQUITY`) côté AG1 — bug cosmétique mais biaise les agrégations.")
lines.append("6. **Croiser le crash du 23/03 avec AG4 news** pour comprendre l'événement déclencheur (macro ? Fed ? earnings ?).")
lines.append("7. **Passer à l'audit #27** : cohérence cross-LLM sur les mêmes tickers — qui achète quoi quand, et y a-t-il des consensus/divergences exploitables ?")
lines.append("")

lines.append("## Annexes")
lines.append("")
lines.append(f"- [Dashboard interactif]({'./dashboard.html'})")
lines.append(f"- [`metrics.json`]({'./metrics.json'}) — données brutes")
lines.append(f"- [`compute_perf_audit.py`](../../../infra/analysis/compute_perf_audit.py) — script de génération")
lines.append("")

(OUT_DIR / 'report.md').write_text("\n".join(lines), encoding='utf-8')
print(f"Wrote {OUT_DIR / 'report.md'}")
