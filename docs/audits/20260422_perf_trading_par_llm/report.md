# Audit perf trading par LLM — Sandbox 48 jours

**Date de l'audit** : 2026-04-22  
**Période analysée** : 2026-03-04 21:16 → 2026-04-21 15:05 (~48 jours calendaires, 334 runs horaires)  
**Périmètre** : Agent #1 Portfolio Manager, 3 LLMs en parallèle sur sandbox DuckDB  
**Source** : snapshots `ag1_v3_{chatgpt52,gemini30_pro,grok41_reasoning}.duckdb` du 2026-04-22

## TL;DR

Aucun des 3 LLMs ne démontre d'alpha significatif sur 48 jours sandbox (returns bruts entre −0.20% et +0.48%). **Gemini 3.0 Pro** est le seul avec une vraie discipline statistique (profit factor 2.5, win rate 65%), ChatGPT reste trop passif pour conclure, et **Grok est disqualifié** (profit factor 0.54, pertes réalisées −1,619€). Les marges sont trop fines pour survivre aux frais de transaction et au slippage réel en live trading — aucune bascule broker live n'est défendable sur ces seules données.

## Résultats consolidés

| Métrique | ChatGPT 5.2 | Gemini 3.0 Pro | Grok 4.1 Reasoning |
|---|---:|---:|---:|
| Equity de départ (sandbox) | 101,240€ | 101,297€ | 101,789€ |
| Equity actuelle | 101,730€ | 101,098€ | 101,672€ |
| Return sandbox | +0.48% | -0.20% | -0.11% |
| Max drawdown | -2.49% | -2.37% | -2.98% |
| P&L réalisé (trades clôturés) | +94€ | +1 955€ | -1 619€ |
| P&L latent (positions ouvertes) | +2 211€ | +602€ | +3 681€ |
| P&L total (réal. + latent) | +2 306€ | +2 557€ | +2 063€ |
| Trades clôturés | 18 | 52 | 42 |
| Win rate | 61.1% | 65.4% | 42.9% |
| Avg winner | +50€ | +96€ | +107€ |
| Avg loser | -76€ | -72€ | -148€ |
| Profit factor | 1.21 | 2.51 | 0.54 |
| Holding moyen | 25.3 j | 15.9 j | 16.2 j |
| Positions actuelles | 20 | 25 | 21 |
| Symboles touchés (total) | 36 | 75 | 61 |

## Analyse par LLM

### ChatGPT 5.2

**Profil** : Conservateur / buy-and-hold long  
**Return** : +0.48% (+490€) sur 48 jours
**Max DD** : -2.49%  
**P&L split** : réalisé +94€ + latent +2 211€  
**Trade stats** : 18 trades, WR 61%, PF 1.21, holding 25j  
**Best trade** : `TTE.PA` +173€ (+28.5%)  
**Worst trade** : `ACA.PA` -239€ (-13.2%)  

**Verdict** : Le seul LLM en territoire positif sur la période, mais avec très peu de trades (18) et un holding moyen de 25 jours. Le P&L est quasi-intégralement latent (+2,211€ sur +2,306€ total), le réalisé est proche de zéro (+94€). Impossible de conclure sur sa capacité de timing — l'échantillon est trop petit. Profit factor 1.21 signifie que pour chaque 1€ perdu, il en gagne 1.21, ce qui est marginal.

### Gemini 3.0 Pro

**Profil** : Trader actif discipliné  
**Return** : -0.20% (-199€) sur 48 jours
**Max DD** : -2.37%  
**P&L split** : réalisé +1 955€ + latent +602€  
**Trade stats** : 52 trades, WR 65%, PF 2.51, holding 16j  
**Best trade** : `TTE.PA` +287€ (+9.8%)  
**Worst trade** : `DIM.PA` -185€ (-7.4%)  

**Verdict** : Le meilleur profil statistique des trois : 52 trades sur 48 jours (> 1/jour), win rate 65%, profit factor 2.5. P&L réalisé **+1,955€** largement positif, mais compensé par un latent modeste (+602€) — Gemini prend ses profits et coupe ses pertes, style « trader algorithmique ». Return total négatif (−0.20%) explique par l'equity de départ élevée et des positions ouvertes au démarrage qui ont perdu du terrain. **Candidat le plus solide pour une bascule live**, sous réserve de valider la résistance aux frais.

### Grok 4.1 Reasoning

**Profil** : Volatile / swing trader à forte conviction  
**Return** : -0.11% (-117€) sur 48 jours
**Max DD** : -2.98%  
**P&L split** : réalisé -1 619€ + latent +3 681€  
**Trade stats** : 42 trades, WR 43%, PF 0.54, holding 16j  
**Best trade** : `FDE.PA` +428€ (+20.5%)  
**Worst trade** : `EAPI.PA` -875€ (-33.8%)  

**Verdict** : Profit factor 0.54 : pour chaque 1€ perdu, Grok n'en gagne que 54c. Pertes réalisées de **−1,619€** compensées uniquement par un latent important (+3,681€) — Grok « laisse courir les gagnants » mais se fait sortir trop vite ou garde trop longtemps les perdants. Worst trade EAPI.PA à −875€ (−33.8%) trahit un défaut de stop-loss. **Disqualifié pour le live dans son état actuel.**

## Événement marché commun — crash du 23 mars

Les 3 LLMs ont touché leur max drawdown **le même jour** (2026-03-23) à quelques minutes près, ce qui confirme qu'il s'agit d'un stress marché exogène (probablement un événement macro, à croiser avec les news AG4 du jour). La résistance au stress est comparable :

| LLM | Drawdown le 23/03 | Recovery |
|---|---:|---|
| ChatGPT 5.2 | -2.49% | Non calculé — voir equity curve |
| Gemini 3.0 Pro | -2.37% | Non calculé — voir equity curve |
| Grok 4.1 Reasoning | -2.98% | Non calculé — voir equity curve |

## Méthodologie

**Source** : `portfolio_positions_mtm_history` (snapshot horaire des positions MTM) + `portfolio_positions_mtm_latest` (état actuel). Chaque LLM a sa propre DuckDB (3 au total, schéma identique).

**Filtres appliqués** :
- Phase **amorce** filtrée (2026-02-23 → 2026-03-02, equity ~51k avec setup demi-portefeuille)
- Run de reset `RUN_20260302154826_4680` filtré (equity=0, snapshot transitoire)
- Sandbox effective : runs post-`2026-03-04 21:00` avec `SUM(market_value) > 80,000€`

**Définition des métriques** :
- *Equity* = somme `market_value` de toutes les positions (inclut ligne synthétique `Meta` 50k + cash + equities)
- *Drawdown* = `(equity / running_peak) - 1` min sur la période
- *Trade clôturé* = symbole présent dans `history` mais absent de `latest`. P&L réalisé estimé sur la dernière ligne snapshot = `(last_price - avg_price) * quantity`
- *Win rate* = `trades_positifs / trades_clôturés`
- *Profit factor* = `sum(winners) / |sum(losers)|`

**Caveats** :
- **48 jours = court**. Les returns sandbox ne sont pas statistiquement significatifs.
- **Pas de frais / slippage / spread** dans ces chiffres. En live, compter **−0.3% à −0.8%** de drag par rotation complète selon le broker.
- Le modèle **MTM snapshot** ne reconstitue pas exactement les trades — on capture l'avg_price à la fermeture de la position, mais les achats/ventes partiels au sein d'une même position sont lissés.
- L'**asset_class** est incohérent dans les DBs (`Equity` vs `EQUITY` vs `FX`). À normaliser côté AG1 MTM updater.
- Pas de benchmark marché (CAC 40, MSCI) croisé ici — à faire pour juger de l'alpha pur.

## Recommandations

1. **Ne pas brancher le broker live uniquement sur la base de cet audit.** Les marges sont trop fines pour absorber les frais réels.
2. **Prolonger la sandbox** à au moins 90-120 jours avant toute bascule, pour capturer plus d'événements marché (le seul stress visible est le 23/03).
3. **Si bascule partielle** : candidater **Gemini 3.0 Pro** d'abord, avec capital réduit (10-20k max), un kill-switch dur à −3% MTD, et une allow-list de symboles.
4. **Disqualifier Grok 4.1 Reasoning** dans sa config actuelle : PF 0.54 + absence visible de stop-loss (perte EAPI.PA à −33.8%). Revoir le prompt de l'agent ou exclure.
5. **Normaliser asset_class** (`Equity` vs `EQUITY`) côté AG1 — bug cosmétique mais biaise les agrégations.
6. **Croiser le crash du 23/03 avec AG4 news** pour comprendre l'événement déclencheur (macro ? Fed ? earnings ?).
7. **Passer à l'audit #27** : cohérence cross-LLM sur les mêmes tickers — qui achète quoi quand, et y a-t-il des consensus/divergences exploitables ?

## Annexes

- [Dashboard interactif](./dashboard.html)
- [`metrics.json`](./metrics.json) — données brutes
- [`compute_perf_audit.py`](../../../infra/analysis/compute_perf_audit.py) — script de génération
