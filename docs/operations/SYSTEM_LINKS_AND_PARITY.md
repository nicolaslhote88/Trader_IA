# Liens entre systèmes & parité à maintenir

**MAJ 2026-06-29.** Ce document recense les endroits où une même logique est **dupliquée** entre le pipeline AG1 (n8n) et le **dashboard** (`app.py`). Toute modification d'un côté DOIT être répercutée de l'autre, sinon les vues divergent silencieusement de la réalité d'exécution (c'est arrivé pour le funnel et la matrice).

## ⚠️ Règle d'or
Le **dashboard `app.py` réimplémente le scoring et les gates d'AG1** (il ne lit PAS la sortie du run — il recalcule à partir des bases DuckDB `ag2_v3` / `ag3_v2` / `ag4_*`). Donc :
> **Toute modif des formules de scoring, des règles de décision, des gates ou des seuils de fraîcheur dans AG1 doit être répercutée dans `app.py` (matrice + funnel), et inversement.**

⚠️ **La version LIVE du dashboard `/opt/trading-dashboard/app/app.py` a DIVERGÉ du repo `services/dashboard/app.py`** (le repo est en retard). La version live est la source de vérité ; déployer en éditant la version LIVE (cf. `SCHEDULING_AND_LOAD.md` pour les pièges Python 3.12 / backup).

## Carte des duplications

| Logique | Côté AG1 (n8n, source d'exécution) | Côté dashboard (`app.py`, réimplémentation) |
|---|---|---|
| **Scoring matrice** (Risk, Reward, R, p_win, EV(R)) | node `Calcul Matrice & Briefing` | fonction de calcul matrice (≈ L4900-5045) — **Vue consolidée Multi-Agents** (nuage Risk/Reward) |
| **prob_score** (poids) | `0.36 tech + 0.34 funda + 0.20 news + 0.10 régime` | **idem** (doit rester identique) |
| **risk_score V2** (2026-06-30) | poids tactiques `vol 0.26 / liq 0.18 / event 0.16 / funda 0.16 / news 0.10 / conc 0.09 / options 0.05` + **renorm sur composantes observées** (exclut les défauts) | **idem** (mêmes poids + même renorm, flags `funda_usable`/`news_count`/earnings/spread/IV) |
| **Règle décision** `enter_core` / `reduce_core` | `Calcul Matrice` (`ev_r≥0.20 ET reward≥seuil ET risk≤seuil ET grade∈{A,B}` ; reduce si ev_r<0 …) | **idem** (≈ L5111-5140) |
| **Grades A/B/C** | quantiles dynamiques sur `prob_score_for_grade` | idem (seuils p-quantile recalculés) |
| **Gates matrice** (data_quality, earnings≤7j, liquidité `liq_risk≥85`, rr_outlier, options) | `Calcul Matrice` | idem |
| **Funnel tradabilité** (System Health) | implicite (R8 + preflight) | recalcul dans `app.py` (`spread_exploitable`, `tech_gate_ready`, etc.) |
| **Seuils de fraîcheur** | H1≤96h, D1≤240h, YF≤72h, funda≤168h (R8) | **idem** (funnel + matrice) |
| **`data_age` = max(stocké, âge réel = now − date du dernier bar)** | R8 (`R8 — Data Prep for Matrix`) | **idem** (`h1_age_hours_effective`/`d1_age_hours_effective`) — ne PAS revenir au `data_age` figé |

## Étapes du pipeline = ce que chaque vue montre (NE PAS confondre)
1. **Étape décision matrice** (`Calcul Matrice` / nuage dashboard + funnel) : scoring + gates basés sur les données **yfinance/AG2/AG3/AG4**. C'est ce que le dashboard affiche.
2. **Étape exécution / preflight** (`AG1.V4 — Liquidity Preflight` + node 7 safety) : verdict liquidité **autoritatif basé IBKR** (snapshot bid/ask), + contrat, permissions, cash. **NON reflété dans le dashboard.** C'est ici que vivent les tolérances liquidité (warm-up sous-lots, `SPREAD_UNQUOTED`, filet haut-volume ≥1M). Cf [[18-ag1-run-timing-us]] en mémoire.
> Conséquence : un symbole « Entrer » dans le dashboard repasse un **contrôle liquidité IBKR final au moment de l'ordre**. Le dashboard le rappelle déjà (« les contrôles IBKR … liquidité restent exécutés au moment de l'ordre »). Ne PAS chercher à dupliquer le verdict IBKR dans le dashboard (il n'a pas d'accès IBKR per-symbole).

## Autres liens inter-systèmes à garder en tête
- **News par-symbole** : `ag4_spe_v2.news_analyzed` → R8 (impact + **texte top-3 vers le LLM**) → `Calcul Matrice` (`opportunity_pack.rows[].news` + `newsGeneratedAt`). Le dashboard n'utilise que l'**impact** (sentiment_prob), pas le texte.
- **Ledger ordres** : `core.orders` (AG1) mis à jour à l'exécution + **post-approbation** par le node `Update Ledger Status` du workflow `Order Approval Decide`. Écrire `ag1_v4_consensus.duckdb` **en duckdb 1.4.3** (lecteur task-runner n8n).
- **Univers** : un symbole n'est traité par AG2/AG3 que s'il est dans `ag2_v3.universe_segments` (pas seulement `universe`). Cf `AGENTS.md` § classification.
- **Ordonnancement / contention DuckDB** : cf `SCHEDULING_AND_LOAD.md` (crons, durées, déconfliction).

## Checklist avant de modifier le scoring/les gates AG1
1. Modifier le node n8n (`Calcul Matrice`, `R8`, preflight, safety).
2. Répercuter la même formule/seuil dans `/opt/trading-dashboard/app/app.py` (matrice ≈ L4900-5175 ET funnel System Health).
3. Vérifier la cohérence : un même symbole doit donner le même grade/décision dans le dashboard et dans le run (`core.model_proposals` / `opportunity_pack`).
4. Committer côté repo (`services/dashboard/app.py` à resynchroniser depuis la version live, qui a divergé).
