# Liens entre systèmes & parité à maintenir

**MAJ 2026-09-14.** Ce document recense les endroits où une même logique est **dupliquée** entre le pipeline AG1 (n8n) et le **dashboard** (`app.py`). Toute modification d'un côté DOIT être répercutée de l'autre, sinon les vues divergent silencieusement de la réalité d'exécution (c'est arrivé pour le funnel et la matrice).

## ⚠️ Règle d'or
Le **dashboard `app.py` réimplémente le scoring et les gates d'AG1** (il ne lit PAS la sortie du run — il recalcule à partir des bases DuckDB `ag2_v3` / `ag3_v2` / `ag4_*`). Donc :
> **Toute modif des formules de scoring, des règles de décision, des gates ou des seuils de fraîcheur dans AG1 doit être répercutée dans `app.py` (matrice + funnel), et inversement.**

La source du dashboard montée sur `/app` est actuellement
`/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard`.
Toujours revérifier le montage avant déploiement.

**Contrat technique du 14 septembre 2026 :** R8 lit `d1_confidence` (0–100),
jamais `d1_score` (−6–6) comme confiance. Des deux côtés, le terme technique
est `50 + direction * max(0, 8 + .2 * (confidence - 50))` : direction +1 BUY,
−1 SELL, 0 sinon, confiance bornée à 0–100. Les scores ne sont pas calibrés en
probabilités. Le test `test_technical_confidence_units.py` vérifie la symétrie
et la parité sur plusieurs confiances. [Contrat complet](20260914_performance_contract_remediation.md).

`execution_constraints` reste un alias BUY : les modèles reçoivent désormais
`buy_feasibility`/`sell_feasibility`. Le préflight sélectionne les entrées après
contrôle, conserve les positions détenues et expose les lots. Les sorties
proposées ne financent pas les achats. Ces contrôles d’exécution ne sont pas
répliqués dans la matrice du dashboard.

## Carte des duplications

| Logique | Côté AG1 (n8n, source d'exécution) | Côté dashboard (`app.py`, réimplémentation) |
|---|---|---|
| **Scoring matrice** (Risk, Reward, R, p_win, EV(R)) | node `Calcul Matrice & Briefing` | fonction de calcul matrice (≈ L4900-5045) — **Vue consolidée Multi-Agents** (nuage Risk/Reward) |
| **prob_score** (poids) | `0.36 tech + 0.34 funda + 0.20 news + 0.10 régime` | **idem** (doit rester identique) |
| **risk_score V2** (2026-06-30) | poids tactiques `vol 0.26 / liq 0.18 / event 0.16 / funda 0.16 / news 0.10 / conc 0.09 / options 0.05` + **renorm sur composantes observées** (exclut les défauts) | **idem** (mêmes poids + même renorm, flags `funda_usable`/`news_count`/earnings/spread/IV) |
| **Règle décision** `enter_core` / `reduce_core` | `Calcul Matrice` (`ev_r≥0.20 ET reward≥seuil ET risk≤seuil ET grade∈{A,B}` ; reduce si ev_r<0 …) | **idem** (≈ L5111-5140) |
| **Grades A/B/C** | quantiles dynamiques sur `prob_score_for_grade` | idem (seuils p-quantile recalculés) |
| **Gates matrice** (data_quality, earnings≤7j, liquidité `liq_risk≥85`, rr_outlier, options, données techniques/YF dures) | `Calcul Matrice` : `MISSING_TECH`, `TECH_BARS_NOT_CLOSED`, `TECH_STATUS_NOT_OK`, `STALE_H1`, `STALE_D1`, `MISSING_YF`, `STALE_YF` interdisent `Entrer / Renforcer` | idem dans `_build_multi_agent_matrix` ; `HARD_DATA_GATE` force `Surveiller` |
| **Funnel tradabilité** (System Health) | implicite (R8 + preflight) | recalcul dans `app.py` (`spread_exploitable`, `tech_gate_ready`, etc.) |
| **Seuils de fraîcheur** | H1≤96h, D1≤96h, YF≤72h, funda≤168h (R8) | **idem** (funnel + matrice) |
| **Contrat barres AG2** | H1/D1 `status=OK`, `closed_only=true`, OHLCV validé ; D1 après close place +10 min | **idem** (funnel + matrice) |
| **`data_age` = max(stocké, âge réel = now − date du dernier bar)** | R8 (`R8 — Data Prep for Matrix`) | **idem** (`h1_age_hours_effective`/`d1_age_hours_effective`) — ne PAS revenir au `data_age` figé |

Depuis le 2026-08-06, la fenêtre AG2 de rappel LLM et le gate de données sont
explicitement séparés. `SOFT_STALE` signifie « ne pas rappeler le LLM » et ne
doit jamais écraser `h1_status=OK`. Le blocage technique autoritatif reste
`closed_only=true`, statut de calcul `OK` et âge effectif H1/D1 ≤96 h. Ne pas
réintroduire une fenêtre globale UTC de 3 h dans le statut consommé par R8 ou
le dashboard.

Depuis le 2026-08-13, le funnel de la page **Analyse technique** suit un contrat
global, monotone et indépendant des filtres graphiques : **univers configuré →
rotation AG2 active → technique prête AG1 → non bloquée par l'IA**. La dernière
étape signifie uniquement `ai_decision != REJECT` : `APPROVE` n'est pas requis
par AG1 et `SKIP`/absence d'appel reste neutre. Les directions D1 BUY/SELL et les
résultats des appels IA sont des métriques séparées, avec leurs dénominateurs
explicites. La fraîcheur ne doit plus être résumée par la seule barre la plus
récente ; afficher la couverture du gate et les âges P50/P90.

## Étapes du pipeline = ce que chaque vue montre (NE PAS confondre)
1. **Étape décision matrice** (`Calcul Matrice` / nuage dashboard + funnel) : scoring + gates basés sur les données **yfinance/AG2/AG3/AG4**. C'est ce que le dashboard affiche.
2. **Étape exécution / preflight** (`AG1.V4 — Liquidity Preflight` + node 7 safety) : verdict liquidité **autoritatif basé IBKR** (snapshot bid/ask), + contrat, permissions, cash. **NON reflété dans le dashboard.** C'est ici que vivent les tolérances liquidité (warm-up sous-lots, `SPREAD_UNQUOTED`, filet haut-volume ≥1M). Cf [[18-ag1-run-timing-us]] en mémoire.
> Conséquence : un symbole « Entrer » dans le dashboard repasse un **contrôle liquidité IBKR final au moment de l'ordre**. Le dashboard le rappelle déjà (« les contrôles IBKR … liquidité restent exécutés au moment de l'ordre »). Ne PAS chercher à dupliquer le verdict IBKR dans le dashboard (il n'a pas d'accès IBKR per-symbole).

Depuis le 2026-08-10, le contrat d'exécution AG1 est également explicite :

- `entry` et le prix limite restent exprimés dans la devise native du contrat ;
- `currency`, `fx_rate_to_eur` et `price_eur` viennent du préflight IBKR et le
  sizing/les limites de risque sont calculés en EUR ; sans taux de change, un
  BUY/INCREASE est bloqué par `FX_RATE_UNAVAILABLE` ;
- `execution_constraints` expose avant les LLM le ticket minimal faisable et
  les poids cibles min/max ; une proposition sous le minimum n'est pas un vote
  exécutable ;
- le broker relit `/iserver/contract/rules` juste avant un ordre LIMIT et
  arrondit le prix au pas actif sans détériorer la limite (BUY vers le bas,
  SELL vers le haut).

Ces règles d'exécution ne sont pas recalculées dans le dashboard, qui n'a pas
accès au contrat IBKR live. Seul le gate dur de qualité des données est dupliqué.

## Autres liens inter-systèmes à garder en tête
- **News par-symbole** : `ag4_spe_v2.news_analyzed` → R8 (impact + **texte top-3 vers le LLM**) → `Calcul Matrice` (`opportunity_pack.rows[].news` + `newsGeneratedAt`). Le dashboard n'utilise que l'**impact** (sentiment_prob), pas le texte.
- **Contexte global** : `global_context_v1` → endpoint compact
  `AG1_GLOBAL_CONTEXT_LLM_V2` → même objet attaché aux trois branches AG1. Le
  dashboard lit le snapshot V1 complet ; il ne recalcule pas la confiance, la
  fraîcheur ou les poids. Aucun score du contexte ne doit entrer dans la matrice,
  la safety ou les quantités tant que `advisory_only=true`.
- **Ledger ordres** : `core.orders` (AG1) mis à jour à l'exécution + **post-approbation** par le node `Update Ledger Status` du workflow `Order Approval Decide`. Écrire `ag1_v4_consensus.duckdb` **en duckdb 1.4.3** (lecteur task-runner n8n).
- **Univers** : un symbole n'est traité par AG2/AG3 que s'il est dans `ag2_v3.universe_segments` (pas seulement `universe`). Cf `AGENTS.md` § classification.
- **Ordonnancement / contention DuckDB** : cf `SCHEDULING_AND_LOAD.md` (crons, durées, déconfliction).

## Checklist avant de modifier le scoring/les gates AG1
1. Modifier le node n8n (`Calcul Matrice`, `R8`, preflight, safety).
2. Répercuter la même formule/seuil dans `/opt/trading-dashboard/app/app.py` (matrice ≈ L4900-5175 ET funnel System Health).
3. Vérifier la cohérence : un même symbole doit donner le même grade/décision dans le dashboard et dans le run (`core.model_proposals` / `opportunity_pack`).
4. Committer ensemble le workflow AG1, `services/dashboard/app.py` et la
   documentation de parité lorsque la modification touche une logique partagée.
