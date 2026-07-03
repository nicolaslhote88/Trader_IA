# Audit AG3-V2 — Fundamental Analyst (pilier fondamental)

**Date** : 2026-06-22
**Auteur** : Claude (Cowork)
**Périmètre** : workflow n8n `AG3-V2 - Fundamental Analyst (DuckDB-first)`, base `ag3_v2.duckdb`, consommation par AG1 V4 et AG2-V3, disponibilité des données fondamentales (yfinance vs IBKR vs alternatives).
**Méthode** : reprise du format validé sur AG4_Spé-V2 (`20260617_ag4_spe_v2_analysis.md`) et AG2-V3 (`20260619_ag2_v3_analyse_pertinence_efficience.md`) — 3 axes Efficience / Efficacité / Pertinence, vérification live sur VPS, plan de remédiation en sprints.

> Convention : 🟢 OK · 🟠 à améliorer · 🔴 problème sérieux.
> Distinction explicite : **[FAIT]** vérifié · **[HYP]** hypothèse · **[DÉCISION]** · **[ACTION]**.

---

## 0. TL;DR (synthèse exécutive)

AG3-V2 n'est **pas** un pilier mort : ses sorties pèsent **lourd** dans le scoring d'AG1 V4 (funda = 0,34 de la probabilité, 0,30 du risque, 0,22 du reward) et dans la sélection `CORE_AUTO` d'AG2 (0,40 du score de priorité). **C'est précisément ce qui rend ses faiblesses dangereuses** : un pilier très influent alimenté par une source unique (yfinance) à couverture structurellement faible sur l'univers réel.

Les trois constats majeurs :

1. **Pertinence 🔴 — fraîcheur** : rotation par batch de 50 symboles/run sur tout l'univers `enabled` (385, quarantaine **incluse**). Âge moyen de la dernière donnée par symbole = **15,7 jours**, max = **112 jours**. Un fondamental vieux de 2 semaines à 3 mois pèse 30-34 % du score AG1 du jour.
2. **Efficacité 🟠 — couverture analystes** : **40 % de l'univers (169/424) sans aucune donnée analyste/target/reco**. Ce sont quasi exclusivement des nano/small caps françaises (Euronext Growth : ATARI, EUROPACORP, CAFOM, EGIDE…). 90 % de l'univers est classé `WATCH` (aucun edge fondamental détecté).
3. **Efficience 🟠 — gaspillage quarantaine** : AG3 score les **131 symboles en quarantaine** (34 % du budget batch) qui ne seront jamais tradés par AG1. Les exclure réduirait le cycle de rafraîchissement complet de **8 → 6 runs**.

**Disponibilité données** : **IBKR ne peut PAS remplacer yfinance.** L'API Client Portal (celle déployée) a **déprécié les tags fondamentaux** (P/E, EPS, market cap, dividende, beta) et **n'expose pas les notes analystes**. Le trou « analystes » sur les small caps FR est **structurel** (aucun broker/data-provider ne couvre ces valeurs — il n'y a pas de sell-side dessus), donc changer de source ne le comblera pas.

**Recommandation de cap** : ne pas réécrire AG3, mais le **recadrer** : (a) exclure la quarantaine + accélérer la rotation sur l'univers actif, (b) durcir la robustesse d'AG1 au fondamental manquant/périmé (flag d'âge), (c) enrichir les *financials reportés* (marges, dette, FCF — disponibles partout) plutôt que de chasser des consensus analystes inexistants.

---

## 1. Cartographie du pilier (FAIT)

### 1.1 Topologie workflow
`Schedule (cron live 05:00 UTC, lun-ven)` → `00 Init Context` → `01 Read Universe` (DuckDB `ag2_v3.duckdb.universe`) → `01 Build Queue` (JS) → `06 DuckDB Init` (schéma + **batch_state**, 50/run) → boucle `Split In Batches` → `04 HTTP Fundamentals` (`yfinance-api:8080/fundamentals`) → `02 Score` (JS) → `03/04/05 Prepare rows` → `07 Write DuckDB` → (done) `08 Finalize Run`.

- **Source de données unique** : `yfinance-api` (`GET /fundamentals?symbol=`). Aucune autre source. **[FAIT]**
- **Zéro LLM** : tout le scoring est déterministe (JS `02_score_fundamentals.js`). L'axe « efficience LLM » des autres audits est ici **sans objet** ; on parle d'efficience compute/API. **[FAIT]**
- **Univers runtime** : depuis 2026-06-14, lecture de `ag2_v3.duckdb.main.universe` (plus de Google Sheets). Filtre = `enabled = TRUE` uniquement ; classe d'actif `EQUITY` retenue dans `01_build_queue.js` (ETF/CRYPTO/FX écartés côté queue). **[FAIT]**

### 1.2 Logique de scoring (`02_score_fundamentals.js`)
5 sous-scores pondérés 0-100 → `triageScore` + `riskScore` + `horizon` :

| Sous-score | Entrées yfinance | Poids dans triage |
|---|---|---|
| quality | marges brute/op/nette, ROE, ROA | 0,32 |
| growth | revenue/earnings/earnings-Q growth | 0,20 |
| valuation | trailing/forward PE, P/B, PEG, upside | 0,20 |
| health | debt/equity, current/quick ratio, FCF yield | 0,18 |
| consensus | recommendationMean, nb analystes, upside, dispersion | 0,10 |

- `horizon` : `LONG_TERM` si triage≥72 & risk≤45 ; `SWING` si triage≥58 & risk≤62 ; sinon `WATCH`.
- Pénalité `-8` si `data_coverage_pct < 35` ; triage plafonné à 40 si `ok=false`.

### 1.3 Schéma DuckDB (`ag3_v2.duckdb`)
Tables : `fundamentals_snapshot` (payload brut JSON par bloc), `fundamentals_triage_history`, `analyst_consensus_history`, `fundamental_metrics_history` (long-format), `run_log`, `batch_state`. Vues `v_latest_triage` / `v_latest_consensus` (dernier par symbole). Idempotence par `RecordId = sha1(run_id|type|symbol)`. Schéma propre et traçable 🟢.

---

## 2. Qui consomme AG3 et avec quel poids (FAIT — point central de valeur)

AG3 a **deux** consommateurs réels. Le pilier n'est donc pas isolé.

### 2.1 AG1 V4 Consensus — `R8_data_prep_matrix.code.py`
Lit `v_latest_triage` : `score`, `risk_score`, `upside_pct`, `recommendation`, `target_price`, `horizon`. Émet dans la matrice par symbole : `Funda_Score` (défaut **50** si absent), `Funda_Risk` (défaut **50**), `Funda_Upside` (défaut 0), `Recommendation`, `Target_Price`, `Funda_Horizon`, flag `MISSING_FUNDA`.

Poids réel dans `calcul_matrice_briefing.code.py` (le scoring qui nourrit le PM) :

| Composante AG1 | Terme fondamental | Poids |
|---|---|---|
| `prob_score` | `funda_prob = 0,7·funda_score + 0,3·(100−funda_risk)` | **0,34** (plus gros contributeur) |
| `risk_score_100` | `funda_risk` | **0,30** (plus gros contributeur) |
| `reward_score_100` | `reward_upside = funda_upside·3` (capé) | **0,22** |
| Target price | `Target_Price` utilisé pour calcul TP candidat | direct |

➡️ **[FAIT] AG3 est un pilier à très forte influence** sur la décision AG1. Quand la donnée manque, les défauts (50/50/0) tirent tout vers la moyenne — neutre mais non informatif, et masqué seulement par le flag `MISSING_FUNDA` (qui n'exclut pas le symbole).

### 2.2 AG2-V3 node 12 — `universe_quarantine_audit.py`
`refresh_universe_segments` lit `ag3.main.v_latest_triage` (`score AS funda_score`, `risk_score`, `quality_score`, `health_score`, `analyst_count`) pour le **priority_score** de sélection `CORE_AUTO` (top 50) :

```
priority = volume_score(≤40) + funda_score·0,40 + quality·0,12 + health·0,08 + analyst·0,35(≤8) − risk·0,18
```

- **[FAIT] AG3 ne pilote PAS la quarantaine.** `decision_for_symbol` ne dépend que de tech_runs / quote yfinance / volume. Le fondamental n'entre que dans le **classement CORE_AUTO** (quels 50 symboles sont « cœur »).
- Pas de bug de nom de colonne (l'alias `score AS funda_score` est correct). 🟢

---

## 3. État live vérifié sur VPS (FAIT — 2026-06-22 06:00 UTC)

> Source : `ag3_v2.duckdb` et `ag2_v3.duckdb` interrogées en lecture seule via conteneur `python:3.11-slim` (`--volumes-from root-task-runners-3:ro`).

### 3.1 Exécution
- Workflow **actif**, cron **live 05:00 UTC** lun-ven. ⚠️ **Écart repo/live** : l'export `AG3-V2-workflow.json` indique `0 7 * * 1-5` (07:00) alors que les runs réels tombent à 05:00. **[ACTION]** réaligner repo↔live.
- Derniers runs : 22/06 SUCCESS 50/50 · 19/06 **PARTIAL 30/35** (5 erreurs) · 18/06→11/06 SUCCESS 50/50.
- Run = ~13-19 min pour 50 symboles. 🟢 stable.

### 3.2 Volumétrie & fraîcheur 🔴
- `fundamentals_triage_history` : 4 903 lignes, **424 symboles distincts**.
- **Âge de la dernière donnée par symbole** : moyenne **375,8 h ≈ 15,7 j**, min 1 h, **max 2 688 h ≈ 112 j**.
- Cause : batch 50/run sur tout l'`enabled` (385) ⇒ cycle complet = **8 sessions** ; chaque symbole n'est revu qu'~1×/8 jours ouvrés. Le `riskScore`/`funda_score` lu par AG1 *aujourd'hui* peut dater de 2 semaines à 3,5 mois.

### 3.3 Qualité de couverture 🟠
- Statut : **418 OK / 6 ERR_SOURCE**.
- `data_coverage_pct` : moyenne 80 %, **50 symboles (12 %) < 35 %** (pénalité), 346 (82 %) ≥ 70 %, 6 NULL.
- **Consensus analystes : 169/424 (40 %) sans analyste, 169 sans target price, 55 sans recommandation.**
- Horizon : **382 WATCH (90 %)**, 34 SWING, 8 LONG_TERM ⇒ AG3 ne détecte un edge fondamental que sur ~10 % de l'univers.

### 3.4 Univers & quarantaine (définition du périmètre « actif »)
- Univers `enabled` (EQUITY/ETF, hors `=X`) = **385** (382 EQUITY + 3 ETF).
- **Quarantaine active = 131** (118 `LOW_VOLUME_30D`, 12 `TECH_DATA_UNUSABLE`, 1 `QUOTE_UNUSABLE`).
- ➡️ **Univers actif hors quarantaine = 254 symboles** (segments : 50 CORE_AUTO, 198 WATCHLIST, 6 HELD).
- Géographie : **343/385 = 89 % France**, 17 US, reste UE diffus.
- **[FAIT] AG3 ne filtre pas la quarantaine** (`01 Read Universe` ne lit que `enabled`, aucun JOIN `universe_quarantine`). Les 131 quarantainés sont scorés inutilement ⇒ **34 % du budget batch gaspillé**.

---

## 4. Disponibilité des données fondamentales (FAIT + analyse)

Question posée : peut-on faire mieux que yfinance avec IBKR ou une autre source fiable, sur tout l'univers actif ?

### 4.1 yfinance (source actuelle)
- ✅ Déjà intégré (microservice `yfinance-api`), gratuit, profil + financials reportés + consensus *quand Yahoo l'a*.
- ❌ Consensus analystes absent pour ~40 % de l'univers (small caps FR), 12 % à très faible couverture.

### 4.2 IBKR — **ne convient pas** pour les fondamentaux
- **[FAIT] Session live DOWN au moment de l'audit** : broker `/health` → `authenticated:false`, `CPAPI HTTP 401`, dernier tickle OK le 19/06 (tombée le week-end). Fragilité connue (relogin 2FA), déjà documentée pour AG4-V3.
- **[FAIT] Le broker n'expose aucun endpoint fondamental** : routes = `/marketdata/fx/snapshot`, `/orders/*`, `/fills`, `/news/portfolio`. Le snapshot ne demande que les champs 31/84/86 (last/bid/ask).
- **[FAIT] IBKR référence bien les small caps FR** comme contrats (ATARI/`ALATA` conid 869747880 SBF, EGIDE/`ALGID` 11715748, Legrand/`LR` 38715873) — mais avoir le contrat ≠ avoir le fondamental.
- **[FAIT — web, IBKR Campus] L'API Client Portal a *déprécié* les tags fondamentaux** (P/E, EPS, market cap, dividende, beta) et **les notes analystes ne sont pas disponibles via API**. Le seul accès fondamental IBKR est `reqFundamentalData`/ticker 258 (états financiers Reuters en XML) **via TWS API** — *pas* le Client Portal déployé, et **sans estimations analystes**.

➡️ **Conclusion : IBKR ne peut pas alimenter AG3 en l'état.** Au mieux, un chantier séparé TWS API/Reuters apporterait des *financials reportés* (revenus, marges, bilan) — pas le consensus.

### 4.3 Le trou « analystes » est structurel, pas un défaut de source
Les 169 symboles sans analyste sont des nano/small caps Euronext Growth Paris (ATARI, EUROPACORP, CAFOM, EGIDE, HF COMPANY, LEBON…). **Aucun sell-side ne les couvre** → aucun fournisseur (yfinance, IBKR/Refinitiv, Bloomberg) n'a de target/reco dessus. Changer de source ne créera pas une donnée qui n'existe pas. (Bigdata.com MCP non testable : souscription expirée.)

### 4.4 Ce qu'une meilleure source *pourrait* réellement apporter
Uniquement sur les **financials reportés** (marges, dette/équité, FCF, croissance) issus des comptes publiés — disponibles pour quasiment toutes les sociétés cotées. Candidats intégrables (pattern microservice HTTP comme `yfinance-api`, le sandbox n8n ne pouvant pas faire d'appels réseau lui-même) : **Financial Modeling Prep**, **Refinitiv/Worldscope via TWS**, **simfin/EOD Historical**. **[HYP]** à valider couverture FR + coût. Gain attendu : meilleure fiabilité de `quality/growth/health` ; **pas** de gain sur `consensus`.

---

## 5. Synthèse par axe

| Axe | Note | Justification |
|---|---|---|
| **Efficience** | 🟠 | Zéro LLM (bon), mais 34 % du budget batch sur des quarantainés ; cycle de rafraîchissement 8 jours. |
| **Efficacité** | 🟠 | Forte influence sur AG1 (0,30-0,34) mais 40 % sans consensus, 90 % en WATCH ⇒ apporte un edge réel sur une minorité de symboles. |
| **Pertinence** | 🔴 | Fraîcheur : donnée moyenne de 15,7 j (max 112 j) pesant 1/3 du score AG1 du jour. Risque de décisions sur fondamentaux périmés. |

---

## 6. Plan de remédiation (sprints)

### Sprint 1 — Recadrage & robustesse (faible risque, fort ROI) — **prioritaire**
- **S1-A [ACTION]** Exclure la quarantaine de la file AG3 : dans `01 Read Universe`, `LEFT JOIN universe_quarantine q … WHERE COALESCE(q.active,FALSE)=FALSE`. Effet : cycle 8→6 runs, +33 % de fraîcheur sur l'univers utile. *Vigilance : garder les HELD même si quarantainés (cohérent avec AG1 R8 qui ne skippe un quarantainé que s'il n'est pas détenu).*
- **S1-B [ACTION]** Prioriser la rotation : passer CORE_AUTO + HELD en tête de file (comme AG4_Spé l'a fait pour le portefeuille) pour que les 50-56 symboles réellement tradés soient les plus frais. Réutiliser `batch_state` mais réordonner la queue.
- **S1-C [ACTION]** Exposer la fraîcheur à AG1 : ajouter un flag `STALE_FUNDA` dans `R8_data_prep_matrix` (ex. `funda_age_h > 240`) symétrique à `STALE_YF`, et le rendre visible au PM. *Décision ouverte : faut-il neutraliser (poids→0) le fondamental périmé plutôt que d'appliquer 50/50 par défaut ?*
- **S1-D [ACTION]** Réaligner repo↔live (cron 05:00 réel vs 07:00 dans le JSON) et committer.

### Sprint 2 — Couverture & qualité (moyen)
- **S2-A [ACTION]** Augmenter `BATCH_SIZE` (50→~85) pour viser un cycle ≤ 3 jours sur les 254 actifs, si la durée run reste raisonnable (~13 min/50 ⇒ ~22 min/85). À mesurer.
- **S2-B [ACTION]** Traiter les 6 `ERR_SOURCE` persistants + `ALLEC.PA` (coverage 3,8 %, pays NULL = ticker mort) : audit ciblé, désactiver/quarantainer les tickers morts plutôt que les re-scorer chaque cycle.
- **S2-C [HYP]** POC source de *financials reportés* complémentaire (FMP ou équivalent) en microservice HTTP, fusion conservatrice (yfinance prioritaire, complément si champ manquant). Ne cible **pas** le consensus.

### Sprint 3 — Valeur analytique (optionnel)
- **S3-A** Revoir les seuils `horizon` (90 % WATCH suggère un calibrage trop strict ou une inadéquation small-cap) — analyser la distribution des sous-scores avant de retoucher.
- **S3-B** Digest fondamental qualitatif au PM (à l'image de D2/AG4) : top forces/risques par symbole détenu, si jugé utile après mesure du coût tokens.

---

## 7. Décisions à trancher par Nicolas

1. **[DÉCISION]** Lancer Sprint 1 (recadrage + robustesse) ? Faible risque, aucun changement de source. *Recommandé.*
2. **[DÉCISION]** Politique sur fondamental périmé dans AG1 : laisser 50/50 par défaut, ou neutraliser le poids quand `STALE_FUNDA` ?
3. **[DÉCISION]** Investir dans une source de financials reportés complémentaire (Sprint 2-C), sachant qu'elle **ne** comblera **pas** le trou analystes sur les small caps FR ?
4. **[DÉCISION]** Abandonner définitivement la piste « IBKR pour fondamentaux » (confirmé non viable via Client Portal) ?

---

## Annexe — Sources & commandes de vérification

- Code : `agents/trading-actions/AG3 - Les fondamentaux/AG3-V2/` (`nodes/02_score_fundamentals.js`, `06_duckdb_init.py`, `AG3-V2-workflow.json`).
- Consommateurs : `AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/R8_data_prep_matrix.code.py` + `calcul_matrice_briefing.code.py` ; `AG2-V3/nodes/12_universe_quarantine_audit.py`.
- Inspection live (lecture seule) :
  ```bash
  ssh -i .ssh/codex_vps_tailscale_ed25519 root@82.112.242.251 \
   "docker run --rm --volumes-from root-task-runners-3:ro -v /tmp:/host_tmp python:3.11-slim \
    bash -lc 'pip install -q duckdb && python3 /host_tmp/ag3_probe.py'"
  ```
- IBKR : broker `/health` (401 au 22/06), Web API Client Portal — fondamentaux dépréciés / notes analystes indisponibles (IBKR Campus).
