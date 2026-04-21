# État des lieux fonctionnel — Trader_IA

**Dernière analyse exhaustive : 2026-03-02**
**Périmètre** : repository `Trader_IA` + configuration VPS `vps_hostinger_config/docker-compose.yml`.
**Objectif** : fournir une base d'entrée claire et opérationnelle pour un mode projet LLM.

> ℹ️ Pour les **issues/écarts connus** (avec statut résolu / en cours / à faire), voir le document séparé [`historique_issues.md`](historique_issues.md).

## 1. Résumé exécutif

Le projet est une plateforme multi-agents de trading organisée autour de :

- `n8n` (orchestration des workflows AG0/AG1/AG2/AG3/AG4/YF enrichment),
- `DuckDB` (source of truth analytique et exécution),
- `Qdrant` (RAG/vector search),
- `yfinance-api` (accès Yahoo avec cache/cooldown robuste),
- `trading-dashboard` (Streamlit, vue opérationnelle multi-agents),
- `Traefik` (reverse proxy TLS).

Le système est déjà en mode "DuckDB-first" sur AG2/AG3/AG4/AG4-SPE et AG1-V3.

Points structurants observés :

- coexistence de versions V2/V3 dans les paths/environnements,
- forte interdépendance AG1 ← AG2/AG3/AG4/YF,
- ensemble AG1 en 3 modèles parallèles (GPT-5.2 / Grok-4.1 / Gemini-3), chacun avec sa DuckDB.

## 2. Services déployés sur le VPS

Source : `vps_hostinger_config/docker-compose.yml` + `vps_hostinger_config/docker-compose.qdrant.yml`.

### 2.1 Catalogue des services

| Service | Rôle principal | Exposition | Dépendances |
|---|---|---|---|
| `traefik` | Reverse proxy TLS + routage host-based | `80`, `443` publics | Docker provider |
| `n8n` | Orchestrateur workflows | `127.0.0.1:5678` (proxyfié via Traefik) | `yfinance-api` |
| `task-runners` (x3 replicas) | Runners externes n8n (Python/JS) | interne réseau Docker | `n8n` |
| `yfinance-api` | API marché (history/quote/options/calendar/fundamentals) avec cache disque | interne réseau Docker | aucune |
| `yf-enrichment` | Microservice FastAPI qui lance `daily_enrichment.py` | interne réseau Docker (`:8081`) | `yfinance-api` |
| `trading-dashboard` | App Streamlit (dashboard) | proxyfié Traefik (`${DASHBOARD_DOMAIN}`) | sources DuckDB + yfinance-api |
| `qdrant` (stack séparée) | Vector DB (RAG) | `127.0.0.1:6333/6334` | aucune |
| `toolbox` | Container utilitaire debug (`curl`, `jq`) | interne | aucune |

### 2.2 Paramètres d'architecture importants

- Réseaux : `web` (externe) et `traefik_proxy` (externe).
- Stockage persistant :
  - `n8n_data`, `traefik_data`, `qdrant_data`, `yfinance_data`, `runner_pydeps` en volumes externes.
  - partage cross-services via `/local-files` monté sur `/files`.
- `n8n` tourne en mode runners externes :
  - `N8N_RUNNERS_ENABLED=true`
  - broker `:5679`
  - `task-runners` en parallélisme (3 replicas).
- `qdrant` sécurisé par API key (`QDRANT__SERVICE__API_KEY`).
- Dashboard protégé par BasicAuth Traefik.

### 2.3 Flux inter-services (fonctionnel)

1. `n8n` orchestre les workflows.
2. AG2/AG3/AG1-PF/YF enrichment interrogent `yfinance-api`.
3. AG2/AG3/AG4-SPE vectorisent dans `qdrant`.
4. AG1-V3 lit DuckDB + outils RAG Qdrant, puis écrit ledger AG1.
5. `trading-dashboard` lit majoritairement DuckDB et appelle `yfinance-api` pour certains graphes/snapshots.

## 3. Workflows et rôle métier

### 3.1 AG0 — Extraction universe

- Fichier : `AG0-V1 - extraction universe/AG0-V1 - extraction universe.json`
- Trigger : manuel.
- Rôle :
  - scrape Boursorama compartiments A/B/C,
  - normalise `Symbol` (`<ticker>.PA`) + `Name`,
  - export CSV + XLSX vers Google Drive.
- Usage : alimentation universe (amont manuel).

### 3.2 AG1-PF-V1 — Portfolio MTM (DuckDB-only, multi AG1-V2)

- Fichier : `AG1-PF-V1/AG1-PF-V1-workflow.json`
- Trigger :
  - schedule `0 0 9-17 * * 1-5`
  - manuel.
- Sources :
  - bases AG1 `ag1_v2_*` (lecture positions),
  - `yfinance-api /history` (1H + 1D),
  - optional enrichment Universe via AG2 DB.
- Traitement :
  - normalisation lignes portefeuille,
  - fetch prix H1/D1,
  - choix meilleur prix (freshest/fallback),
  - calcul MTM (`LastPrice`, `MarketValue`, `UnrealizedPnL`),
  - écriture DuckDB latest/history/run_log.
- Sorties :
  - tables `portfolio_positions_mtm_latest`, `portfolio_positions_mtm_history`, `portfolio_positions_mtm_run_log`.

### 3.3 AG1-V3 — Portfolio Manager (3 variants modèles)

- Fichiers :
  - template : `AG1-V3-Portfolio manager/workflow/AG1_workflow_template_v3.json`
  - variants : `.../variants/AG1_workflow_v3__chatgpt52.json`, `...grok41_reasoning.json`, `...gemini30_pro.json`.
- Trigger (variant ChatGPT 5.2) : `0 15 9 * * 1-5`, `0 30 12 * * 1-5`, `0 45 16 * * 1-5`.
- Rôle métier :
  - construit contexte portefeuille + marché (multi-agent pack),
  - appelle agent LLM Portfolio Manager,
  - applique garde-fous d'exécution (`Validate & Enforce Safety`),
  - produit bundle d'ordres/fills/signaux/alertes,
  - upsert dans ledger AG1 DuckDB (`core.*` + `cfg.*`),
  - calcule snapshots + health post-run.
- RAG utilisé par agent :
  - `financial_news_v3_clean` (news),
  - `fundamental_analysis` (fonda),
  - `financial_tech_v1` (tech).
- Note : legacy branches Google Sheets conservées mais désactivées dans variants exportés.

### 3.4 AG2-V3 — Analyse technique

- Fichiers : `AG2-V3/AG2-V3 - Analyse technique (FX only).json` + `AG2-V3/AG2-V3 - Analyse technique (non-FX).json`
  - ces deux variants sont les sources de vérité (l'ancien canonique `AG2-V3 - Analyse technique.json` a été retiré du repo en avril 2026).
- Trigger :
  - cron `10 9-17 * * 1-5`
  - manuel.
- Sources :
  - Universe (Google Sheets),
  - `yfinance-api /history` (1H et 1D),
  - LLM validation (route FX vs Equity/ETF),
  - Qdrant collection `financial_tech_v1`.
- Pipeline fonctionnel :
  1. init config + batch rotation,
  2. init schema DuckDB,
  3. loop symboles,
  4. calcul indicateurs techniques (H1/D1),
  5. pré-filtres PM + dedup AI cache,
  6. validation IA (prompts différenciés FX vs actions),
  7. write `technical_signals`,
  8. finalize run + optional sync sheets,
  9. build vector docs (`VectorDoc_v2`), delete-by-doc_id, upsert Qdrant, mark vectorized.
- Sorties principales :
  - table `technical_signals` + vues `v_latest_signals`, `v_ag1_summary`, `v_ag2_fx_output`.

### 3.5 AG3-V2 — Analyse fondamentale

- Fichier : `AG3-V2/AG3-V2-workflow.json`
- Trigger :
  - schedule `0 7 * * 1-5`
  - manuel.
- Sources :
  - Universe (Google Sheets),
  - `yfinance-api /fundamentals`,
  - Qdrant collection `fundamental_analysis`.
- Pipeline :
  1. init contexte + queue,
  2. init schema + run (DuckDB),
  3. fetch fondamentaux par symbole,
  4. scoring (quality/growth/valuation/health/consensus/risk),
  5. écriture triage/consensus/metrics/snapshot,
  6. finalize run,
  7. vector docs + delete/upsert + mark vectorized.

### 3.6 AG4-V3 — Macro & News

- Fichier : `AG4-V3/AG4-V3-workflow.json`
- Trigger :
  - schedule `*/30 7-20 * * 1-5`
  - manuel.
- Sources :
  - Google Sheets `Source_RSS`,
  - Google Sheets `Universe`.
- Pipeline :
  1. chargement flux RSS + dictionnaire symboles/secteurs,
  2. init schema DuckDB + run log,
  3. lecture index historique,
  4. normalisation RSS, tagging symboles, dedupe clustering,
  5. pré-score + routage new/seen + analyse IA news,
  6. écriture `news_history` / `news_errors`,
  7. finalize run et génération vues FX macro (`ag4_fx_macro`, `ag4_fx_pairs`).
- Rôle : fournir régime macro, thèmes, secteurs/currencies bullish-bearish.

### 3.7 AG4-SPE-V2 — News spécifiques par valeur

- Fichier : `AG4-SPE-V2/AG4-SPE-V2-workflow.json` (régénéré depuis `build_workflow.py` — ≈ 112 KB).
- Source de vérité : `AG4-SPE-V2/build_workflow.py` + `AG4-SPE-V2/nodes/*`.
- Trigger (dans `build_workflow.py`) :
  - `0 5 9,12,15 * * 1-5` + manuel.
- Sources :
  - Universe Google Sheets,
  - Boursorama listing pages + article pages.
- Pipeline :
  1. init DB + queue rotative symboles (`workflow_state`),
  2. scrape listing actualités par symbole,
  3. extraction URLs + normalisation + dedupe (`news_id=sha1(symbol|canonical_url)`),
  4. routage new vs seen,
  5. fetch article + parsing,
  6. préparation prompt + analyse OpenAI (schema JSON strict),
  7. upsert `news_history`, write `news_errors`,
  8. finalize run,
  9. vector docs Qdrant (`financial_news_v3_clean`) + mark vectorized.

### 3.8 YF-ENRICH-V1 — Enrichissement quotidien marché

- Workflow : `yf-enrichment-v1/YF-ENRICH-V1-daily-workflow.json`
- Trigger :
  - schedule `15 6 * * *`
  - manuel.
- Exécution :
  - n8n fait `POST http://yf-enrichment:8081/run`,
  - service `yf-enrichment` lance `daily_enrichment.py`.
- Sources :
  - symboles de `ag2` table `universe` (ou argument `--symbols`),
  - `yfinance-api` endpoints `/quote`, `/options`, `/calendar`.
- Sortie :
  - DuckDB `yf_enrichment_v1.duckdb` (`run_log`, `yf_symbol_enrichment_history`, `v_latest_symbol_enrichment`).

## 4. Sources de données (détail)

### 4.1 Sources externes

| Source | Type | Consommateurs |
|---|---|---|
| Yahoo Finance (via `yfinance` python) | Marché (OHLCV, quote L1, options, earnings, fundamentals) | `yfinance-api`, puis AG1/AG2/AG3/AG1-PF/YF enrichment/dashboard |
| Boursorama cotations | Universe actions FR | AG0 |
| Boursorama actualités par valeur | News symboles | AG4-SPE-V2 |
| Flux RSS (liste en Sheet `Source_RSS`) | News macro | AG4-V3 |
| OpenAI API | LLM analyse news/agent + embeddings | AG1-V3, AG4-SPE-V2, vectorisation |
| Google Sheets | Configuration/source universe/rss | AG0, AG2, AG3, AG4, AG4-SPE, dashboard fallback |

### 4.2 Sources internes (data products)

| Source interne | Produit par | Consommé par |
|---|---|---|
| `ag2_v3.duckdb` | AG2 | AG1-V3, dashboard, YF enrichment (universe) |
| `ag3_v2.duckdb` | AG3 | AG1-V3, dashboard |
| `ag4_v3.duckdb` | AG4 macro | AG1-V3, dashboard |
| `ag4_spe_v2.duckdb` | AG4-SPE | AG1-V3, dashboard |
| `yf_enrichment_v1.duckdb` | YF enrichment | AG1-V3, dashboard |
| `ag1_v3*.duckdb` (×3 modèles) | AG1-V3 | dashboard, AG1-PF (selon config) |
| `ag1_v2*.duckdb` | AG1-V2/AG1-PF | dashboard legacy + compat |
| Qdrant collections | AG2/AG3/AG4-SPE vector docs | AG1-V3 tools (RAG) |

## 5. Bases de données générées et schémas

### 5.1 DuckDB AG1-PF (MTM) — `AG1-PF-V1/sql/schema.sql`

Tables :
- `portfolio_positions_mtm_run_log`
- `portfolio_positions_mtm_latest`
- `portfolio_positions_mtm_history`
- vue `v_portfolio_positions_mtm_latest`

Colonnes clés :
- run lifecycle : `run_id`, `started_at`, `finished_at`, `status`, compteurs.
- positions latest/history : `symbol`, `quantity`, `avg_price`, `last_price`, `market_value`, `unrealized_pnl`, `updated_at`, `run_id`.

### 5.2 DuckDB AG1-V3 ledger — `AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql`

Schemas :
- `core`
- `cfg`

Tables `core` :
- `runs`, `instruments`, `market_prices`,
- `orders`, `fills`, `cash_ledger`,
- `position_lots`, `positions_snapshot`, `portfolio_snapshot`,
- `ai_signals`, `risk_metrics`, `alerts`, `backfill_queue`.

Tables `cfg` :
- `portfolio_config` (seeded avec `kill_switch_active=True`, `max_pos_pct=25`, `max_sector_pct=40`, `max_daily_drawdown_pct=6`).

Rôle :
- modèle ledger complet exécution + audit + risque + snapshots portefeuille.

### 5.3 DuckDB AG2-V3 — `AG2-V3/sql/schema.sql`

Tables :
- `universe`
- `technical_signals`
- `ai_dedup_cache`
- `run_log`
- `batch_state`

Vues :
- `v_latest_signals`
- `v_pending_vectors`
- `v_ag1_summary`
- `v_ag2_fx_output`

Schema notable `technical_signals` :
- identifiants/run : `id`, `run_id`, `symbol`, `symbol_internal`, `symbol_yahoo`, `asset_class`, `workflow_date`
- H1/D1 status/actions/scores/confidence
- indicateurs techniques complets (SMA/EMA/MACD/RSI/ATR/BB/Stoch/ADX/OBV/Support/Resistance)
- métadonnées FX (`base_ccy`, `quote_ccy`, `pip_size`, `atr_pips_*`)
- AI validation (`ai_decision`, `ai_quality`, `ai_alignment`, `ai_stop_loss`, `ai_rr_theoretical`, etc.)
- vector tracking (`vector_status`, `vector_id`, `vectorized_at`).

### 5.4 DuckDB AG3-V2 — `AG3-V2/nodes/06_duckdb_init.py`

Tables :
- `run_log`
- `fundamentals_snapshot`
- `fundamentals_triage_history`
- `analyst_consensus_history`
- `fundamental_metrics_history`
- `batch_state`

Vues :
- `v_latest_triage`
- `v_latest_consensus`

Colonnes métier :
- triage : `score`, `risk_score`, `quality_score`, `growth_score`, `valuation_score`, `health_score`, `consensus_score`, `horizon`, `upside_pct`, `recommendation`.
- consensus : targets mean/high/low + analyst count + dispersion/risk proxy.
- metrics : données atomiques section/metric/value/unit.

### 5.5 DuckDB AG4-V3 — `AG4-V3/nodes/12_duckdb_init.py`

Tables :
- `news_history`
- `news_errors`
- `run_log`
- `ag4_fx_macro`
- `ag4_fx_pairs`

Colonnes notables `news_history` :
- dedupe/event : `dedupe_key`, `event_key`, `canonical_url`
- contenu : `title`, `snippet`, `theme`, `regime`, `notes`
- impacts : `impact_score`, `confidence`, `urgency`, `action`
- tagging macro : `sectors_bullish`, `sectors_bearish`, `currencies_bullish`, `currencies_bearish`
- trace run : `run_id`, `analyzed_at`, `first_seen_at`, `last_seen_at`.

### 5.6 DuckDB AG4-SPE-V2 — `AG4-SPE-V2/nodes/00_duckdb_prepare.py`

Tables :
- `universe_symbols`
- `news_history`
- `news_errors`
- `run_log`
- `workflow_state`

Colonnes notables `news_history` :
- identité : `news_id`, `symbol`, `canonical_url`
- contenu : `title`, `snippet`, `text`, `summary`, `published_at`
- IA : `impact_score`, `sentiment`, `confidence_score`, `horizon`, `urgency`, `suggested_signal`, `key_drivers`, `needs_follow_up`, `is_relevant`
- vector : `vector_status`, `vector_id`, `vectorized_at`, `chunk_total`
- lifecycle : `first_seen_at`, `last_seen_at`, `analyzed_at`, `fetched_at`.

### 5.7 DuckDB YF enrichment — `yf-enrichment-v1/daily_enrichment.py`

Tables :
- `run_log`
- `yf_symbol_enrichment_history`

Vue :
- `v_latest_symbol_enrichment`

Colonnes notables :
- quote : `regular_market_price`, `bid`, `ask`, `spread_pct`, `slippage_proxy_pct`, `market_state`
- options : `iv_atm`, `skew_put_minus_call_5pct`, `put_call_oi_ratio`, `options_ok/options_error/options_warning`
- calendar : `next_earnings_date`, `days_to_earnings`, `calendar_ok/calendar_error`.

### 5.8 Qdrant (vector DB)

Collections observées :
- `financial_tech_v1` (AG2)
- `fundamental_analysis` (AG3)
- `financial_news_v3_clean` (AG4-SPE)

Convention metadata (`VectorDoc_v2`) :
- `doc_id` stable,
- `schema_version="VectorDoc_v2"`,
- `doc_kind` (`TECH`/`FUNDA`/`NEWS`),
- delete-by-filter `doc_id` avant upsert pour idempotence.

## 6. Dashboard — fonctionnement détaillé

Fichiers :
- `dashboard/app.py`
- `dashboard/app_modules/core.py`
- `dashboard/app_modules/tables.py`
- `dashboard/app_modules/visualizations.py`

### 6.1 Sources et connecteurs du dashboard

Variables/env lues :
- `DUCKDB_PATH` (AG2),
- `AG1_DUCKDB_PATH`,
- `AG1_CHATGPT52_DUCKDB_PATH`,
- `AG1_GROK41_REASONING_DUCKDB_PATH`,
- `AG1_GEMINI30_PRO_DUCKDB_PATH`,
- `AG3_DUCKDB_PATH`,
- `AG4_DUCKDB_PATH`,
- `AG4_SPE_DUCKDB_PATH`,
- `YF_ENRICH_DUCKDB_PATH`,
- `YFINANCE_API_URL`,
- `SHEET_ID`, credentials Google.

Chargements data principaux :
- Google Sheets fallback/metadata (`load_data`),
- DuckDB read-only avec cache signatures fichiers (`duckdb_file_signature`, `_read_duckdb_df`),
- loaders par domaine (`load_ag2_overview`, `load_ag3_overview`, `load_ag4_*`, `load_yf_enrichment_latest`),
- loaders pages composites (`load_system_health_page_data`, `load_multi_agent_page_data`, etc.).

### 6.2 Navigation fonctionnelle (6 pages)

1. `Dashboard Trading`
2. `System Health (Monitoring)`
3. `Vue consolidée Multi-Agents`
4. `Analyse Technique V2`
5. `Analyse Fondamentale V2`
6. `Macro & News (AG4)`

### 6.3 Page 1 — Dashboard Trading

Fonctions métier :
- comparaison simultanée 3 portefeuilles AG1-V3 (GPT/Grok/Gemini),
- sélection `Focus` pour détails portefeuille actif,
- scoreboard KPI (valeur, ROI, cash, DD, Sharpe, exposition, frais, score agent),
- graphe overlay compare (equity + optional drawdown),
- 5 onglets détail :
  - `Allocation (actif)` :
    - répartition secteur/industrie/classe,
    - sparklines 90j par position (historique `yfinance-api /history`),
    - table positions enrichie.
  - `Rendement (actif)` :
    - sous-onglets `Rendement Financier`, `Efficacité du Capital`, `Qualité du Trading`, `Risque`,
    - waterfall PnL, courbes equity, distributions trades/durées, drawdown/Sharpe/ProfitFactor.
  - `Cerveau IA (actif)` :
    - tables signaux + alertes AG1.
  - `Marché & Recherche (global)` :
    - sous-onglets `Macro & Buzz` + `Recherche`,
    - baromètres sectoriels/news, top convictions, treemap opportunités, scénarios.
  - `Benchmarks & Indices` :
    - compare AG1 vs CAC40/S&P500/EURO STOXX 50,
    - mode base100 ou performance %, alpha vs benchmark référence.

### 6.4 Page 2 — System Health

Fonctions :
- freshness par symbole sur AG2 (tech), AG3 (fonda), AG4-SPE (news),
- freshness macro globale AG4,
- statut dernier run par workflow (RUNNING stale détecté, SUCCESS/PARTIAL/FAILED/NO_DATA),
- KPI statuts (`À jour`, `À surveiller`, `En retard`, `Manquant`),
- détails filtrables par symbole/secteur.

### 6.5 Page 3 — Vue consolidée Multi-Agents

Fonctions :
- fusion AG2+AG3+AG4(+AG4-SPE)+YF enrichment,
- construction matrice Risk/Reward/probabilité (`_build_multi_agent_matrix`),
- décisions finales :
  - `Entrer / Renforcer`
  - `Surveiller`
  - `Réduire / Sortir`
- grades dynamiques A/B/C, gates data quality/earnings/liquidité/options,
- visualisation scatter interactive (sélection symbole → fiche rapide),
- mode `Vue par valeur` avec :
  - KPI détail,
  - panel débutant,
  - badges gates hard/soft,
  - trade card (copie texte/json),
  - audit data quality.

### 6.6 Page 4 — Macro & News (AG4)

Fonctions :
- fenêtre historique configurable,
- overview macro (alertes régime/thèmes),
- news par valeur avec scopes (portefeuille actif/tous portefeuilles/universe),
- historique runs macro + spe,
- qualité pipeline news.

### 6.7 Page 5 — Analyse Technique V2 (AG2)

Fonctions :
- onglet `Vue d'ensemble` :
  - santé run AG2,
  - KPI BUY/SELL/NEUTRAL/actionables/appels IA/approb IA,
  - graphes mix signal/heatmap/matrice H1-D1/funnel/scatter quality,
  - filtres rapides + top BUY/SELL/divergences.
- onglet `Vue détaillée` :
  - fiche symbole (KPI H1/D1/IA),
  - indicateurs visuels (RSI gauge, bars indicateurs),
  - alignement SMA,
  - chandeliers H1/D1 via `yfinance-api /history`,
  - carte analyse IA textuelle.
- onglet `Historique Runs` :
  - table runs + historique signaux filtrable.

### 6.8 Page 6 — Analyse Fondamentale V2 (AG3)

Fonctions :
- onglet `Vue d'ensemble` :
  - KPI conviction/risque/potentiel/couverture,
  - distribution score,
  - carte conviction vs risque,
  - qualité des runs dans le temps,
  - table synthèse triage.
- onglet `Vue détaillée` :
  - fiche symbole (triage/risque/horizon/upside/analystes),
  - gauges multi-facteurs,
  - table interprétation indicateurs,
  - évolution historique symbole,
  - consensus analystes,
  - scénarios 12 mois (Bear/Base/Bull) + historique prix.
- onglet `Historique Runs` :
  - KPIs dernier run + bar chart OK/erreurs + table historique.

### 6.9 Fonctions utilitaires transverses

- `app_modules/tables.py` : recherche globale, filtres colonnes (num/date/text), tri, rendu table interactive.
- `app_modules/visualizations.py` : prefetch concurrent `yfinance-api`, sparklines portefeuille, extraction events BUY/SELL.
- `app_modules/core.py` : parsing robuste, normalisation colonnes, enrichissement universe, calcul sentiment sectoriel et momentum symbole.

## 7. Inventaire des fonctions dashboard

> Pour le détail exhaustif des noms de fonctions (snapshot 2026-03-02), consulter l'historique Git de ce fichier ou relancer un pass de découverte sur `dashboard/app.py`.

Les modules sont structurés comme suit :

- **`dashboard/app.py`** : point d'entrée Streamlit, contient les loaders (Google Sheets + DuckDB), les rendus de chaque page, et les helpers UI (badges, KPI, charts).
- **`dashboard/app_modules/core.py`** : normalisation, fresheness check, valorisations, sentiment sectoriel, momentum symbole.
- **`dashboard/app_modules/tables.py`** : helpers de tri / recherche / filtres sur DataFrames.
- **`dashboard/app_modules/visualizations.py`** : sparklines portefeuille, extraction events de trade, prefetch concurrent historiques.
