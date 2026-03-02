# ETAT_DES_LIEUX_FONCTIONNEL - Trader_IA

Date d'analyse: 2026-03-02
Perimetre: repository `Trader_IA` + configuration VPS `vps_hostinger_config/docker-compose.yml`.
Objectif: fournir une base d'entree claire et operationnelle pour un mode projet GPT-5.2.

## 1. Resume executif

Le projet est une plateforme multi-agents de trading organisee autour de:
- `n8n` (orchestration des workflows AG0/AG1/AG2/AG3/AG4/YF enrichment),
- `DuckDB` (source of truth analytique et execution),
- `Qdrant` (RAG/vector search),
- `yfinance-api` (acces Yahoo avec cache/cooldown robuste),
- `trading-dashboard` (Streamlit, vue operationnelle multi-agents),
- `Traefik` (reverse proxy TLS).

Le systeme est deja en mode "DuckDB-first" sur AG2/AG3/AG4/AG4-SPE et AG1-V3.
Points structurants observes:
- coexistence de versions V2/V3 dans les paths/environnements,
- forte interdependance AG1 <- AG2/AG3/AG4/YF,
- workflow JSON `AG4-SPE-V2/AG4-SPE-V2-workflow.json` corrompu (4 bytes), regeneration via `build_workflow.py` obligatoire.

## 2. Services deployes sur le VPS

Source: `vps_hostinger_config/docker-compose.yml`.

### 2.1 Catalogue des services

| Service | Role principal | Exposition | Dependances |
|---|---|---|---|
| `traefik` | Reverse proxy TLS + routage host-based | `80`, `443` publics | Docker provider |
| `n8n` | Orchestrateur workflows | `127.0.0.1:5678` (proxifie via Traefik) | `yfinance-api` |
| `task-runners` (x3 replicas) | Runners externes n8n (Python/JS) | interne reseau Docker | `n8n` |
| `yfinance-api` | API marche (history/quote/options/calendar/fundamentals) avec cache disque | interne reseau Docker | aucune |
| `yf-enrichment` | Microservice FastAPI qui lance `daily_enrichment.py` | interne reseau Docker (`:8081`) | `yfinance-api` |
| `trading-dashboard` | App Streamlit (dashboard) | proxifie Traefik (`dashboard.nlhconsulting.fr`) | sources DuckDB + yfinance-api |
| `qdrant` | Vector DB (RAG) | `127.0.0.1:6333/6334` | aucune |
| `toolbox` | Container utilitaire debug (`curl`, `jq`) | interne | aucune |

### 2.2 Parametres d'architecture importants

- Reseau unique: `web` (external network Docker).
- Stockage persistant:
  - `n8n_data`, `traefik_data`, `qdrant_data`, `yfinance_data` en volumes externes.
  - partage cross-services via `/local-files` monte sur `/files`.
- `n8n` tourne en mode runners externes:
  - `N8N_RUNNERS_ENABLED=true`
  - broker `:5679`
  - `task-runners` en parallelisme.
- `qdrant` securise par API key (`QDRANT__SERVICE__API_KEY`).
- Dashboard protege par BasicAuth Traefik.

### 2.3 Flux inter-services (fonctionnel)

1. `n8n` orchestre les workflows.
2. AG2/AG3/AG1-PF/YF enrichment interrogent `yfinance-api`.
3. AG2/AG3/AG4-SPE vectorisent dans `qdrant`.
4. AG1-V3 lit DuckDB + outils RAG Qdrant, puis ecrit ledger AG1.
5. `trading-dashboard` lit majoritairement DuckDB et appelle `yfinance-api` pour certains graphes/snapshots.

## 3. Workflows et role metier

## 3.1 AG0 - Extraction universe

- Fichier: `AG0-V1 - extraction universe/AG0-V1 - extraction universe.json`
- Trigger: manuel.
- Role:
  - scrape Boursorama compartiments A/B/C,
  - normalise `Symbol` (`<ticker>.PA`) + `Name`,
  - export CSV + XLSX vers Google Drive.
- Usage: alimentation universe (amont manuel).

## 3.2 AG1-PF-V1 - Portfolio MTM (DuckDB-only, multi AG1-V2)

- Fichier: `AG1-PF-V1/AG1-PF-V1-workflow.json`
- Trigger:
  - schedule `0 0 9-17 * * 1-5`
  - manuel.
- Sources:
  - bases AG1 `ag1_v2_*` (lecture positions),
  - `yfinance-api /history` (1H + 1D),
  - optional enrichment Universe via AG2 DB.
- Traitement:
  - normalisation lignes portefeuille,
  - fetch prix H1/D1,
  - choix meilleur prix (freshest/fallback),
  - calcul MTM (`LastPrice`, `MarketValue`, `UnrealizedPnL`),
  - ecriture DuckDB latest/history/run_log.
- Sorties:
  - tables `portfolio_positions_mtm_latest`, `portfolio_positions_mtm_history`, `portfolio_positions_mtm_run_log`.

## 3.3 AG1-V3 - Portfolio Manager (3 variants modeles)

- Fichiers:
  - template: `AG1-V3-Portfolio manager/workflow/AG1_workflow_template_v3.json`
  - variants: `.../variants/AG1_workflow_v3__chatgpt52.json`, `...grok41_reasoning.json`, `...gemini30_pro.json`.
- Trigger (variant ChatGPT 5.2): `0 15 9 * * 1-5`, `0 30 12 * * 1-5`, `0 45 16 * * 1-5`.
- Role metier:
  - construit contexte portefeuille + marche (multi-agent pack),
  - appelle agent LLM Portfolio Manager,
  - applique garde-fous d'execution (`Validate & Enforce Safety`),
  - produit bundle d'ordres/fills/signaux/alertes,
  - upsert dans ledger AG1 DuckDB (`core.*` + `cfg.*`),
  - calcule snapshots + health post-run.
- RAG utilise par agent:
  - `financial_news_v3_clean` (news),
  - `fundamental_analysis` (fonda),
  - `financial_tech_v1` (tech).
- Note: legacy branches Google Sheets conservees mais desactivees dans variants exportes.

## 3.4 AG2-V3 - Analyse technique

- Fichier: `AG2-V3/AG2-V3 - Analyse technique.json`
- Trigger:
  - cron `10 9-17 * * 1-5`
  - manuel.
- Sources:
  - Universe (Google Sheets),
  - `yfinance-api /history` (1H et 1D),
  - LLM validation (route FX vs Equity/ETF),
  - Qdrant collection `financial_tech_v1`.
- Pipeline fonctionnel:
  1. init config + batch rotation,
  2. init schema DuckDB,
  3. loop symboles,
  4. calcul indicateurs techniques (H1/D1),
  5. pre-filtres PM + dedup AI cache,
  6. validation IA (prompts differencies FX vs actions),
  7. write `technical_signals`,
  8. finalize run + optional sync sheets,
  9. build vector docs (`VectorDoc_v2`), delete-by-doc_id, upsert Qdrant, mark vectorized.
- Sorties principales:
  - table `technical_signals` + vues `v_latest_signals`, `v_ag1_summary`, `v_ag2_fx_output`.

## 3.5 AG3-V2 - Analyse fondamentale

- Fichier: `AG3-V2/AG3-V2-workflow.json`
- Trigger:
  - schedule `0 7 * * 1-5`
  - manuel.
- Sources:
  - Universe (Google Sheets),
  - `yfinance-api /fundamentals`,
  - Qdrant collection `fundamental_analysis`.
- Pipeline:
  1. init contexte + queue,
  2. init schema + run (DuckDB),
  3. fetch fondamentaux par symbole,
  4. scoring (quality/growth/valuation/health/consensus/risk),
  5. ecriture triage/consensus/metrics/snapshot,
  6. finalize run,
  7. vector docs + delete/upsert + mark vectorized.

## 3.6 AG4-V3 - Macro & News

- Fichier: `AG4-V3/AG4-V3-workflow.json`
- Trigger:
  - schedule `*/30 7-20 * * 1-5`
  - manuel.
- Sources:
  - Google Sheets `Source_RSS`,
  - Google Sheets `Universe`.
- Pipeline:
  1. chargement flux RSS + dictionnaire symboles/secteurs,
  2. init schema DuckDB + run log,
  3. lecture index historique,
  4. normalisation RSS, tagging symboles, dedupe clustering,
  5. pre-score + routage new/seen + analyse IA news,
  6. ecriture `news_history` / `news_errors`,
  7. finalize run et generation vues FX macro (`ag4_fx_macro`, `ag4_fx_pairs`).
- Role: fournir regime macro, themes, secteurs/currencies bullish-bearish.

## 3.7 AG4-SPE-V2 - News specifiques par valeur

- Etat fichier workflow:
  - `AG4-SPE-V2/AG4-SPE-V2-workflow.json` corrompu (BOM + newline, 4 bytes).
  - Source fiable: `AG4-SPE-V2/build_workflow.py` + `AG4-SPE-V2/nodes/*`.
- Trigger (dans `build_workflow.py`):
  - `0 5 9,12,15 * * 1-5` + manuel.
- Sources:
  - Universe Google Sheets,
  - Boursorama listing pages + article pages.
- Pipeline:
  1. init DB + queue rotative symboles (`workflow_state`),
  2. scrape listing actualites par symbole,
  3. extraction URLs + normalisation + dedupe (`news_id=sha1(symbol|canonical_url)`),
  4. routage new vs seen,
  5. fetch article + parsing,
  6. preparation prompt + analyse OpenAI (schema JSON strict),
  7. upsert `news_history`, write `news_errors`,
  8. finalize run,
  9. vector docs Qdrant (`financial_news_v3_clean`) + mark vectorized.

## 3.8 YF-ENRICH-V1 - Enrichissement quotidien marche

- Workflow: `yf-enrichment-v1/YF-ENRICH-V1-daily-workflow.json`
- Trigger:
  - schedule `15 6 * * *`
  - manuel.
- Execution:
  - n8n fait `POST http://yf-enrichment:8081/run`,
  - service `yf-enrichment` lance `daily_enrichment.py`.
- Sources:
  - symboles de `ag2` table `universe` (ou argument `--symbols`),
  - `yfinance-api` endpoints `/quote`, `/options`, `/calendar`.
- Sortie:
  - DuckDB `yf_enrichment_v1.duckdb` (`run_log`, `yf_symbol_enrichment_history`, `v_latest_symbol_enrichment`).

## 4. Sources de donnees (detail)

## 4.1 Sources externes

| Source | Type | Consommateurs |
|---|---|---|
| Yahoo Finance (via `yfinance` python) | Marche (OHLCV, quote L1, options, earnings, fundamentals) | `yfinance-api`, puis AG1/AG2/AG3/AG1-PF/YF enrichment/dashboard |
| Boursorama cotations | Universe actions FR | AG0 |
| Boursorama actualites par valeur | News symboles | AG4-SPE-V2 |
| Flux RSS (liste en Sheet `Source_RSS`) | News macro | AG4-V3 |
| OpenAI API | LLM analyse news/agent + embeddings | AG1-V3, AG4-SPE-V2, vectorisation |
| Google Sheets | Configuration/source universe/rss | AG0, AG2, AG3, AG4, AG4-SPE, dashboard fallback |

## 4.2 Sources internes (data products)

| Source interne | Produit par | Consomme par |
|---|---|---|
| `ag2_v3.duckdb` | AG2 | AG1-V3, dashboard, YF enrichment (universe) |
| `ag3_v2.duckdb` | AG3 | AG1-V3, dashboard |
| `ag4_v3.duckdb` | AG4 macro | AG1-V3, dashboard |
| `ag4_spe_v2.duckdb` | AG4-SPE | AG1-V3, dashboard |
| `yf_enrichment_v1.duckdb` | YF enrichment | AG1-V3, dashboard |
| `ag1_v3*.duckdb` | AG1-V3 | dashboard, AG1-PF (selon config) |
| `ag1_v2*.duckdb` | AG1-V2/AG1-PF | dashboard legacy + compat |
| Qdrant collections | AG2/AG3/AG4-SPE vector docs | AG1-V3 tools (RAG) |

## 5. Bases de donnees generees et schemas

## 5.1 DuckDB AG1-PF (MTM) - `AG1-PF-V1/sql/schema.sql`

Tables:
- `portfolio_positions_mtm_run_log`
- `portfolio_positions_mtm_latest`
- `portfolio_positions_mtm_history`
- vue `v_portfolio_positions_mtm_latest`

Colonnes clefs:
- run lifecycle: `run_id`, `started_at`, `finished_at`, `status`, compteurs.
- positions latest/history: `symbol`, `quantity`, `avg_price`, `last_price`, `market_value`, `unrealized_pnl`, `updated_at`, `run_id`.

## 5.2 DuckDB AG1-V3 ledger - `AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql`

Schemas:
- `core`
- `cfg`

Tables `core`:
- `runs`, `instruments`, `market_prices`,
- `orders`, `fills`, `cash_ledger`,
- `position_lots`, `positions_snapshot`, `portfolio_snapshot`,
- `ai_signals`, `risk_metrics`, `alerts`, `backfill_queue`.

Tables `cfg`:
- `portfolio_config`.

Role:
- modele ledger complet execution + audit + risque + snapshots portefeuille.

## 5.3 DuckDB AG2-V3 - `AG2-V3/sql/schema.sql`

Tables:
- `universe`
- `technical_signals`
- `ai_dedup_cache`
- `run_log`
- `batch_state`

Vues:
- `v_latest_signals`
- `v_pending_vectors`
- `v_ag1_summary`
- `v_ag2_fx_output`

Schema notable `technical_signals`:
- identifiants/run: `id`, `run_id`, `symbol`, `symbol_internal`, `symbol_yahoo`, `asset_class`, `workflow_date`
- H1/D1 status/actions/scores/confidence
- indicateurs techniques complets (SMA/EMA/MACD/RSI/ATR/BB/Stoch/ADX/OBV/Support/Resistance)
- metadonnees FX (`base_ccy`, `quote_ccy`, `pip_size`, `atr_pips_*`)
- AI validation (`ai_decision`, `ai_quality`, `ai_alignment`, `ai_stop_loss`, `ai_rr_theoretical`, etc.)
- vector tracking (`vector_status`, `vector_id`, `vectorized_at`).

## 5.4 DuckDB AG3-V2 - `AG3-V2/nodes/06_duckdb_init.py`

Tables:
- `run_log`
- `fundamentals_snapshot`
- `fundamentals_triage_history`
- `analyst_consensus_history`
- `fundamental_metrics_history`
- `batch_state`

Vues:
- `v_latest_triage`
- `v_latest_consensus`

Colonnes metier:
- triage: `score`, `risk_score`, `quality_score`, `growth_score`, `valuation_score`, `health_score`, `consensus_score`, `horizon`, `upside_pct`, `recommendation`.
- consensus: targets mean/high/low + analyst count + dispersion/risk proxy.
- metrics: donnees atomiques section/metric/value/unit.

## 5.5 DuckDB AG4-V3 - `AG4-V3/nodes/12_duckdb_init.py`

Tables:
- `news_history`
- `news_errors`
- `run_log`
- `ag4_fx_macro`
- `ag4_fx_pairs`

Colonnes notables `news_history`:
- dedupe/event: `dedupe_key`, `event_key`, `canonical_url`
- contenu: `title`, `snippet`, `theme`, `regime`, `notes`
- impacts: `impact_score`, `confidence`, `urgency`, `action`
- tagging macro: `sectors_bullish`, `sectors_bearish`, `currencies_bullish`, `currencies_bearish`
- trace run: `run_id`, `analyzed_at`, `first_seen_at`, `last_seen_at`.

## 5.6 DuckDB AG4-SPE-V2 - `AG4-SPE-V2/nodes/00_duckdb_prepare.py`

Tables:
- `universe_symbols`
- `news_history`
- `news_errors`
- `run_log`
- `workflow_state`

Colonnes notables `news_history`:
- identite: `news_id`, `symbol`, `canonical_url`
- contenu: `title`, `snippet`, `text`, `summary`, `published_at`
- IA: `impact_score`, `sentiment`, `confidence_score`, `horizon`, `urgency`, `suggested_signal`, `key_drivers`, `needs_follow_up`, `is_relevant`
- vector: `vector_status`, `vector_id`, `vectorized_at`, `chunk_total`
- lifecycle: `first_seen_at`, `last_seen_at`, `analyzed_at`, `fetched_at`.

## 5.7 DuckDB YF enrichment - `yf-enrichment-v1/daily_enrichment.py`

Tables:
- `run_log`
- `yf_symbol_enrichment_history`

Vue:
- `v_latest_symbol_enrichment`

Colonnes notables:
- quote: `regular_market_price`, `bid`, `ask`, `spread_pct`, `slippage_proxy_pct`, `market_state`
- options: `iv_atm`, `skew_put_minus_call_5pct`, `put_call_oi_ratio`, `options_ok/options_error/options_warning`
- calendar: `next_earnings_date`, `days_to_earnings`, `calendar_ok/calendar_error`.

## 5.8 Qdrant (vector DB)

Collections observees:
- `financial_tech_v1` (AG2)
- `fundamental_analysis` (AG3)
- `financial_news_v3_clean` (AG4-SPE)

Convention metadata (`VectorDoc_v2`):
- `doc_id` stable,
- `schema_version="VectorDoc_v2"`,
- `doc_kind` (`TECH`/`FUNDA`/`NEWS`),
- delete-by-filter `doc_id` avant upsert pour idempotence.

## 6. Dashboard - fonctionnement detaille

Fichiers:
- `dashboard/app.py`
- `dashboard/app_modules/core.py`
- `dashboard/app_modules/tables.py`
- `dashboard/app_modules/visualizations.py`

## 6.1 Sources et connecteurs du dashboard

Variables/env lues:
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

Chargements data principaux:
- Google Sheets fallback/metadata (`load_data`),
- DuckDB read-only avec cache signatures fichiers (`duckdb_file_signature`, `_read_duckdb_df`),
- loaders par domaine (`load_ag2_overview`, `load_ag3_overview`, `load_ag4_*`, `load_yf_enrichment_latest`),
- loaders pages composites (`load_system_health_page_data`, `load_multi_agent_page_data`, etc.).

## 6.2 Navigation fonctionnelle (6 pages)

1. `Dashboard Trading`
2. `System Health (Monitoring)`
3. `Vue consolidee Multi-Agents`
4. `Analyse Technique V2`
5. `Analyse Fondamentale V2`
6. `Macro & News (AG4)`

## 6.3 Page 1 - Dashboard Trading

Fonctions metier:
- comparaison simultanee 3 portefeuilles AG1-V3 (GPT/Grok/Gemini),
- selection `Focus` pour details portefeuille actif,
- scoreboard KPI (valeur, ROI, cash, DD, Sharpe, exposition, frais, score agent),
- graphe overlay compare (equity + optional drawdown),
- 5 onglets detail:
  - `Allocation (actif)`:
    - repartition secteur/industrie/classe,
    - sparklines 90j par position (historique `yfinance-api /history`),
    - table positions enrichie.
  - `Rendement (actif)`:
    - sous-onglets `Rendement Financier`, `Efficacite du Capital`, `Qualite du Trading`, `Risque`,
    - waterfall PnL, courbes equity, distributions trades/durees, drawdown/Sharpe/ProfitFactor.
  - `Cerveau IA (actif)`:
    - tables signaux + alertes AG1.
  - `Marche & Recherche (global)`:
    - sous-onglets `Macro & Buzz` + `Recherche`,
    - barometres sectoriels/news, top convictions, treemap opportunites, scenarios.
  - `Benchmarks & Indices`:
    - compare AG1 vs CAC40/S&P500/EURO STOXX 50,
    - mode base100 ou performance %, alpha vs benchmark reference.

## 6.4 Page 2 - System Health

Fonctions:
- freshness par symbole sur AG2 (tech), AG3 (fonda), AG4-SPE (news),
- freshness macro globale AG4,
- statut dernier run par workflow (RUNNING stale detecte, SUCCESS/PARTIAL/FAILED/NO_DATA),
- KPI statuts (`A jour`, `A surveiller`, `En retard`, `Manquant`),
- details filtrables par symbole/secteur.

## 6.5 Page 3 - Vue consolidee Multi-Agents

Fonctions:
- fusion AG2+AG3+AG4(+AG4-SPE)+YF enrichment,
- construction matrice Risk/Reward/probabilite (`_build_multi_agent_matrix`),
- decisions finales:
  - `Entrer / Renforcer`
  - `Surveiller`
  - `Reduire / Sortir`
- grades dynamiques A/B/C, gates data quality/earnings/liquidite/options,
- visualisation scatter interactive (selection symbole -> fiche rapide),
- mode `Vue par valeur` avec:
  - KPI detail,
  - panel debutant,
  - badges gates hard/soft,
  - trade card (copie texte/json),
  - audit data quality.

## 6.6 Page 4 - Macro & News (AG4)

Fonctions:
- fenetre historique configurable,
- overview macro (alertes regime/themes),
- news par valeur avec scopes (portefeuille actif/tous portefeuilles/universe),
- historique runs macro + spe,
- qualite pipeline news.

## 6.7 Page 5 - Analyse Technique V2 (AG2)

Fonctions:
- onglet `Vue d'ensemble`:
  - sante run AG2,
  - KPI BUY/SELL/NEUTRAL/actionables/appels IA/approb IA,
  - graphes mix signal/heatmap/matrice H1-D1/funnel/scatter quality,
  - filtres rapides + top BUY/SELL/divergences.
- onglet `Vue detaillee`:
  - fiche symbole (KPI H1/D1/IA),
  - indicateurs visuels (RSI gauge, bars indicateurs),
  - alignement SMA,
  - chandeliers H1/D1 via `yfinance-api /history`,
  - carte analyse IA textuelle.
- onglet `Historique Runs`:
  - table runs + historique signaux filtrable.

## 6.8 Page 6 - Analyse Fondamentale V2 (AG3)

Fonctions:
- onglet `Vue d'ensemble`:
  - KPI conviction/risque/potentiel/couverture,
  - distribution score,
  - carte conviction vs risque,
  - qualite des runs dans le temps,
  - table synthese triage.
- onglet `Vue detaillee`:
  - fiche symbole (triage/risque/horizon/upside/analystes),
  - gauges multi-facteurs,
  - table interpretation indicateurs,
  - evolution historique symbole,
  - consensus analystes,
  - scenarios 12 mois (Bear/Base/Bull) + historique prix.
- onglet `Historique Runs`:
  - KPIs dernier run + bar chart OK/erreurs + table historique.

## 6.9 Fonctions utilitaires transverses

- `app_modules/tables.py`: recherche globale, filtres colonnes (num/date/text), tri, rendu table interactive.
- `app_modules/visualizations.py`: prefetch concurrent `yfinance-api`, sparklines portefeuille, extraction events BUY/SELL.
- `app_modules/core.py`: parsing robuste, normalisation colonnes, enrichissement universe, calcul sentiment sectoriel et momentum symbole.

## 7. Inventaire complet des fonctions dashboard

## 7.1 `dashboard/app.py`

```
_load_benchmarks_config_from_env
safe_text
safe_num
safe_pct
safe_score
safe_dt
safe_get
_is_truthy
_value_is_na
_render_inline_info
_render_copy_buttons
_fmt_pct_auto
_kpi_metric_with_info
_metric_meta
_render_metric_help_popover
_metrics_dictionary_df
validate_configuration
get_gspread_client
load_data
_env_int
_env_float
duckdb_file_signature
_connect_readonly
_read_duckdb_df
load_universe_latest
load_ag1_portfolio_latest
load_ag2_overview
load_ag2_history
load_ag3_overview
load_ag3_symbol_history
load_ag3_run_quality_history
load_ag4_macro_history
load_ag4_macro_runs
load_ag4_symbol_history
load_ag4_symbol_history_from_macro
load_ag4_symbol_runs
load_yf_enrichment_latest
load_dashboard_market_data
load_system_health_page_data
load_multi_agent_page_data
load_ag4_page_data
load_ag2_page_data
load_ag3_page_data
_action_badge
_ai_badge
_status_badge
_make_rsi_gauge
_sma_alignment_text
_indicator_bar
_ag2_short_run_id
_ag2_safe_div
_ag2_ratio_text
_ag2_delta_text
_ag2_age_hours
_ag2_fmt_age
_ag2_status_pill_html
_ag2_norm_action_value
_ag2_norm_ai_decision_value
_ag2_pick_col
_ag2_prepare_overview_working_df
_ag2_kpi_counts
_ag2_latest_run_meta
_ag2_signal_mix_figure
_ag2_sector_action_heatmap_figure
_ag2_h1_d1_matrix_figure
_ag2_funnel_figure
_ag2_score_rsi_scatter_figure
_ag2_make_display_table
_ag2_style_display_table
fetch_yfinance_history
_benchmark_lookback_days
fetch_benchmarks_history
normalize_to_base100
_align_daily_normalized_series
_series_period_return_pct
fetch_yfinance_quote_batch
fetch_yfinance_options_snapshot
fetch_yfinance_calendar_snapshot
_portfolio_exposure_maps
_score_to_1_5
_grade_from_prob
_score_unit
_stable_jitter
_build_multi_agent_matrix
_make_candlestick_chart
_first_existing_column
_prepare_performance_timeseries
_append_current_efficiency_point
_prepare_transactions
_build_realized_vs_total_curve
_build_trade_quality_dataframe
_build_underwater_dataframe
_compute_risk_scorecards
_make_return_gauge
_funda_eval
_make_funda_gauge
_safe_series
_clamp_pct
_estimate_scenario_probabilities
_normalize_macro_news_df
_normalize_symbol_news_df
_news_parse_listish
_news_to_numeric_0_100
_news_urgency_to_score
_news_confidence_to_score
_news_bool_or_none
_news_extract_symbols
_news_infer_direction
normalize_news_schema
_news_short_run_id
_news_latest_run_snapshot
_news_window_cutoff
_news_filter_window
_news_pill_html
_news_dedupe_clusters
_news_priority_agg
_news_fmt_ts_paris
_news_fmt_age_h
_news_fmt_pct
_news_fmt_score
_news_pill_html
_news_scope_catalog_from_ag1
_render_macro_alert_card
render_macro_alerts
render_macro_overview
render_symbol_news
render_news_runs_history
_news_health_metrics
render_news_health
_load_fundamentals_for_dashboard
_clean_context_token
_to_dt_utc
_fmt_dt_short
_gate_badge_html
_freshness_status
_latest_timestamp
_freshness_label_from_age
_build_multi_agent_data_freshness
_macro_relevance_score
_synthesis_conclusion
_prepare_multi_agent_view
_duckdb_connect_readonly_retry
_ag1_fetchdf
_ag1_expected_model_tokens
_ag1_model_matches_expected
_ag1_resolve_display_model
_ag1_default_payload
_ag1_load_single_portfolio_ledger
load_ag1_multi_portfolios
_fmt_currency
_fmt_number
_fmt_pct
_fmt_delta_eur
_fmt_delta_pp
_signed_color
_position_pnl_row_html
_fmt_paris_datetime
_short_run_id
_coerce_bool_or_none
_slice_timeseries_by_period
_slice_events_by_period
_compute_position_pnl_lists
_compute_concentration_and_sectors
_compute_order_completeness
_compute_freshness_score
_make_scoreboard_status
_build_mini_equity_curve
_build_compare_overlay_chart
_prepare_compare_card
```

## 7.2 `dashboard/app_modules/core.py`

```
safe_float
safe_float_series
_split_sector_cell
truthy_series
safe_json_parse
clean_text
clean_research_text
format_impact_html
normalize_cols
norm_symbol
enrich_df_with_name
check_freshness
extract_valuation_scenarios
calculate_sector_sentiment
calculate_symbol_momentum
```

## 7.3 `dashboard/app_modules/tables.py`

```
_coerce_for_sort
_apply_global_search
_apply_column_filters
_apply_sort
render_interactive_table
```

## 7.4 `dashboard/app_modules/visualizations.py`

```
_first_existing_column
_normalize_portfolio_positions
_normalize_transactions
_fetch_one_symbol_history
_prefetch_histories
_compute_buy_levels
_extract_trade_events
_build_position_sparkline
render_portfolio_sparklines
```

## 8. Ecarts, risques et points d'attention

1. `AG4-SPE-V2/AG4-SPE-V2-workflow.json` est inutilisable (corrompu). Le workflow doit etre regenere via `python AG4-SPE-V2/build_workflow.py`.
2. Coexistence V2/V3:
   - compose `n8n` configure `AG1_DUCKDB_PATH=/files/duckdb/ag1_v2.duckdb`,
   - dashboard code est centre sur `ag1_v3_*` pour la comparaison multi-modeles.
   - verifier la coherence des variables env en production.
3. `docker-compose.yml` contient des secrets/token en clair (runner token + basic auth hashes). A traiter avant industrialisation.
4. Duplication de fonction `_news_pill_html` dans `dashboard/app.py` (definition ecrasee par la seconde).
5. Plusieurs textes FR montrent des artefacts d'encodage cp1252/utf-8; impact principal: lisibilite.

## 9. Recommandations pour le mode projet GPT-5.2

1. Corriger d'abord la coherence de deploiement (paths AG1 V2/V3 + env dashboard).
2. Regenerer et versionner `AG4-SPE-V2-workflow.json` depuis `build_workflow.py` avant tout changement fonctionnel.
3. Formaliser une matrice "workflow -> DB -> dashboard page" pour fiabiliser les futures evolutions.
4. Ajouter un audit automatique post-deploiement:
   - presence DB/tables/views attendues,
   - dernier run status par workflow,
   - couverture YF enrichment,
   - disponibilite Qdrant collections.

