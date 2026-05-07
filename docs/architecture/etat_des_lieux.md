# Ã‰tat des lieux fonctionnel â€” Trader_IA

**DerniÃ¨re analyse exhaustive : 2026-03-02**
**DerniÃ¨re mise Ã  jour partielle : 2026-05-04** (AG1-FX compact LLM brief, schedules AG4/AG4-Forex, AG4-FX digest, fallback DuckDB AG1-V3, wiring AG3-V2 â€” cf Â§3.3 / Â§3.3bis / Â§3.5 / Â§3.6 / Â§3.6bis / Â§3.6ter)
**PÃ©rimÃ¨tre** : repository `Trader_IA` + configuration VPS `infra/vps_hostinger_config/docker-compose.yml`.
**Objectif** : fournir une base d'entrÃ©e claire et opÃ©rationnelle pour un mode projet LLM.

> â„¹ï¸ Pour les **issues/Ã©carts connus** (avec statut rÃ©solu / en cours / Ã  faire), voir le document sÃ©parÃ© [`historique_issues.md`](historique_issues.md).

## 1. RÃ©sumÃ© exÃ©cutif

Le projet est une plateforme multi-agents de trading organisÃ©e autour de :

- `n8n` (orchestration des workflows AG0/AG1/AG2/AG3/AG4/YF enrichment),
- `DuckDB` (source of truth analytique et exÃ©cution),
- `yfinance-api` (accÃ¨s Yahoo avec cache/cooldown robuste),
- `trading-dashboard` (Streamlit, vue opÃ©rationnelle multi-agents),
- `Traefik` (reverse proxy TLS).

Le systÃ¨me est dÃ©jÃ  en mode "DuckDB-first" sur AG2/AG3/AG4/AG4-SPE et AG1-V3.

Points structurants observÃ©s :

- coexistence de versions V2/V3 dans les paths/environnements,
- forte interdÃ©pendance AG1 â† AG2/AG3/AG4/YF,
- ensemble AG1 en 3 modÃ¨les parallÃ¨les (GPT-5.2 / Grok-4.1 / Gemini-3), chacun avec sa DuckDB.
- ensemble AG1-FX-V1 en 3 portefeuilles Forex-only parallÃ¨les (ChatGPT/Grok/Gemini), chacun avec sa DuckDB et un brief LLM compact.

## 2. Services dÃ©ployÃ©s sur le VPS


### 2.1 Catalogue des services

| Service | RÃ´le principal | Exposition | DÃ©pendances |
|---|---|---|---|
| `traefik` | Reverse proxy TLS + routage host-based | `80`, `443` publics | Docker provider |
| `n8n` | Orchestrateur workflows | `127.0.0.1:5678` (proxyfiÃ© via Traefik) | `yfinance-api` |
| `task-runners` (x3 replicas) | Runners externes n8n (Python/JS) | interne rÃ©seau Docker | `n8n` |
| `yfinance-api` | API marchÃ© (history/quote/options/calendar/fundamentals) avec cache disque | interne rÃ©seau Docker | aucune |
| `yf-enrichment` | Microservice FastAPI qui lance `daily_enrichment.py` | interne rÃ©seau Docker (`:8081`) | `yfinance-api` |
| `trading-dashboard` | App Streamlit (dashboard) | proxyfiÃ© Traefik (`${DASHBOARD_DOMAIN}`) | sources DuckDB + yfinance-api |
| `toolbox` | Container utilitaire debug (`curl`, `jq`) | interne | aucune |

### 2.2 ParamÃ¨tres d'architecture importants

- RÃ©seaux : `web` (externe) et `traefik_proxy` (externe).
- Stockage persistant :
  - partage cross-services via `/local-files` montÃ© sur `/files`.
- `n8n` tourne en mode runners externes :
  - `N8N_RUNNERS_ENABLED=true`
  - broker `:5679`
  - `task-runners` en parallÃ©lisme (3 replicas).
- Dashboard protÃ©gÃ© par BasicAuth Traefik.

### 2.3 Flux inter-services (fonctionnel)

1. `n8n` orchestre les workflows.
2. AG2/AG3/AG1-PF/YF enrichment interrogent `yfinance-api`.
5. `trading-dashboard` lit majoritairement DuckDB et appelle `yfinance-api` pour certains graphes/snapshots.

## 3. Workflows et rÃ´le mÃ©tier

### 3.1 AG0 â€” Extraction universe (utilitaire ponctuel)

- Fichier : `outils/AG0-V1 - extraction universe/AG0-V1 - extraction universe.json`
- Trigger : manuel (workflow inactif en production, conservÃ© dans `outils/` comme utilitaire).
- RÃ´le :
  - scrape Boursorama compartiments A/B/C,
  - normalise `Symbol` (`<ticker>.PA`) + `Name`,
  - export CSV + XLSX vers Google Drive.
- Usage : alimentation universe (amont manuel, aujourd'hui pilotÃ© directement via la Google Sheets d'univers).

### 3.2 AG1-PF-V1 â€” Portfolio MTM (DuckDB-only, multi AG1-V2)

- Fichier : `agents/trading-actions/AG1-PF-V1/AG1-PF-V1-workflow.json`
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
  - Ã©criture DuckDB latest/history/run_log.
- Sorties :
  - tables `portfolio_positions_mtm_latest`, `portfolio_positions_mtm_history`, `portfolio_positions_mtm_run_log`.

### 3.3 AG1-V3 â€” Portfolio Manager (3 variants modÃ¨les)

- Fichiers :
  - template : `agents/trading-actions/AG1-V3-Portfolio manager/workflow/AG1_workflow_template_v3.json`
  - variants : `.../variants/AG1_workflow_v3__chatgpt52.json`, `...grok41_reasoning.json`, `...gemini30_pro.json`.
- Trigger (variant ChatGPT 5.2) : `0 15 9 * * 1-5`, `0 30 12 * * 1-5`, `0 45 16 * * 1-5`.
- RÃ´le mÃ©tier :
  - construit contexte portefeuille + marchÃ© (multi-agent pack),
  - appelle agent LLM Portfolio Manager,
  - applique garde-fous d'exÃ©cution (`Validate & Enforce Safety`),
  - produit bundle d'ordres/fills/signaux/alertes,
  - upsert dans ledger AG1 DuckDB (`core.*` + `cfg.*`),
  - calcule snapshots + health post-run.
- RAG utilisÃ© par agent :
- Notes :
  - legacy branches Google Sheets conservÃ©es mais dÃ©sactivÃ©es dans variants exportÃ©s.
  - les nodes de contexte portefeuille et d'enrichissement prix marchÃ© rÃ©solvent les chemins DuckDB `/files/...` et `/local-files/...`, puis retombent sur le ledger par dÃ©faut si le chemin entrant est absent ou inutilisable.

### 3.3bis AG1-FX-V1 â€” Portfolio Manager Forex-only (3 variants modÃ¨les)

- Fichiers :
  - template : `agents/trading-forex/AG1-FX-V1-Portfolio manager/workflow/AG1_FX_workflow_template_v1.json`
  - variants : `.../AG1_FX_workflow_chatgpt52_v1.json`, `...grok41_reasoning_v1.json`, `...gemini30_pro_v1.json`.
- Triggers :
  - `chatgpt52` : `30 9,14 * * 1-5`
  - `grok41_reasoning` : `45 9,14 * * 1-5`
  - `gemini30_pro` : `0 10,15 * * 1-5`
- Sources :
  - `ag2_fx_v1.duckdb` pour universe FX + signaux techniques,
  - `ag4_fx_v1.duckdb` pour digest macro/news FX (`top_news`, `pair_focus`, `macro_regime`),
  - ledger propre par modÃ¨le (`ag1_fx_v1_chatgpt52.duckdb`, `ag1_fx_v1_grok41_reasoning.duckdb`, `ag1_fx_v1_gemini30_pro.duckdb`).
- RÃ´le mÃ©tier :
  - gÃ¨re un portefeuille sandbox Forex-only de 10 000 EUR par modÃ¨le,
  - envoie au LLM un `llm_brief` compact au lieu du payload brut AG2/AG4 complet,
  - conserve le `brief` complet dans le contexte workflow pour les checks risque, conversions, fills et snapshots,
  - filtre le pack `top_news` pour privilÃ©gier les news avec hint directionnel FX quand elles existent,
  - dÃ©rive `llm_model` depuis le variant; `AG1_FX_LLM_MODEL_OVERRIDE` sert uniquement aux overrides ponctuels.
- Garde-fous :
  - leverage max, exposition par paire/devise, daily drawdown, kill switch,
  - downstream parser + risk manager + simulate fills + ledger write aprÃ¨s chaque agent provider.

### 3.4 AG2-V3 â€” Analyse technique

- Fichiers : `agents/trading-actions/AG2-V3/AG2-V3 - Analyse technique (FX only).json` + `agents/trading-actions/AG2-V3/AG2-V3 - Analyse technique (non-FX).json`
  - ces deux variants sont les sources de vÃ©ritÃ© (l'ancien canonique `AG2-V3 - Analyse technique.json` a Ã©tÃ© retirÃ© du repo en avril 2026).
- Trigger :
  - cron `10 9-17 * * 1-5`
  - manuel.
- Sources :
  - Universe (Google Sheets),
  - `yfinance-api /history` (1H et 1D),
  - LLM validation (route FX vs Equity/ETF),
- Pipeline fonctionnel :
  1. init config + batch rotation,
  2. init schema DuckDB,
  3. loop symboles,
  4. calcul indicateurs techniques (H1/D1),
  5. prÃ©-filtres PM + dedup AI cache,
  6. validation IA (prompts diffÃ©renciÃ©s FX vs actions),
  7. write `technical_signals`,
  8. finalize run + optional sync sheets,
- Sorties principales :
  - table `technical_signals` + vues `v_latest_signals`, `v_ag1_summary`, `v_ag2_fx_output`.

### 3.5 AG3-V2 â€” Analyse fondamentale

- Fichier : `agents/trading-actions/AG3-V2/AG3-V2-workflow.json`
- Trigger :
  - schedule `0 7 * * 1-5`
  - manuel.
- Sources :
  - Universe (Google Sheets),
  - `yfinance-api /fundamentals`,
- Pipeline :
  1. init contexte + queue,
  2. init schema + run (DuckDB),
  3. fetch fondamentaux par symbole,
  4. scoring (quality/growth/valuation/health/consensus/risk),
  5. Ã©criture triage/consensus/metrics/snapshot,
  6. finalize run,
- Note wiring n8n : le `Split In Batches` utilise `main[0]` comme branche done (`Finalize Run`) et `main[1]` comme branche loop (fetch/process/write).

### 3.6 AG4-V3 â€” Macro & News (+ geo-tagging depuis 2026-04-24)

- Fichier : `agents/common/AG4-V3/AG4-V3-workflow.json`
- Trigger :
  - schedule `45 1,6,10,18 * * 1-5` (4 runs par jour ouvrÃ©, Europe/Paris)
  - manuel.
- Sources :
  - Google Sheets `Source_RSS`,
  - Google Sheets `Universe`.
- Pipeline :
  1. chargement flux RSS + dictionnaire symboles/secteurs,
  2. init schema DuckDB + run log,
  3. lecture index historique,
  4. normalisation RSS, tagging symboles, dedupe clustering,
  5. prÃ©-score + routage new/seen + analyse IA news (le prompt LLM produit dÃ©sormais 4 champs additionnels : `impact_region`, `impact_asset_class`, `impact_magnitude`, `impact_fx_pairs`),
  6. sanitize des 4 champs avec taxonomie fixe + dÃ©rivation automatique de `impact_fx_pairs` Ã  partir de `currencies_bullish/bearish` si manquant,
  7. Ã©criture `news_history` / `news_errors` avec `tagger_version='geo_v1'`,
  8. **dual-write conditionnel** vers `ag4_forex_v1.fx_news_history` (`origin='global_base'`) si `impact_asset_class âˆˆ {FX, Mixed}`,
  9. finalize run et gÃ©nÃ©ration vues FX macro (`ag4_fx_macro`, `ag4_fx_pairs`).
- RÃ´le : fournir rÃ©gime macro, thÃ¨mes, secteurs/currencies bullish-bearish, ET zone gÃ©ographique + classe d'actif + magnitude + paires FX impactÃ©es pour permettre au PM de router les dÃ©cisions par segment de marchÃ©.
- Source du prompt et des guardrails : node `20H1 - Analyze with OpenAI` + `nodes/10_parse_llm_output.js` + `nodes/14_write_fx_news_duckdb.py`.

### 3.6bis AG4-Forex â€” Canaux FX dÃ©diÃ©s (ajoutÃ© 2026-04-24)

- Fichier : `agents/trading-forex/AG4-Forex/AG4-Forex-workflow.json`
- Source de vÃ©ritÃ© : `agents/trading-forex/AG4-Forex/build_workflow.py` + `agents/trading-forex/AG4-Forex/nodes/*`.
- Trigger :
  - schedule `45 3,9,15,21 * * 1-5` (4 runs par jour ouvrÃ©, Europe/Paris), dÃ©calÃ© d'AG4-V3 pour limiter les conflits DuckDB.
- Sources :
  - `infra/config/sources/fx_sources.yaml` â€” liste dans l'ordre : ForexLive, DailyFX, FXStreet, Investing economic calendar, BIS, Fed, ECB, BoJ. Toutes les entrÃ©es sont dÃ©sormais `enabled: true` pour le passage sandbox complet; le loader AG4-Forex ne consomme toutefois que `type=rss`, donc `investing_econ_calendar` nÃ©cessite encore une branche API avant ingestion effective.
- Pipeline :
  1. `00_load_fx_sources.py` â€” chargement YAML + filtre `enabled=true`, init schema, ouverture run log,
  2. `01_normalize_fx_rss_items.js` â€” normalisation RSS â†’ schÃ©ma commun,
  3. `02_add_keys.js` â€” calcul `dedupe_key` / `event_key` alignÃ©s sur AG4-V3,
  4. `03_route_seen_fx_news.py` â€” bypass prÃ©-LLM : si le `dedupe_key` existe dÃ©jÃ  dans `ag4_forex_v1.fx_news_history`, mise Ã  jour de `last_seen_at` puis retour Ã  la boucle sans appel OpenAI,
  5. `20G2 - Router Analyze vs Skip` â€” route les news nouvelles vers le LLM et les doublons vers `20G - Split FX Items`,
  6. `03_prepare_llm_input.js` â€” prompt harmonisÃ© avec AG4-V3 (rÃ©utilisation du mÃªme LLM),
  7. `04_parse_llm_output.js` â€” parsing + sanitize identique,
  8. `05_write_fx_news_duckdb.py` â€” Ã©criture dans `ag4_forex_v1.fx_news_history` avec `origin='fx_channel'` (`dedupe_key` reste la garde finale cÃ´tÃ© DuckDB),
  9. `06_finalize_fx_run.py` â€” clÃ´ture `run_log`.
- RÃ´le : alimenter une base FX isolÃ©e (`ag4_forex_v1.duckdb`) consommÃ©e par AG4-FX-V1 pour produire le digest macro/news FX utilisÃ© par AG1-FX-V1.

### 3.6ter AG4-FX-V1 â€” Digest macro/news FX pour AG1-FX

- Fichier : `agents/trading-forex/AG4-FX-V1/workflow/AG4_FX_workflow_v1.json`
- Trigger :
  - schedule `15 9,14 * * 1-5` (09:15 et 14:15, Europe/Paris)
  - manuel.
- Sources :
  - `ag4_v3.duckdb` pour news globales taguÃ©es `FX` ou `Mixed`,
  - `ag4_forex_v1.duckdb` pour news canal FX, rÃ©gime macro et biais par paire.
- Sortie :
  - `ag4_fx_v1.duckdb`, table `main.fx_digest`, avec trois sections JSON : `top_news`, `pair_focus`, `macro_regime`.
- RÃ´le : fournir Ã  AG1-FX-V1 un pack macro/news mutualisÃ©, dÃ©dupliquÃ© sur la fenÃªtre rÃ©cente, directement consommable par les trois PMs Forex.

### 3.7 AG4-SPE-V2 â€” News spÃ©cifiques par valeur

- Fichier : `agents/trading-actions/AG4-SPE-V2/AG4-SPE-V2-workflow.json` (rÃ©gÃ©nÃ©rÃ© depuis `build_workflow.py` â€” â‰ˆ 112 KB).
- Source de vÃ©ritÃ© : `agents/trading-actions/AG4-SPE-V2/build_workflow.py` + `agents/trading-actions/AG4-SPE-V2/nodes/*`.
- Trigger (dans `build_workflow.py`) :
  - `0 5 9,12,15 * * 1-5` + manuel.
- Sources :
  - Universe Google Sheets,
  - Boursorama listing pages + article pages.
- Pipeline :
  1. init DB + queue rotative symboles (`workflow_state`),
  2. scrape listing actualitÃ©s par symbole,
  3. extraction URLs + normalisation + dedupe (`news_id=sha1(symbol|canonical_url)`),
  4. routage new vs seen,
  5. fetch article + parsing,
  6. prÃ©paration prompt + analyse OpenAI (schema JSON strict),
  7. upsert `news_history`, write `news_errors`,
  8. finalize run,

### 3.8 YF-ENRICH-V1 â€” Enrichissement quotidien marchÃ©

- Workflow : `agents/common/yf-enrichment-v1/YF-ENRICH-V1-daily-workflow.json`
- Trigger :
  - schedule `15 6 * * *`
  - manuel.
- ExÃ©cution :
  - n8n fait `POST http://yf-enrichment:8081/run`,
  - service `yf-enrichment` lance `daily_enrichment.py`.
- Sources :
  - symboles de `ag2` table `universe` (ou argument `--symbols`),
  - `yfinance-api` endpoints `/quote`, `/options`, `/calendar`.
- Sortie :
  - DuckDB `yf_enrichment_v1.duckdb` (`run_log`, `yf_symbol_enrichment_history`, `v_latest_symbol_enrichment`).

## 4. Sources de donnÃ©es (dÃ©tail)

### 4.1 Sources externes

| Source | Type | Consommateurs |
|---|---|---|
| Yahoo Finance (via `yfinance` python) | MarchÃ© (OHLCV, quote L1, options, earnings, fundamentals) | `yfinance-api`, puis AG1/AG2/AG3/AG1-PF/YF enrichment/dashboard |
| Boursorama cotations | Universe actions FR | AG0 |
| Boursorama actualitÃ©s par valeur | News symboles | AG4-SPE-V2 |
| Flux RSS (liste en Sheet `Source_RSS`) | News macro | AG4-V3 |
| Flux FX dÃ©diÃ©s (ForexLive, DailyFX, FXStreet, BIS, Fed, ECB, BoJ â€” `infra/config/sources/fx_sources.yaml`) | News forex | AG4-Forex |
| OpenAI API | LLM analyse news/agent + embeddings (tagging geo/asset-class inclus) | AG1-V3, AG1-FX, AG4-V3, AG4-Forex, AG4-SPE-V2, vectorisation |
| xAI / Google Gemini APIs | LLM Portfolio Manager variants | AG1-V3, AG1-FX |
| Google Sheets | Configuration/source universe/rss | AG0, AG2, AG3, AG4, AG4-SPE, dashboard fallback |

### 4.2 Sources internes (data products)

| Source interne | Produit par | ConsommÃ© par |
|---|---|---|
| `ag2_v3.duckdb` | AG2 | AG1-V3, dashboard, YF enrichment (universe) |
| `ag3_v2.duckdb` | AG3 | AG1-V3, dashboard |
| `ag4_v3.duckdb` | AG4 macro (avec tags geo/asset-class depuis 2026-04-24) | AG1-V3, dashboard |
| `ag4_forex_v1.duckdb` | AG4-V3 (dual-write) + AG4-Forex (canaux dÃ©diÃ©s) | AG4-FX-V1, dashboard Forex P&L/news |
| `ag4_fx_v1.duckdb` | AG4-FX-V1 digest macro/news FX | AG1-FX-V1, dashboard |
| `ag4_spe_v2.duckdb` | AG4-SPE | AG1-V3, dashboard |
| `yf_enrichment_v1.duckdb` | YF enrichment | AG1-V3, dashboard |
| `ag1_v3*.duckdb` (Ã—3 modÃ¨les) | AG1-V3 | dashboard, AG1-PF (selon config) |
| `ag1_fx_v1*.duckdb` (Ã—3 modÃ¨les) | AG1-FX-V1 | dashboard Forex Trading |
| `ag1_v2*.duckdb` | AG1-V2/AG1-PF | dashboard legacy + compat |

## 5. Bases de donnÃ©es gÃ©nÃ©rÃ©es et schÃ©mas

### 5.1 DuckDB AG1-PF (MTM) â€” `agents/trading-actions/AG1-PF-V1/sql/schema.sql`

Tables :
- `portfolio_positions_mtm_run_log`
- `portfolio_positions_mtm_latest`
- `portfolio_positions_mtm_history`
- vue `v_portfolio_positions_mtm_latest`

Colonnes clÃ©s :
- run lifecycle : `run_id`, `started_at`, `finished_at`, `status`, compteurs.
- positions latest/history : `symbol`, `quantity`, `avg_price`, `last_price`, `market_value`, `unrealized_pnl`, `updated_at`, `run_id`.

### 5.2 DuckDB AG1-V3 ledger â€” `agents/trading-actions/AG1-V3-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql`

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

RÃ´le :
- modÃ¨le ledger complet exÃ©cution + audit + risque + snapshots portefeuille.

### 5.2bis DuckDB AG1-FX-V1 ledger â€” `agents/trading-forex/AG1-FX-V1-Portfolio manager/sql/ag1_fx_v1_schema.sql`

Schemas :
- `core`
- `cfg`

Tables `core` :
- `runs`, `orders`, `fills`, `position_lots`,
- `portfolio_snapshot`, `cash_ledger`, `ai_signals`, `alerts`.

Tables `cfg` :
- `portfolio_config` (seeded avec capital 10 000 EUR, `leverage_max=1.0`, `max_pair_pct=0.20`, `max_currency_exposure_pct=0.50`, `max_daily_drawdown_pct=0.05`, `kill_switch_active=FALSE`, `universe_filter='forex_34'`).

RÃ´le :
- ledger exÃ©cution + audit + risque pour les trois PMs Forex-only (`chatgpt52`, `grok41_reasoning`, `gemini30_pro`).

### 5.3 DuckDB AG2-V3 â€” `agents/trading-actions/AG2-V3/sql/schema.sql`

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
- mÃ©tadonnÃ©es FX (`base_ccy`, `quote_ccy`, `pip_size`, `atr_pips_*`)
- AI validation (`ai_decision`, `ai_quality`, `ai_alignment`, `ai_stop_loss`, `ai_rr_theoretical`, etc.)

### 5.4 DuckDB AG3-V2 â€” `agents/trading-actions/AG3-V2/nodes/06_duckdb_init.py`

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

Colonnes mÃ©tier :
- triage : `score`, `risk_score`, `quality_score`, `growth_score`, `valuation_score`, `health_score`, `consensus_score`, `horizon`, `upside_pct`, `recommendation`.
- consensus : targets mean/high/low + analyst count + dispersion/risk proxy.
- metrics : donnÃ©es atomiques section/metric/value/unit.

### 5.5 DuckDB AG4-V3 â€” `agents/common/AG4-V3/nodes/12_duckdb_init.py` + migration `infra/migrations/ag4_v3/20260425_add_geo_tagging.sql`

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
- **tagging geo / asset class (ajout 2026-04-24)** : `impact_region`, `impact_asset_class`, `impact_magnitude`, `impact_fx_pairs`, `tagger_version`
- trace run : `run_id`, `analyzed_at`, `first_seen_at`, `last_seen_at`.

Taxonomies fixÃ©es (cf `docs/specs/ag4_geo_tagging_and_forex_base_v1.md`) :
- `impact_region âˆˆ {Global, US, EU, France, UK, APAC, Emerging, Other}`
- `impact_asset_class âˆˆ {Equity, FX, Commodity, Bond, Crypto, Mixed, None}`
- `impact_magnitude âˆˆ {Low, Medium, High}`
- `impact_fx_pairs` : CSV de paires format `XXXYYY` (sans slash), liste fermee de 34 paires FX.

### 5.5bis DuckDB AG4-Forex â€” `infra/migrations/ag4_forex_v1/20260425_init.sql`

Base dÃ©diÃ©e `ag4_forex_v1.duckdb`, crÃ©Ã©e le 2026-04-24.

Tables :
- `fx_news_history` â€” news FX avec `origin âˆˆ {global_base, fx_channel}` pour distinguer les news remontÃ©es depuis `ag4_v3` (filtrage `FX|Mixed`) de celles ingÃ©rÃ©es via les canaux spÃ©cifiques. `dedupe_key` partagÃ© avec `ag4_v3.news_history` pour jointure.
- `fx_macro` â€” snapshot rÃ©gime FX global (biais par devise USD/EUR/JPY/GBP/CHF/AUD/CAD/NZD + `market_regime`). MÃªme schÃ©ma que `ag4_v3.ag4_fx_macro`.
- `fx_pairs` â€” snapshot par paire avec `directional_bias`, `rationale`, `urgent_event_window`.
- `run_log` â€” runs AG4-Forex avec dÃ©compte `news_from_global` vs `news_from_fx_channels`.
- `news_errors` â€” erreurs d'ingestion par source FX.

Alimentation :
- Ã©criture **primaire** par AG4-V3 (dual-write conditionnel depuis `nodes/14_write_fx_news_duckdb.py` quand `impact_asset_class âˆˆ {FX, Mixed}`) â€” `origin='global_base'`,
- Ã©criture **secondaire** par AG4-Forex (sources FX dÃ©diÃ©es) â€” `origin='fx_channel'`.

### 5.5ter DuckDB AG4-FX-V1 â€” `agents/trading-forex/AG4-FX-V1/sql/ag4_fx_v1_schema.sql`

Base dÃ©diÃ©e `ag4_fx_v1.duckdb`, digest macro/news FX consommÃ© par AG1-FX-V1.

Tables :
- `fx_digest` â€” sections JSON `top_news`, `pair_focus`, `macro_regime` par `run_id`, avec `items_count`.
- `run_log` â€” compteurs `news_global_pulled`, `news_fx_channel_pulled`, `news_after_dedupe`, `sections_written`, `errors`.

Alimentation :
- lecture de `ag4_v3.duckdb` pour news globales FX/Mixed,
- lecture de `ag4_forex_v1.duckdb` pour news canal FX, rÃ©gime macro et biais paires,
- Ã©criture des trois sections compactes dans `main.fx_digest`.

### 5.6 DuckDB AG4-SPE-V2 â€” `agents/trading-actions/AG4-SPE-V2/nodes/00_duckdb_prepare.py`

Tables :
- `universe_symbols`
- `news_history`
- `news_errors`
- `run_log`
- `workflow_state`

Colonnes notables `news_history` :
- identitÃ© : `news_id`, `symbol`, `canonical_url`
- contenu : `title`, `snippet`, `text`, `summary`, `published_at`
- IA : `impact_score`, `sentiment`, `confidence_score`, `horizon`, `urgency`, `suggested_signal`, `key_drivers`, `needs_follow_up`, `is_relevant`
- lifecycle : `first_seen_at`, `last_seen_at`, `analyzed_at`, `fetched_at`.

### 5.7 DuckDB YF enrichment â€” `agents/common/yf-enrichment-v1/daily_enrichment.py`

Tables :
- `run_log`
- `yf_symbol_enrichment_history`

Vue :
- `v_latest_symbol_enrichment`

Colonnes notables :
- quote : `regular_market_price`, `bid`, `ask`, `spread_pct`, `slippage_proxy_pct`, `market_state`
- options : `iv_atm`, `skew_put_minus_call_5pct`, `put_call_oi_ratio`, `options_ok/options_error/options_warning`
- calendar : `next_earnings_date`, `days_to_earnings`, `calendar_ok/calendar_error`.


Collections observÃ©es :

- `doc_id` stable,
- `doc_kind` (`TECH`/`FUNDA`/`NEWS`),
- delete-by-filter `doc_id` avant upsert pour idempotence.

## 6. Dashboard â€” fonctionnement dÃ©taillÃ©

Fichiers :
- `services/dashboard/app.py`
- `services/dashboard/app_modules/core.py`
- `services/dashboard/app_modules/tables.py`
- `services/dashboard/app_modules/visualizations.py`

### 6.1 Sources et connecteurs du dashboard

Variables/env lues :
- `DUCKDB_PATH` (AG2),
- `AG1_DUCKDB_PATH`,
- `AG1_CHATGPT52_DUCKDB_PATH`,
- `AG1_GROK41_REASONING_DUCKDB_PATH`,
- `AG1_GEMINI30_PRO_DUCKDB_PATH`,
- `AG3_DUCKDB_PATH`,
- `AG4_DUCKDB_PATH`,
- `AG4_FOREX_DB_PATH` (ajoutÃ© 2026-04-24, pointant vers `ag4_forex_v1.duckdb`),
- `AG4_FX_V1_DUCKDB_PATH` (digest `ag4_fx_v1.duckdb` pour AG1-FX),
- `AG4_SPE_DUCKDB_PATH`,
- `AG1_FX_V1_CHATGPT52_DUCKDB_PATH`,
- `AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH`,
- `AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH`,
- `YF_ENRICH_DUCKDB_PATH`,
- `YFINANCE_API_URL`,
- `SHEET_ID`, credentials Google.

Chargements data principaux :
- Google Sheets fallback/metadata (`load_data`),
- DuckDB read-only avec cache signatures fichiers (`duckdb_file_signature`, `_read_duckdb_df`),
- loaders par domaine (`load_ag2_overview`, `load_ag3_overview`, `load_ag4_*`, `load_yf_enrichment_latest`),
- loaders pages composites (`load_system_health_page_data`, `load_multi_agent_page_data`, etc.).

### 6.2 Navigation fonctionnelle (8 pages)

1. `Dashboard Trading`
2. `System Health (Monitoring)`
3. `Vue consolidÃ©e Multi-Agents`
4. `Analyse Technique V2`
5. `Analyse Fondamentale V2`
6. `Macro & News (AG4)`
7. `Forex P&L (LLM x Paire)` *(ajoutÃ©e 2026-04-25)*
8. `Forex Trading (AG1-FX)` *(ajoutÃ©e 2026-04-26)*

### 6.2bis Page 7 â€” Forex P&L (LLM x Paire)

Vue dÃ©diÃ©e Ã  la performance Forex isolÃ©e, depuis l'activation du geo-tagging AG4 (date par dÃ©faut **2026-04-24**, modifiable via le sÃ©lecteur de pÃ©riode).

Source de donnÃ©es :
- `core.fills`, `core.orders`, `core.position_lots`, `core.runs` des 3 portefeuilles AG1 (`ag1_v3_chatgpt52.duckdb`, `ag1_v3_grok41_reasoning.duckdb`, `ag1_v3_gemini30_pro.duckdb`), filtrÃ©s par le helper `_fx_mask` / `_fx_prepare_symbol_frame` (dÃ©tection FX par `asset_class`, prefixe `FX:`, suffixe `=X` ou parsing XXXYYY).
- `ag4_forex_v1.duckdb` (`fx_news_history`, `fx_macro`, `run_log`) pour la couverture news.

Composantes UI :
- 3 KPI cards par LLM (P&L FX net, trades fermÃ©s, winrate, lots ouverts, notional ouvert, frais cumulÃ©s).
- Matrice LLM Ã— paire FX colorÃ©e (P&L net) + tableau agrÃ©gÃ© (trades, winrate, P&L).
- Courbe P&L FX cumulÃ©e par LLM (overlay 3 lignes, couleurs `AG1_MULTI_PORTFOLIO_CONFIG[k]["accent"]`).
- Tableau lots FX ouverts (exposition courante, partial P&L rÃ©alisÃ©).
- Tableau fills FX dÃ©taillÃ©s (interactif).
- Couverture AG4-Forex : nb news taguÃ©es FX sur la fenÃªtre, dernier run AG4-Forex (ingestion globale + canal FX dÃ©diÃ©), top paires par volume de news, biais macro courants (table `fx_macro`).

Variables d'environnement requises cÃ´tÃ© `trading-dashboard` :
- `AG4_FOREX_DUCKDB_PATH=/files/duckdb/ag4_forex_v1.duckdb` (ajoutÃ©e dans le docker-compose).

### 6.2ter Page 8 â€” Forex Trading (AG1-FX)

Vue dÃ©diÃ©e aux trois portefeuilles Forex-only AG1-FX-V1, chacun isolÃ© dans sa propre base DuckDB avec capital initial 10 000 EUR.
Les dÃ©cisions AG1-FX sont produites Ã  partir d'un `llm_brief` compact (portfolio, macro, `pair_matrix`, `market_watch`) tandis que le workflow conserve le brief complet pour les contrÃ´les downstream.

Source de donnÃ©es :
- `ag1_fx_v1_chatgpt52.duckdb`, `ag1_fx_v1_grok41_reasoning.duckdb`, `ag1_fx_v1_gemini30_pro.duckdb` (`core.portfolio_snapshot`, `core.position_lots`, `core.orders`, `core.fills`, `core.fill_costs`, `cfg.portfolio_config`).
- `ag2_fx_v1.duckdb` pour les derniers prix/signaux techniques FX.
- `ag4_fx_v1.duckdb` pour la couverture digest macro FX.

Composantes UI :
- 3 cartes KPI par LLM : equity, P&L total, leverage, lots ouverts, winrate, profit factor, ordres rejetÃ©s.
- Scoreboard de performance nette : P&L realise brut, frais d'execution, P&L net, couverture `core.fill_costs`, sources de commission IBKR/simulees/manquantes.
- Radar paires FX : matrice momentum technique composite (AG2-FX score + retours 5D/20D + RSI) vs biais macro/news directionnel AG4-Forex (-1 base bearish, +1 base bullish). Le volume/urgence news n'est plus un axe sature; il devient `Event risk` et pilote la taille des bulles, avec quadrants long aligned / short aligned / conflit / neutre. Un bloc d'aide de lecture et un dictionnaire des indicateurs expliquent les axes, la taille, la forme et les cas d'usage.
- Clic sur une paire dans la matrice : affiche une seconde matrice sous le graphe principal avec la trace historique de la paire. La trajectoire reprend les memes axes et colore les points du blanc (ancien) au bleu fonce (recent), a partir de `technical_signals_fx` et d'une fenetre news AG4-Forex roulante de 72h.
- Onglet `Sparklines paires` : evolution 90j des 34 paires FX via `yfinance-api`, avec separation paires principales / autres cross.
- Courbe equity superposÃ©e pour les 3 LLMs.
- Matrice P&L net LLM x paire FX.
- Distribution des trades clos.
- Tables lots ouverts et trades clos.
- Coverage AG4-FX-V1 et bloc Risk Manager avec raisons de rejet + kill switch.

Variables d'environnement requises cÃ´tÃ© `trading-dashboard` :
- `AG1_FX_V1_CHATGPT52_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_chatgpt52.duckdb`
- `AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb`
- `AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb`
- `AG2_FX_V1_DUCKDB_PATH=/files/duckdb/ag2_fx_v1.duckdb`
- `AG4_FX_V1_DUCKDB_PATH=/files/duckdb/ag4_fx_v1.duckdb`

### 6.3 Page 1 â€” Dashboard Trading

Fonctions mÃ©tier :
- comparaison simultanÃ©e 3 portefeuilles AG1-V3 (GPT/Grok/Gemini),
- sÃ©lection `Focus` pour dÃ©tails portefeuille actif,
- scoreboard KPI (valeur, ROI, cash, DD, Sharpe, exposition, frais, score agent),
- graphe overlay compare (equity + optional drawdown),
- 5 onglets dÃ©tail :
  - `Allocation (actif)` :
    - rÃ©partition secteur/industrie/classe,
    - sparklines 90j par position (historique `yfinance-api /history`),
    - table positions enrichie.
  - `Rendement (actif)` :
    - sous-onglets `Rendement Financier`, `EfficacitÃ© du Capital`, `QualitÃ© du Trading`, `Risque`,
    - waterfall PnL, courbes equity, distributions trades/durÃ©es, drawdown/Sharpe/ProfitFactor.
  - `Cerveau IA (actif)` :
    - tables signaux + alertes AG1.
  - `MarchÃ© & Recherche (global)` :
    - sous-onglets `Macro & Buzz` + `Recherche`,
    - baromÃ¨tres sectoriels/news, top convictions, treemap opportunitÃ©s, scÃ©narios.
  - `Benchmarks & Indices` :
    - compare AG1 vs CAC40/S&P500/EURO STOXX 50,
    - mode base100 ou performance %, alpha vs benchmark rÃ©fÃ©rence.

### 6.4 Page 2 â€” System Health

Fonctions :
- freshness par symbole sur AG2 (tech), AG3 (fonda), AG4-SPE (news),
- freshness macro globale AG4,
- statut dernier run par workflow (RUNNING stale dÃ©tectÃ©, SUCCESS/PARTIAL/FAILED/NO_DATA),
- KPI statuts (`Ã€ jour`, `Ã€ surveiller`, `En retard`, `Manquant`),
- dÃ©tails filtrables par symbole/secteur.

### 6.5 Page 3 â€” Vue consolidÃ©e Multi-Agents

Fonctions :
- fusion AG2+AG3+AG4(+AG4-SPE)+YF enrichment,
- construction matrice Risk/Reward/probabilitÃ© (`_build_multi_agent_matrix`),
- dÃ©cisions finales :
  - `Entrer / Renforcer`
  - `Surveiller`
  - `RÃ©duire / Sortir`
- grades dynamiques A/B/C, gates data quality/earnings/liquiditÃ©/options,
- visualisation scatter interactive (sÃ©lection symbole â†’ fiche rapide),
- mode `Vue par valeur` avec :
  - KPI dÃ©tail,
  - panel dÃ©butant,
  - badges gates hard/soft,
  - trade card (copie texte/json),
  - audit data quality.

### 6.6 Page 4 â€” Macro & News (AG4)

Fonctions :
- fenÃªtre historique configurable,
- overview macro (alertes rÃ©gime/thÃ¨mes),
- news par valeur avec scopes (portefeuille actif/tous portefeuilles/universe),
- historique runs macro + spe,
- qualitÃ© pipeline news.

### 6.7 Page 5 â€” Analyse Technique V2 (AG2)

Fonctions :
- onglet `Vue d'ensemble` :
  - santÃ© run AG2,
  - KPI BUY/SELL/NEUTRAL/actionables/appels IA/approb IA,
  - graphes mix signal/heatmap/matrice H1-D1/funnel/scatter quality,
  - filtres rapides + top BUY/SELL/divergences.
- onglet `Vue dÃ©taillÃ©e` :
  - fiche symbole (KPI H1/D1/IA),
  - indicateurs visuels (RSI gauge, bars indicateurs),
  - alignement SMA,
  - chandeliers H1/D1 via `yfinance-api /history`,
  - carte analyse IA textuelle.
- onglet `Historique Runs` :
  - table runs + historique signaux filtrable.

### 6.8 Page 6 â€” Analyse Fondamentale V2 (AG3)

Fonctions :
- onglet `Vue d'ensemble` :
  - KPI conviction/risque/potentiel/couverture,
  - distribution score,
  - carte conviction vs risque,
  - qualitÃ© des runs dans le temps,
  - table synthÃ¨se triage.
- onglet `Vue dÃ©taillÃ©e` :
  - fiche symbole (triage/risque/horizon/upside/analystes),
  - gauges multi-facteurs,
  - table interprÃ©tation indicateurs,
  - Ã©volution historique symbole,
  - consensus analystes,
  - scÃ©narios 12 mois (Bear/Base/Bull) + historique prix.
- onglet `Historique Runs` :
  - KPIs dernier run + bar chart OK/erreurs + table historique.

### 6.9 Fonctions utilitaires transverses

- `app_modules/tables.py` : recherche globale, filtres colonnes (num/date/text), tri, rendu table interactive.
- `app_modules/visualizations.py` : prefetch concurrent `yfinance-api`, sparklines portefeuille, extraction events BUY/SELL.
- `app_modules/core.py` : parsing robuste, normalisation colonnes, enrichissement universe, calcul sentiment sectoriel et momentum symbole.

## 7. Inventaire des fonctions dashboard

> Pour le dÃ©tail exhaustif des noms de fonctions (snapshot 2026-03-02), consulter l'historique Git de ce fichier ou relancer un pass de dÃ©couverte sur `services/dashboard/app.py`.

Les modules sont structurÃ©s comme suit :

- **`services/dashboard/app.py`** : point d'entrÃ©e Streamlit, contient les loaders (Google Sheets + DuckDB), les rendus de chaque page, et les helpers UI (badges, KPI, charts).
- **`services/dashboard/app_modules/core.py`** : normalisation, fresheness check, valorisations, sentiment sectoriel, momentum symbole.
- **`services/dashboard/app_modules/tables.py`** : helpers de tri / recherche / filtres sur DataFrames.
- **`services/dashboard/app_modules/visualizations.py`** : sparklines portefeuille, extraction events de trade, prefetch concurrent historiques.
