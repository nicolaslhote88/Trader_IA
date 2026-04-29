# Brief d'implementation - Systeme Forex isole (AG1-FX-V1 + AG2-FX-V1 + AG4-FX-V1)

**Pour** : Codex5.4 (agent d'implementation)
**Auteur spec** : Nicolas + Claude (session du 25/04/2026)
**Statut** : Implemente par Codex le 25/04/2026 - cron ajustes 26/04/2026
**Priorite** : P0 (etape cle avant connexion broker FX live)
**Version** : v1.1
**Derniere mise a jour** : 2026-04-26

---

## 0. TL;DR

On clone (fork integral) la chaine V3 - AG2 + AG4 + AG1 - pour fabriquer un systeme **dedie exclusivement au Forex** : 27 paires, 3 PMs (1 par LLM), 10 000 EUR de capital chacun, levier 1 (configurable). Le systeme ne touche pas a la chaine actions/ETF/crypto existante : nouvelles bases DuckDB, nouveaux workflows n8n, nouvelle page dashboard. Le systeme actuel V3 continue a tourner sur la base `ag1_v3*.duckdb` sans modification.

Pourquoi maintenant : la base `ag4_forex_v1.duckdb` est en place depuis le 24/04, le geo-tagging d'AG4 fournit `impact_asset_class` et `impact_fx_pairs`, et l'audit segments du 23/04 a confirme que ni Grok ni ChatGPT ne tradent vraiment le FX dans V3 (Gemini seule, 4 lots, asymetrie defavorable). Un PM Forex dedie, avec son propre prompt et ses propres sources de news taguees, est necessaire pour produire un signal exploitable et mesurable avant tout passage live broker.

---

## 1. Decisions validees par Nicolas

| Date | Question | Choix |
|---|---|---|
| 25/04/2026 | **Perimetre** | Fork integral : AG2-FX-V1 + AG4-FX-V1 + AG1-FX-V1 (3 LLMs) - pas de branchement opportuniste sur l'existant. |
| 25/04/2026 | **Univers** | 27 paires (taxonomie identique a `ALLOWED_PAIRS` de AG4-V3). |
| 25/04/2026 | **Capital + levier** | 10 000 EUR par LLM, **levier = 1** au demarrage, **parametrable** dans `cfg.portfolio_config` (colonne `leverage_max`). |
| 25/04/2026 | **Livraison** | Spec complete d'abord (ce document), puis implementation. |
| **26/04/2026** | **Cron AG2-FX (technique)** | **6x/jour** sur l'amplitude forex 24/5 (0h, 4h, 8h, 12h, 16h, 20h Paris, lun-ven). |
| **26/04/2026** | **Cron AG4-FX (news)** | **2x/jour** dans la fenetre d'ouverture bourse FR 9h-17h30 (9h15 et 14h15 Paris, lun-ven). |
| **26/04/2026** | **Cron AG1-FX (PM)** | **2x/jour** par LLM, **decales de 15 min entre LLMs** pour eviter les conflits de lecture concurrente DuckDB. |

---

## 2. Contexte et motivation

### 2.1 Pourquoi un fork integral plutot qu'un branchement

La chaine V3 existante (AG1-V3 x 3 LLMs) trade **toutes classes d'actifs simultanement** : actions US, actions EU, ETF, crypto et forex se partagent le meme prompt, le meme budget de 50 kEUR initial, le meme Risk Manager et la meme base de donnees par LLM. Trois consequences genantes pour mesurer la performance Forex :

1. **Le signal Forex est noye.** Sur 50 kEUR de portefeuille, une position FX de quelques milliers d'euros ne pese pas dans la decision. Les 3 LLMs preferent statistiquement rester sur des tickers familiers (AAPL, NVDA, MSFT).
2. **Le prompt n'est pas specialise FX.** Les concepts de paires (devise base / devise quote), de pip, d'effet de levier et de calendrier macro ne sont pas appuyes. Le PM voit le forex comme un actif parmi d'autres.
3. **La performance n'est pas isolable.** Le P&L Forex apparait mele au P&L actions dans `core.position_lots` ; la nouvelle page dashboard "Forex P&L (LLM x Paire)" extrait le sous-ensemble FX a posteriori, mais ne permet pas de repondre a la question "quel rendement sur 10 kEUR alloue a 100 % au Forex".

Un systeme dedie resout ces 3 points : prompt FX-only, capital alloue, base isolee, cron et limites adaptes au rythme Forex.

### 2.2 Ce qu'on garde et ce qu'on duplique

**On garde** (lecture seule depuis le systeme Forex) :
- `ag4_v3.duckdb` (lecture des news taguees `impact_asset_class IN ('FX','Mixed')`).
- `ag4_forex_v1.duckdb` (lecture directe : c'est deja la base FX du projet).
- L'API `yfinance-api` (memes endpoints `/history` et `/info` pour les paires `XXXYYY=X`).
- L'image `task-runners` et toute l'infra Docker / Traefik.

**On duplique** (par fork) :
- AG2-V3 -> AG2-FX-V1 (technique FX-only, 27 paires).
- AG4-V3 -> AG4-FX-V1 (digest macro filtre FX qui combine `ag4_v3` + `ag4_forex_v1`).
- AG1-V3 -> AG1-FX-V1 (Portfolio Manager FX-only, 3 LLMs, 10 kEUR chacun).
- 3 bases `ag1_fx_v1_*.duckdb` (1 par LLM).
- 1 base `ag2_fx_v1.duckdb` (technique FX, mutualisee entre les 3 PMs FX).
- 1 base `ag4_fx_v1.duckdb` (digest macro FX, mutualisee entre les 3 PMs FX).

### 2.3 Hors scope de ce brief

- Connexion broker live (IG, Saxo, IBKR) - decision separee apres ~4 semaines de donnees AG1-FX-V1.
- Refonte du Risk Manager cote actions (issues #8 / #9 / #10 de `historique_issues.md`) - on **corrige ces 3 bugs uniquement dans le fork FX**, pas dans le systeme V3 actions.
- Optimisation du sourcing FX (sources `forexlive_main` etc.) - deja cadre par le brief AG4 geo-tagging du 24/04.
- Backtests historiques sur ces 3 mois - chantier separe.

---

## 3. Architecture cible

### 3.1 Arborescence

Note post-implementation : les agents Forex livres par ce brief vivent desormais sous
`agents/trading-forex/`, les agents actions sous `agents/trading-actions/`, et les agents
transverses sous `agents/common/`.

```
agents/
+-- trading-actions/
|   +-- AG1-V3-Portfolio manager/      # inchange (systeme actions/ETF/crypto)
|   +-- AG2-V3/                         # inchange
|   +-- AG4-V3/                         # inchange
|   +-- AG4-Forex/                      # inchange (sourcing FX en place depuis 24/04)
+-- trading-forex/
|   +-- AG1-FX-V1-Portfolio manager/    # NOUVEAU
|   |   +-- workflow/
|   |   |   +-- AG1_FX_workflow_template_v1.json
|   |   |   +-- AG1_FX_workflow_chatgpt52_v1.json
|   |   |   +-- AG1_FX_workflow_grok41_reasoning_v1.json
|   |   |   +-- AG1_FX_workflow_gemini30_pro_v1.json
|   |   +-- nodes/
|   |   |   +-- pre_agent/
|   |   |   |   +-- 01_init_run_fx.js
|   |   |   |   +-- 02_load_universe_fx.py
|   |   |   |   +-- 03_load_portfolio_state_fx.py
|   |   |   |   +-- 04_load_technical_signals_fx.py
|   |   |   |   +-- 05_load_news_macro_fx.py
|   |   |   |   +-- 06_assemble_brief_fx.js
|   |   |   +-- agent_input/
|   |   |   |   +-- 07_system_prompt_fx.md
|   |   |   |   +-- 08_user_prompt_fx.md
|   |   |   |   +-- 09_response_schema_fx.json
|   |   |   +-- post_agent/
|   |   |       +-- 10_parse_decision_fx.js
|   |   |       +-- 11_validate_enforce_safety_fx.js   # Risk Manager FX (issues 8/9/10)
|   |   |       +-- 12_simulate_fills_fx.py
|   |   |       +-- 13_write_orders_fx.py
|   |   |       +-- 14_write_lots_fx.py
|   |   |       +-- 15_close_lots_fx.py
|   |   |       +-- 16_snapshot_portfolio_fx.py
|   |   |       +-- 17_log_run_fx.py
|   |   +-- generate_model_variants.py
|   |   +-- README.md
|   +-- AG2-FX-V1/                      # NOUVEAU
|   |   +-- workflow/
|   |   |   +-- AG2_FX_workflow_v1.json
|   |   +-- nodes/
|   |   |   +-- 01_init_config_fx.js
|   |   |   +-- 02_load_fx_universe.py
|   |   |   +-- 03_fetch_yfinance_fx.py
|   |   |   +-- 04_compute_indicators_fx.py
|   |   |   +-- 05_compute_levels_fx.py
|   |   |   +-- 06_compute_regime_fx.py
|   |   |   +-- 07_score_signal_fx.py
|   |   |   +-- 08_write_universe_fx.py
|   |   |   +-- 09_write_signals_fx.py
|   |   |   +-- 10_log_run_fx.py
|   |   +-- README.md
|   +-- AG4-FX-V1/                      # NOUVEAU
|       +-- workflow/
|       |   +-- AG4_FX_workflow_v1.json
|       +-- nodes/
|       |   +-- 01_init_run_fx.js
|       |   +-- 02_pull_global_fx_news.py    # lit ag4_v3 WHERE impact_asset_class IN ('FX','Mixed')
|       |   +-- 03_pull_fx_channel_news.py   # lit ag4_forex_v1.fx_news_history
|       |   +-- 04_dedupe_and_score.py
|       |   +-- 05_compute_fx_macro_digest.py
|       |   +-- 06_write_digest_fx.py
|       |   +-- 07_log_run_fx.py
|       +-- README.md

infra/
+-- migrations/
|   +-- ag1_fx_v1/
|   |   +-- 20260426_init.sql
|   +-- ag2_fx_v1/
|   |   +-- 20260426_init.sql
|   +-- ag4_fx_v1/
|       +-- 20260426_init.sql
+-- vps_hostinger_config/
    +-- docker-compose.yml             # env vars + volumes ajoutes
```

### 3.2 Bases DuckDB

| Base | Role | Lecture | Ecriture |
|---|---|---|---|
| `ag2_fx_v1.duckdb` | Snapshots techniques FX (univers + signaux + indicateurs). | yfinance-api | AG2-FX-V1 |
| `ag4_fx_v1.duckdb` | Digest macro FX (combine global + specifique). | `ag4_v3`, `ag4_forex_v1` | AG4-FX-V1 |
| `ag1_fx_v1_chatgpt52.duckdb` | Portfolio FX du PM ChatGPT 5.2. | tous les ag*_fx | AG1-FX-V1 / chatgpt52 |
| `ag1_fx_v1_grok41_reasoning.duckdb` | Portfolio FX du PM Grok 4.1 Reasoning. | tous les ag*_fx | AG1-FX-V1 / grok41 |
| `ag1_fx_v1_gemini30_pro.duckdb` | Portfolio FX du PM Gemini 3.0 Pro. | tous les ag*_fx | AG1-FX-V1 / gemini30 |

Les 5 bases vivent dans `/local-files/duckdb/` (volume `/local-files` deja monte cote n8n + dashboard).

---

## 4. Cron schedules (v1.1, ajustes 26/04/2026)

### 4.1 Tableau recapitulatif

| Workflow | Cron expression | Heures Paris | Frequence | Justification |
|---|---|---|---|---|
| **AG2-FX-V1** | `0 0,4,8,12,16,20 * * 1-5` | 0h, 4h, 8h, 12h, 16h, 20h | 6x/jour | Forex 24/5 -> couverture sessions Asie / Europe / US ; signaux techniques rafraichis toutes les 4h. |
| **AG4-FX-V1** | `15 9,14 * * 1-5` | 9h15, 14h15 | 2x/jour | Fenetre ouverture bourse FR (9h-17h30) ; matin = post-ouverture EU + macro asiatique nuit, apres-midi = pre-ouverture US. |
| **AG1-FX-V1 chatgpt52** | `30 9,14 * * 1-5` | 9h30, 14h30 | 2x/jour | Run apres AG2 (8h) + AG4 (9h15) le matin ; apres AG2 (12h) + AG4 (14h15) l'apres-midi. |
| **AG1-FX-V1 grok41_reasoning** | `45 9,14 * * 1-5` | 9h45, 14h45 | 2x/jour | +15 min vs chatgpt52 pour etaler la charge runner et eviter conflits lecture concurrente DuckDB. |
| **AG1-FX-V1 gemini30_pro** | `0 10,15 * * 1-5` | 10h00, 15h00 | 2x/jour | +30 min vs chatgpt52. Reste dans la fenetre d'ouverture bourse FR (9h-17h30). |
| **AG1-FX-PF-V1 valuation** | `0 0 * * * 1-5` | toutes les heures | 24x/jour lun-ven | Mark-to-market horaire des 3 bases AG1-FX. Met a jour `core.portfolio_snapshot` sans decision LLM. |

### 4.2 Frise temporelle journee type (lun-ven)

```
00h  04h  08h  09h15  09h30  09h45  10h00  12h  14h15  14h30  14h45  15h00  16h  20h
 |    |    |     |      |      |      |     |    |      |      |      |     |    |
 AG2  AG2  AG2   AG4    AG1A   AG1B   AG1C  AG2  AG4    AG1A   AG1B   AG1C  AG2  AG2
 (technique 6x/j -- forex 24/5)        (PM matin)               (PM apres-midi)

Legende :
- AG2  = AG2-FX-V1 (technique, 6x/j, mutualise entre les 3 PMs)
- AG4  = AG4-FX-V1 (news macro FX, 2x/j, mutualise entre les 3 PMs)
- AG1A = chatgpt52    (run +15 min apres AG4)
- AG1B = grok41       (run +30 min apres AG4)
- AG1C = gemini30_pro (run +45 min apres AG4)
```

### 4.3 Garanties cherches

1. **Pas de conflit DuckDB** : les 3 PMs lisent `ag2_fx_v1.duckdb` et `ag4_fx_v1.duckdb` en concurrence. Le decalage 15 min garantit que chaque LLM a son propre creneau de lecture, sans collision avec un autre PM ni avec AG4 (qui ecrit ces bases).
2. **Donnees fraiches** : AG2 tourne **avant** AG4, AG4 tourne **avant** AG1. La sequence garantit que chaque PM lit le dernier snapshot technique + le dernier digest macro.
3. **Couverture forex 24/5** : AG2 capte les sessions Asie (4h, 8h Paris), Europe (8h, 12h, 16h Paris) et US (16h, 20h Paris). Les 6 runs/j permettent au PM, qui tourne lui en heures bourse FR, de toujours disposer d'un snapshot recent (max 4h d'age).
4. **Cadence PM compatible discretion humaine** : 2 runs/j (matin + apres-midi) reste comparable au comportement d'un trader particulier ; pas de scalping algorithmique.

### 4.4 Source de verite

Le fichier `agents/trading-forex/AG1-FX-V1-Portfolio manager/generate_model_variants.py` est la **source de verite** des cron AG1. Il regenere les 3 fichiers `AG1_FX_workflow_*_v1.json` a partir du template. Les cron des 3 fichiers ne doivent jamais etre edites a la main.

Pour AG2-FX-V1 et AG4-FX-V1, le cron est defini directement dans le node `Schedule Trigger` de chaque workflow JSON (un seul fichier par agent).

Pour AG1-FX-PF-V1, le cron horaire est defini dans `agents/trading-forex/AG1-FX-PF-V1/build_workflow.py`, qui genere `AG1-FX-PF-V1-workflow.json`.

---

## 5. AG2-FX-V1 - fork technique

### 5.1 Univers FX

27 paires, alignees strictement sur `ALLOWED_PAIRS` de `agents/common/AG4-V3/nodes/10_parse_llm_output.js` (ligne 95). Format **sans slash** (`EURUSD`, `USDJPY`, ...).

Symbole yfinance : `<pair>=X` (ex. `EURUSD=X`).

```
EURUSD GBPUSD USDJPY USDCHF AUDUSD NZDUSD USDCAD
EURGBP EURJPY EURCHF EURAUD EURNZD EURCAD
GBPJPY GBPCHF GBPAUD GBPNZD GBPCAD
AUDJPY AUDNZD AUDCAD NZDJPY NZDCAD CADJPY
CHFJPY USDCNH USDMXN
```

(27 paires - recompter contre AG4-V3 lors de l'implementation et **fail si ecart**.)

### 5.2 Schema `ag2_fx_v1.duckdb`

```sql
CREATE SCHEMA IF NOT EXISTS main;

CREATE TABLE IF NOT EXISTS main.universe_fx (
    pair                VARCHAR PRIMARY KEY,
    symbol_yf           VARCHAR NOT NULL,         -- 'EURUSD=X'
    base_ccy            VARCHAR NOT NULL,
    quote_ccy           VARCHAR NOT NULL,
    pip_size            DOUBLE NOT NULL,          -- 0.0001 sauf JPY (0.01)
    price_decimals      INTEGER NOT NULL,         -- 4 sauf JPY (2)
    liquidity_tier      VARCHAR,                  -- 'major' | 'cross' | 'exotic'
    enabled             BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main.technical_signals_fx (
    run_id              VARCHAR NOT NULL,
    as_of               TIMESTAMP NOT NULL,
    pair                VARCHAR NOT NULL,
    last_close          DOUBLE,
    ret_1d              DOUBLE,
    ret_5d              DOUBLE,
    ret_20d             DOUBLE,
    rsi14               DOUBLE,
    atr14               DOUBLE,
    sma20               DOUBLE,
    sma50               DOUBLE,
    sma200              DOUBLE,
    ema12               DOUBLE,
    ema26               DOUBLE,
    macd                DOUBLE,
    macd_signal         DOUBLE,
    macd_hist           DOUBLE,
    bb_upper            DOUBLE,
    bb_lower            DOUBLE,
    bb_width            DOUBLE,
    pivot               DOUBLE,
    r1                  DOUBLE,
    r2                  DOUBLE,
    s1                  DOUBLE,
    s2                  DOUBLE,
    regime              VARCHAR,                  -- 'trend_up' | 'trend_down' | 'range' | 'breakout'
    signal_score        DOUBLE,                   -- [-1, +1]
    signal_label        VARCHAR,                  -- 'strong_buy' | 'buy' | 'neutral' | 'sell' | 'strong_sell'
    pip_size            DOUBLE,
    base_ccy            VARCHAR,
    quote_ccy           VARCHAR,
    PRIMARY KEY (run_id, pair)
);

CREATE INDEX IF NOT EXISTS idx_tsfx_pair ON main.technical_signals_fx(pair);
CREATE INDEX IF NOT EXISTS idx_tsfx_asof ON main.technical_signals_fx(as_of);

CREATE TABLE IF NOT EXISTS main.run_log (
    run_id              VARCHAR PRIMARY KEY,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    pairs_fetched       INTEGER,
    pairs_with_signal   INTEGER,
    errors              INTEGER,
    notes               VARCHAR
);
```

### 5.3 Differences notables vs AG2-V3

- **Pas de table `universe` partagee** : les paires FX sont gerees separement (champs `base_ccy`, `quote_ccy`, `pip_size`, `price_decimals`).
- **Pas d'indicateurs sectoriels** (les paires n'ont pas de secteur).
- **`signal_score` borne [-1, +1]** pour faciliter la consommation par le PM.
- Le node `11_build_vector_docs_fx.py` reste un best-effort si Qdrant est utilise ; sinon, le supprimer pour la v1.

---

## 6. AG4-FX-V1 - digest macro FX

### 6.1 Role

Preparer un **digest unique** que le PM consomme : top 30 news FX-pertinentes des 24 dernieres heures, regroupees par paire, avec le regime macro courant (`fx_macro` lecture) et le bias par paire (`fx_pairs` lecture).

### 6.2 Sources

1. `ag4_v3.main.news_history` filtree `impact_asset_class IN ('FX','Mixed')`, fenetre 24h glissantes.
2. `ag4_forex_v1.main.fx_news_history` filtree `published_at >= now() - interval '24 hours'`.
3. `ag4_forex_v1.main.fx_macro` (dernier `as_of`).
4. `ag4_forex_v1.main.fx_pairs` (dernier `as_of` par `pair`).

### 6.3 Schema `ag4_fx_v1.duckdb`

```sql
CREATE SCHEMA IF NOT EXISTS main;

CREATE TABLE IF NOT EXISTS main.fx_digest (
    run_id              VARCHAR NOT NULL,
    as_of               TIMESTAMP NOT NULL,
    section             VARCHAR NOT NULL,         -- 'top_news' | 'pair_focus' | 'macro_regime'
    payload             VARCHAR NOT NULL,         -- JSON serialise pret a injecter dans le brief PM
    items_count         INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, section)
);

CREATE INDEX IF NOT EXISTS idx_fxdigest_asof ON main.fx_digest(as_of);

CREATE TABLE IF NOT EXISTS main.run_log (
    run_id                  VARCHAR PRIMARY KEY,
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP,
    news_global_pulled      INTEGER,
    news_fx_channel_pulled  INTEGER,
    news_after_dedupe       INTEGER,
    sections_written        INTEGER,
    errors                  INTEGER,
    notes                   VARCHAR
);
```

### 6.4 Format du payload `top_news`

```json
{
  "items": [
    {
      "dedupe_key": "...",
      "published_at": "2026-04-25T07:12:00Z",
      "title": "ECB hints at June cut as inflation cools",
      "source": "Reuters",
      "snippet": "Eurozone CPI fell to 2.1%...",
      "impact_magnitude": "high",
      "impact_fx_pairs": ["EURUSD", "EURGBP", "EURJPY"],
      "currencies_bullish": ["USD"],
      "currencies_bearish": ["EUR"],
      "fx_directional_hint": "EUR weakness short-term",
      "origin": "global_base"
    }
  ],
  "as_of": "2026-04-25T09:15:00+02:00",
  "lookback_hours": 24
}
```

### 6.5 Format du payload `pair_focus`

Une entree par paire qui apparait dans au moins une news des 24h, ou dans `fx_pairs.directional_bias`. Attention : on **agrege par paire** pour eviter de repeter 30 news quasi-identiques.

```json
{
  "pairs": {
    "EURUSD": {
      "news_count_24h": 7,
      "bias_news": "bearish_eur",
      "bias_macro": "bearish_eur",
      "confidence": 0.72,
      "top_drivers": ["ECB June cut hint", "US Q1 GDP beat"],
      "urgent_event_within_4h": false
    }
  }
}
```

### 6.6 Format du payload `macro_regime`

Lecture directe du dernier `fx_macro` :

```json
{
  "market_regime": "risk_off",
  "drivers": "geopolitical tension Middle East, oil spike",
  "confidence": 0.65,
  "biases": {"USD": 0.5, "EUR": -0.2, "JPY": 0.3},
  "as_of": "2026-04-25T06:30:00Z"
}
```

---

## 7. AG1-FX-V1 - Portfolio Manager Forex

### 7.1 Schema `ag1_fx_v1_*.duckdb` (1 base par LLM, schema identique)

Reprend les tables `core.*` et `cfg.*` de AG1-V3 (`agents/trading-actions/AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql`) avec les **adaptations FX suivantes** :

```sql
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS cfg;

-- ----------------------------------------------------------------------
-- cfg.portfolio_config (NOUVEAU FX)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cfg.portfolio_config (
    config_key                VARCHAR PRIMARY KEY,
    llm_model                 VARCHAR NOT NULL,
    initial_capital_eur       DOUBLE NOT NULL DEFAULT 10000,
    leverage_max              DOUBLE NOT NULL DEFAULT 1.0,
    max_pos_pct               DOUBLE NOT NULL DEFAULT 0.20,
    max_pair_pct              DOUBLE NOT NULL DEFAULT 0.20,
    max_currency_exposure_pct DOUBLE NOT NULL DEFAULT 0.50,
    max_daily_drawdown_pct    DOUBLE NOT NULL DEFAULT 0.05,
    kill_switch_active        BOOLEAN NOT NULL DEFAULT FALSE,
    universe_filter           VARCHAR,
    notes                     VARCHAR,
    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------
-- core.runs
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.runs (
    run_id              VARCHAR PRIMARY KEY,
    llm_model           VARCHAR NOT NULL,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    decision_json       VARCHAR,
    decisions_count     INTEGER,
    orders_count        INTEGER,
    fills_count         INTEGER,
    errors              INTEGER,
    leverage_max_used   DOUBLE,
    kill_switch_active  BOOLEAN,
    notes               VARCHAR
);

-- ----------------------------------------------------------------------
-- core.orders (FX adapte)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.orders (
    order_id            VARCHAR PRIMARY KEY,
    client_order_id     VARCHAR NOT NULL UNIQUE,  -- issue #8 corrigee : present et unique
    run_id              VARCHAR NOT NULL,
    pair                VARCHAR NOT NULL,
    side                VARCHAR NOT NULL,         -- 'buy_base' | 'sell_base' | 'close_long' | 'close_short'
    order_type          VARCHAR NOT NULL,         -- 'market' | 'limit'
    size_lots           DOUBLE NOT NULL,
    notional_quote      DOUBLE NOT NULL,
    notional_eur        DOUBLE NOT NULL,
    leverage_used       DOUBLE NOT NULL DEFAULT 1.0,
    limit_price         DOUBLE,
    stop_loss_price     DOUBLE,
    take_profit_price   DOUBLE,
    requested_at        TIMESTAMP NOT NULL,
    status              VARCHAR NOT NULL,         -- 'pending' | 'filled' | 'rejected' | 'cancelled'
    rejection_reason    VARCHAR,
    risk_check_passed   BOOLEAN NOT NULL DEFAULT TRUE,
    risk_check_notes    VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_orders_run ON core.orders(run_id);
CREATE INDEX IF NOT EXISTS idx_orders_pair ON core.orders(pair);

-- ----------------------------------------------------------------------
-- core.fills
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.fills (
    fill_id             VARCHAR PRIMARY KEY,
    order_id            VARCHAR NOT NULL,
    pair                VARCHAR NOT NULL,
    side                VARCHAR NOT NULL,
    fill_price          DOUBLE NOT NULL,
    fill_size_lots      DOUBLE NOT NULL,
    fees_eur            DOUBLE NOT NULL DEFAULT 0,
    swap_eur            DOUBLE NOT NULL DEFAULT 0,
    filled_at           TIMESTAMP NOT NULL,
    fill_source         VARCHAR DEFAULT 'simulated_yfinance'
);

-- ----------------------------------------------------------------------
-- core.position_lots (FX adapte)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.position_lots (
    lot_id              VARCHAR PRIMARY KEY,
    run_id_open         VARCHAR NOT NULL,
    run_id_close        VARCHAR,
    pair                VARCHAR NOT NULL,
    side                VARCHAR NOT NULL,         -- 'long' | 'short'
    size_lots           DOUBLE NOT NULL,
    open_price          DOUBLE NOT NULL,
    open_at             TIMESTAMP NOT NULL,
    close_price         DOUBLE,
    close_at            TIMESTAMP,
    pnl_quote           DOUBLE,
    pnl_eur             DOUBLE,
    fees_eur            DOUBLE DEFAULT 0,
    swap_eur_total      DOUBLE DEFAULT 0,
    leverage_used       DOUBLE NOT NULL DEFAULT 1.0,
    stop_loss_price     DOUBLE,
    take_profit_price   DOUBLE,
    status              VARCHAR NOT NULL,         -- 'open' | 'closed'
    notes               VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_lots_pair ON core.position_lots(pair);
CREATE INDEX IF NOT EXISTS idx_lots_status ON core.position_lots(status);
CREATE INDEX IF NOT EXISTS idx_lots_open_at ON core.position_lots(open_at);

-- ----------------------------------------------------------------------
-- core.portfolio_snapshot
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.portfolio_snapshot (
    snapshot_id         VARCHAR PRIMARY KEY,
    run_id              VARCHAR NOT NULL,
    as_of               TIMESTAMP NOT NULL,
    cash_eur            DOUBLE NOT NULL,
    equity_eur          DOUBLE NOT NULL,
    margin_used_eur     DOUBLE NOT NULL DEFAULT 0,
    margin_free_eur     DOUBLE NOT NULL,
    leverage_effective  DOUBLE,
    open_lots_count     INTEGER NOT NULL,
    pnl_day_eur         DOUBLE,
    pnl_total_eur       DOUBLE,
    drawdown_day_pct    DOUBLE,
    drawdown_total_pct  DOUBLE,
    notes               VARCHAR
);

-- ----------------------------------------------------------------------
-- core.cash_ledger
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.cash_ledger (
    ledger_id           VARCHAR PRIMARY KEY,
    run_id              VARCHAR,
    as_of               TIMESTAMP NOT NULL,
    movement_type       VARCHAR NOT NULL,         -- 'deposit' | 'fill_open' | 'fill_close' | 'fees' | 'swap'
    amount_eur          DOUBLE NOT NULL,
    balance_after_eur   DOUBLE NOT NULL,
    related_lot_id      VARCHAR,
    notes               VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_cash_asof ON core.cash_ledger(as_of);

-- ----------------------------------------------------------------------
-- core.ai_signals
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.ai_signals (
    signal_id           VARCHAR PRIMARY KEY,
    run_id              VARCHAR NOT NULL,
    pair                VARCHAR NOT NULL,
    decision            VARCHAR NOT NULL,         -- 'open_long' | 'open_short' | 'close' | 'hold'
    conviction          DOUBLE,
    rationale           VARCHAR,
    target_size_lots    DOUBLE,
    stop_loss_price     DOUBLE,
    take_profit_price   DOUBLE,
    horizon             VARCHAR,                  -- 'intraday' | '1d' | '3d' | '1w'
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------
-- core.alerts
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.alerts (
    alert_id            VARCHAR PRIMARY KEY,
    run_id              VARCHAR,
    occurred_at         TIMESTAMP NOT NULL,
    severity            VARCHAR NOT NULL,         -- 'info' | 'warn' | 'error' | 'critical'
    category            VARCHAR NOT NULL,         -- 'risk' | 'data' | 'execution' | 'kill_switch'
    message             VARCHAR NOT NULL,
    payload             VARCHAR
);
```

### 7.2 Univers FX (snapshot au demarrage)

Le node `02_load_universe_fx.py` lit `ag2_fx_v1.main.universe_fx` et applique le filtre `cfg.portfolio_config.universe_filter` (default `'forex_27'`).

### 7.3 Pre-LLM nodes - assemblage du brief

`06_assemble_brief_fx.js` produit un objet JSON unique transmis au LLM. Structure :

```json
{
  "run": {"run_id": "...", "as_of": "2026-04-25T09:30:00+02:00", "llm_model": "gpt-5.2-2025-12-11"},
  "config": {"capital_eur": 10000, "leverage_max": 1.0, "kill_switch_active": false},
  "portfolio_state": {
    "cash_eur": 9847.30,
    "equity_eur": 9912.40,
    "open_lots": [{"lot_id": "...", "pair": "EURUSD", "side": "long", "size_lots": 0.1, "open_price": 1.0712, "current_price": 1.0735, "pnl_eur": 23.0}],
    "leverage_effective": 0.10,
    "drawdown_total_pct": -0.0088
  },
  "universe": {"pairs": ["EURUSD", "GBPUSD"]},
  "technical_signals": [
    {"pair": "EURUSD", "regime": "trend_up", "rsi14": 56.2, "signal_label": "buy", "signal_score": 0.42, "atr14": 0.0042, "last_close": 1.0735}
  ],
  "macro_news": {
    "top_news": [],
    "pair_focus": {},
    "macro_regime": {}
  },
  "limits": {
    "max_pair_pct": 0.20,
    "max_currency_exposure_pct": 0.50,
    "max_daily_drawdown_pct": 0.05
  }
}
```

### 7.4 Prompt LLM - `07_system_prompt_fx.md`

```
You are the Forex Portfolio Manager for a 10,000 EUR sandbox account managed by {{llm_model}}.
You trade ONLY the 27 FX pairs listed in the universe. No equities, no ETFs, no crypto.
Your job at each run is to:

1. Read the current portfolio state, the technical signals (AG2-FX) and the FX-specific macro news digest (AG4-FX).
2. Decide for each open lot: keep, partial close, or full close.
3. Decide whether to open new positions (max 5 per run), specifying for each: pair, side (long/short), size_lots, stop_loss_price, take_profit_price, horizon, conviction (0-1), rationale.
4. Respect HARD constraints (the Risk Manager will reject violators):
   - leverage_max = {{leverage_max}} -> sum(notional_eur) / equity_eur must stay <= leverage_max
   - max_pair_pct = 20% -> notional_eur per pair / equity_eur <= 0.20
   - max_currency_exposure_pct = 50% -> cumulative directional exposure on any single currency / equity_eur <= 0.50
   - max_daily_drawdown_pct = 5% -> if breached, kill_switch flips and all opens are blocked

5. Trading style: short to medium term (intraday to 1 week). Do NOT scalp; favor moves of 30+ pips with conviction.
6. Always reason from the news + macro regime first, then confirm with technicals. Do not open against a strong macro bias.
7. If macro regime is unclear OR no high-conviction setup exists, return decision='hold' for all pairs.

Return a single JSON object matching the response schema. Do not output anything else.
```

### 7.5 User prompt - `08_user_prompt_fx.md`

Insertion litterale du brief assemble en 7.3 (serialise JSON, encadre par 3 backticks).

### 7.6 JSON response schema - `09_response_schema_fx.json`

```json
{
  "type": "object",
  "required": ["as_of", "decisions"],
  "properties": {
    "as_of": {"type": "string"},
    "narrative": {"type": "string", "maxLength": 4000},
    "decisions": {
      "type": "array",
      "maxItems": 30,
      "items": {
        "type": "object",
        "required": ["pair", "decision", "conviction"],
        "properties": {
          "pair": {"type": "string", "pattern": "^[A-Z]{6}$"},
          "decision": {"type": "string", "enum": ["open_long", "open_short", "close", "partial_close", "hold"]},
          "conviction": {"type": "number", "minimum": 0, "maximum": 1},
          "size_lots": {"type": "number", "minimum": 0, "maximum": 5},
          "size_pct_equity": {"type": "number", "minimum": 0, "maximum": 0.2},
          "stop_loss_price": {"type": "number"},
          "take_profit_price": {"type": "number"},
          "horizon": {"type": "string", "enum": ["intraday", "1d", "3d", "1w"]},
          "rationale": {"type": "string", "maxLength": 600},
          "lot_id_to_close": {"type": "string"}
        }
      }
    }
  }
}
```

### 7.7 Risk Manager FX - `11_validate_enforce_safety_fx.js`

**Reprend la structure de `agents/trading-actions/AG1-V3-Portfolio manager/nodes/post_agent/07_validate_enforce_safety_v5.code.js` avec les corrections suivantes** (issues #8/9/10 de `historique_issues.md`) :

Pipeline de checks (un echec -> ordre rejete avec `rejection_reason`, pas crash) :

1. **Read kill switch** (NOUVEAU vs V3 #9) : `SELECT kill_switch_active FROM cfg.portfolio_config WHERE config_key='default'`. Si `TRUE` -> toutes les decisions `open_*` deviennent `hold`. Les `close` restent autorises.
2. **Parse + normalize FX actions** : convertir `decision` en `side` (`open_long`->`buy_base`, `open_short`->`sell_base`, etc.), convertir `size_pct_equity` en `size_lots` si seule l'une est fournie.
3. **Compute `client_order_id`** (NOUVEAU vs V3 #8) : `${run_id}::${pair}::${side}::${seq}` - unique, idempotent.
4. **Universe check** : `pair` doit etre dans `universe_fx.pair WHERE enabled=TRUE`. Sinon reject.
5. **Cash + margin affordability** : `notional_eur / leverage_max <= margin_free_eur`. Sinon reject.
6. **Max pair exposure** (NOUVEAU vs V3 #10 PARTIAL) : projeter `notional_eur(pair) / equity_eur` post-fill, verifier `<= max_pair_pct`. Sinon reject (pour la v1, on rejette plutot que de redimensionner pour ne pas masquer les erreurs de sizing du LLM).
7. **Max currency exposure** : pour chaque devise (USD, EUR, JPY), calculer l'exposition directionnelle nette projetee (`sum(notional_eur signed)`), verifier `abs() / equity_eur <= max_currency_exposure_pct`. Sinon reject.
8. **Leverage check** : `sum(all_open_notional_eur post-fill) / equity_eur <= leverage_max`. Sinon reject.
9. **Daily drawdown gate** (NOUVEAU vs V3 #10 PARTIAL) : si `drawdown_day_pct <= -max_daily_drawdown_pct` au debut du run -> flip `kill_switch_active = TRUE` (UPDATE `cfg.portfolio_config`), bloquer tous les opens, ecrire alert `severity='critical' category='kill_switch'`.
10. **Stop / TP sanity** : `stop_loss_price` et `take_profit_price` doivent etre du bon cote (long -> SL < entry < TP).
11. **Build executable orders** : pour chaque decision validee, ecrire dans `core.orders` avec `risk_check_passed=TRUE`, `client_order_id`. Les rejets sont aussi traces (`status='rejected'`, `rejection_reason` rempli).

### 7.8 Simulation des fills

`12_simulate_fills_fx.py` :
- Pour chaque ordre `pending` : prix de fill = mid-price du dernier `technical_signals_fx.last_close` + slippage 1 pip.
- `fees_eur` = `0.5 * notional_eur / 10000` (= ~0.005 % du notional, ordre de grandeur retail FX). **A calibrer.** Ce calcul doit etre visible dans le code (variable `FEE_BPS = 0.5`).
- `swap_eur` = 0 si ferme le jour meme, sinon preleve en `19_overnight_swap.py` (hors v1, a laisser en TODO).

### 7.9 Generation des variants 3 LLMs (v1.1)

`generate_model_variants.py` (calque sur `agents/trading-actions/AG1-V3-Portfolio manager/generate_model_variants.py`) prend le template et produit 3 workflows :

| Variant | LLM | Cron Paris | Cron expression |
|---|---|---|---|
| `chatgpt52` | `gpt-5.2-2025-12-11` | 9h30, 14h30 lun-ven | `30 9,14 * * 1-5` |
| `grok41_reasoning` | `grok-4-1-fast-reasoning` | 9h45, 14h45 lun-ven | `45 9,14 * * 1-5` |
| `gemini30_pro` | `models/gemini-3-pro-preview` | 10h00, 15h00 lun-ven | `0 10,15 * * 1-5` |

(Decalage 15 min entre LLMs pour eviter les conflits de lecture concurrente DuckDB sur les bases mutualisees `ag2_fx_v1.duckdb` et `ag4_fx_v1.duckdb`.)

---

## 8. Dashboard - page "Forex Trading (AG1-FX)"

### 8.1 Difference avec la page existante "Forex P&L (LLM x Paire)"

| Page | Source | Angle |
|---|---|---|
| **Forex P&L (LLM x Paire)** (existante) | extrait FX du portfolio AG1-V3 multi-actifs | Voir si les 3 LLMs *quand ils tradent FX* dans le systeme global s'en sortent. |
| **Forex Trading (AG1-FX)** (NOUVELLE) | bases dediees `ag1_fx_v1_*.duckdb` | Suivre les 3 portfolios FX-only a 10 kEUR, comparer leur perf sur le meme univers. |

Les deux coexistent : la premiere reste utile pour mesurer "quand les LLMs choisissent de trader FX dans un univers libre", la seconde pour mesurer "quand on les force a ne faire QUE du FX".

### 8.2 Contenu de la page

1. **En-tete** : selecteur de periode (default = depuis le 1er run AG1-FX-V1), selecteur de granularite (jour / semaine), bouton "filtrer ouvertures uniquement".
2. **3 cartes KPI par LLM** : Capital initial 10 000 EUR / Equity actuelle / P&L total EUR + % / P&L jour, P&L semaine / Drawdown courant, drawdown max / Nombre de lots ouverts, notional ouvert, leverage effectif / Winrate clos, profit factor.
3. **Courbe equity** : 3 series superposees (1 par LLM), couleurs `AG1_FX_MULTI_PORTFOLIO_CONFIG[k]['accent']`.
4. **Matrice LLM x Paire** : P&L net cumule par paire et par LLM, `background_gradient(cmap='RdYlGn', vmin/vmax symetriques)`.
5. **Distribution des trades clos** : histogramme P&L par LLM (overlay), avec marker median.
6. **Tableau lots ouverts** : `pair`, `side`, `size_lots`, `notional_eur`, `open_price`, `current_price`, `pnl_floating_eur`, `held_for`.
7. **Tableau trades clos** : 50 derniers, triables.
8. **AG4-FX-V1 coverage** : nombre de news ingerees 24h, dernier run AG4-FX-V1, top 10 paires news, regime macro courant.
9. **Risk Manager** : nombre d'ordres rejetes par categorie sur la periode, top 5 raisons de rejet, etat du `kill_switch_active`.

### 8.3 Implementation

Ajouter dans `services/dashboard/app.py` une nouvelle entree de menu radio `"Forex Trading (AG1-FX)"`, similaire a `"Forex P&L (LLM x Paire)"`. Reutilise massivement les helpers existants (`_fx_prepare_symbol_frame`, `AG1_FX_MULTI_PORTFOLIO_CONFIG`, `_duckdb_connect_readonly_retry`, `render_interactive_table`).

Variables d'env a brancher :

```
AG1_FX_V1_CHATGPT52_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_chatgpt52.duckdb
AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb
AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb
AG2_FX_V1_DUCKDB_PATH=/files/duckdb/ag2_fx_v1.duckdb
AG4_FX_V1_DUCKDB_PATH=/files/duckdb/ag4_fx_v1.duckdb
```

---

## 9. Variables d'env, docker-compose, secrets

### 9.1 `infra/vps_hostinger_config/docker-compose.yml`

Ajouter dans **n8n** + **task-runners** + **trading-dashboard** les variables :

```yaml
- AG1_FX_V1_CHATGPT52_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_chatgpt52.duckdb
- AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb
- AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb
- AG2_FX_V1_DUCKDB_PATH=/files/duckdb/ag2_fx_v1.duckdb
- AG4_FX_V1_DUCKDB_PATH=/files/duckdb/ag4_fx_v1.duckdb
- AG1_FX_V1_WRITER_PATH=/files/AG1-FX-V1-EXPORT/nodes/post_agent/duckdb_writer.py
- AG1_FX_V1_LEDGER_SCHEMA_PATH=/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql
```

Le volume `/local-files:/files` est deja monte cote n8n et runners - pas de changement.

### 9.2 `docs/operations/env_vars.md`

Ajouter une nouvelle section "Variables internes au systeme Forex AG1-FX-V1" listant les 7 variables ci-dessus.

### 9.3 Secrets API LLM

Aucun nouveau secret : on reutilise les credentials n8n existants (`OpenAI`, `xAI Grok`, `Google Vertex AI`).

---

## 10. Plan de deploiement

| Phase | Description | Critere go |
|---|---|---|
| **P0** | Migrations + schemas. Creer les 5 bases `.duckdb` vides via les SQL `infra/migrations/ag*_fx_v1/20260426_init.sql`. Seed `cfg.portfolio_config` avec capital=10000, leverage=1, kill_switch=false. | `SELECT count(*) FROM cfg.portfolio_config` retourne 1 ligne sur chaque base AG1. |
| **P1** | AG2-FX-V1 deploye. Activer cron **`0 0,4,8,12,16,20 * * 1-5`** (6x/j). Verifier 27 paires ecrites dans `technical_signals_fx` apres 1 run. | `SELECT count(DISTINCT pair) FROM technical_signals_fx WHERE run_id = (SELECT max(run_id) FROM run_log)` = 27. |
| **P2** | AG4-FX-V1 deploye. Cron **`15 9,14 * * 1-5`** (2x/j, fenetre bourse FR). Verifier 3 sections (`top_news`, `pair_focus`, `macro_regime`) ecrites. | `SELECT section, items_count FROM fx_digest WHERE run_id = (SELECT max(run_id) FROM run_log)`. |
| **P3** | AG1-FX-V1 chatgpt52 SEUL en activation manuelle (1 run test). Verifier que le brief assemble est coherent, que le prompt systeme est bien injecte, que la decision LLM est parsee, que le Risk Manager log les checks dans `core.alerts`. Aucune ecriture de `position_lots` permise tant que la review du run manuel n'est pas validee par Nicolas. | Run test sans crash + revue Nicolas OK. |
| **P4** | Activation cron AG1-FX-V1 : chatgpt52 (`30 9,14 * * 1-5`) + grok41_reasoning (`45 9,14 * * 1-5`) + gemini30_pro (`0 10,15 * * 1-5`). | 3 runs reussis par variant pendant 5 jours ouvres. |
| **P5** | Page dashboard "Forex Trading (AG1-FX)" deployee. | Page accessible, KPI coherents avec requetes manuelles SQL. |
| **P6** (~4 semaines) | Revue de perf : si >=1 LLM depasse +3 % cumule sur 4 semaines avec drawdown < 5 % et winrate >= 50 %, candidat pour test broker live FX. | Decision Nicolas. |

---

## 11. Criteres go / no-go par phase

**Criteres "stop & rollback" qui doivent declencher un arret immediat** :

- **P1** : si AG2-FX-V1 ne renvoie pas les 27 paires ou si plus de 3 paires en erreur sur 3 runs consecutifs -> rollback (desactiver cron, lever ticket).
- **P2** : si AG4-FX-V1 ne produit aucune section `top_news` pendant 24h -> rollback.
- **P3** : si le Risk Manager FX ne loggue aucun rejet sur 5 runs alors que le LLM propose des decisions hors limites (ex. `size_pct_equity > 0.20`), c'est qu'il ne fonctionne pas -> rollback.
- **P4** : drawdown jour > 5 % sur un LLM -> kill_switch doit s'enclencher automatiquement ; sinon, reprise manuelle + investigation.
- **P5** : si la page dashboard affiche des KPI divergents de la verification SQL manuelle -> rollback de la page (les 3 PMs continuent en background).

---

## 12. Suivi qualite et metriques

A consigner pour la revue P6 (~4 semaines) :

| Metrique | Calcul | Cible |
|---|---|---|
| ROI cumule | `(equity_final - 10000) / 10000` | > +3 % par LLM = candidat live |
| Drawdown max | `min(drawdown_day_pct)` sur la periode | < -5 % = kill_switch (auto-bloquant) |
| Winrate | `count(pnl_eur > 0) / count(closed)` | >= 50 % |
| Profit factor | `sum(pnl+) / abs(sum(pnl-))` | > 1.5 |
| Lots / jour / LLM | `count(open_lots) / nb_jours` | 0.5 a 3 (pas de scalping) |
| Taux rejet Risk Manager | `count(rejected) / count(decisions)` | < 30 % (sinon prompt mal calibre) |
| Couverture news | `news_after_dedupe / news_global_pulled` | > 50 % |

---

## 13. Annexe - recap des changements vs systeme actions V3

| Aspect | V3 actions | V1 Forex |
|---|---|---|
| Capital | 50 000 EUR (heritage discontinu V1->V2) | 10 000 EUR par LLM, baseline propre |
| Univers | actions US/EU + ETF + crypto + FX | 27 paires FX exclusivement |
| Cron PM (AG1) | quotidien 7h Paris (1x/j) | 2x/j decales par LLM (9h30 / 9h45 / 10h, puis 14h30 / 14h45 / 15h) |
| Cron technique (AG2) | 1x/j | **6x/j toutes les 4h** (forex 24/5) |
| Cron news (AG4) | 1x/j | **2x/j fenetre bourse FR** (9h15, 14h15) |
| Levier | NA (cash uniquement) | parametrable (default 1, peut monter a 5) |
| Risk checks | partial (issues 8/9/10 ouvertes) | corriges a 100 % dans le fork FX |
| client_order_id | absent | present + unique (idempotence) |
| Kill switch drawdown | absent | actif |
| Currency exposure cap | NA | 50 % par devise |

---

## 14. Historique des versions

| Version | Date | Auteur | Changements |
|---|---|---|---|
| v1.0 | 2026-04-25 | Nicolas + Claude | Spec initiale ready-to-implement (Codex5.4). |
| v1.1 | 2026-04-26 | Nicolas + Claude | Cron schedules ajustes : AG2 6x/j (forex 24/5), AG4 2x/j (bourse FR 9h-17h30), AG1 2x/j decales 15 min entre LLMs. Generation source-of-truth via `generate_model_variants.py` mise a jour, regeneration des 3 fichiers `AG1_FX_workflow_*_v1.json` confirmee. |
