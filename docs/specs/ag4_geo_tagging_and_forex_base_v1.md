# Brief d'implÃ©mentation â€” AG4 Geo-Tagging + Base AG4_Forex

**Pour** : Codex5.4 (agent d'implÃ©mentation)
**Auteur spec** : Nicolas Ã— Claude (session d'audit 23-24/04/2026)
**Statut** : âœ… **ImplÃ©mentÃ© le 2026-04-24** (commits `53b4dd3`, `147f912`, `08cd363`)
**PrioritÃ©** : P0 (bloque dÃ©cision live broker)
**Version** : v1.0
**DerniÃ¨re mise Ã  jour** : 2026-04-24

---

## Statut d'implÃ©mentation (2026-04-24)

La livraison Codex a Ã©tÃ© vÃ©rifiÃ©e section par section contre ce brief (voir `docs/architecture/historique_issues.md` issues #11 et #12). Tous les livrables sont conformes :

- âœ… Migrations additives `infra/migrations/ag4_v3/20260425_add_geo_tagging.sql` et `infra/migrations/ag4_forex_v1/20260425_init.sql`.
- âœ… Guardrails taxonomie dans `agents/common/AG4-V3/nodes/10_parse_llm_output.js` (ALLOWED_REGIONS / CLASSES / MAG / PAIRS, dÃ©rivation FX pairs depuis `currencies_bullish/bearish`, `tagger_version = 'geo_v1'`).
- âœ… Dual-write conditionnel via `agents/common/AG4-V3/nodes/14_write_fx_news_duckdb.py` (filtre `impact_asset_class âˆˆ {FX, Mixed}`, `origin='global_base'`).
- âœ… Prompt LLM et JSON schema Ã©tendus dans `AG4-V3-workflow.json`.
- âœ… Workflow `AG4-Forex` complet (7 nodes, cron `*/30 7-20 * * 1-5`, `origin='fx_channel'`).
- âœ… Config `infra/config/sources/fx_sources.yaml` (forexlive_main enabled par dÃ©faut, 7 autres sources prÃªtes Ã  activer).
- âœ… Backfill idempotent `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py` + runner VPS.

**Reste Ã  faire cÃ´tÃ© opÃ©rationnel (non couvert par la livraison code)** :

1. DÃ©ployer les migrations sur le VPS via `infra/maintenance/ag4_geo_backfill/run_ag4_geo_forex_migration_vps.sh`.
2. Lancer le backfill 3 mois (`python backfill_geo_tags.py --since 90`) et vÃ©rifier la couverture via les requÃªtes Â§9.
3. Laisser tourner AG4-V3 enrichi pendant ~48 h avant d'activer AG4-Forex puis, plus tard, un PM Forex dÃ©diÃ©.
4. Activer progressivement les sources FX supplÃ©mentaires (`enabled: true` dans `fx_sources.yaml`) aprÃ¨s contrÃ´le qualitÃ©.

---

## 1. Contexte et motivation

### 1.1 D'oÃ¹ Ã§a vient

L'audit du 23/04/2026 a montrÃ© que la perf V2 pure (22/02 â†’ 21/04) sur les actions US est incohÃ©rente entre les 3 IA (Gemini âˆ’435 â‚¬, ChatGPT 0/3, Grok 2/2 sur AAPL â€” trop peu d'Ã©chantillon pour conclure). Sur le Forex, seule Gemini trade effectivement (4 lots fermÃ©s, âˆ’39 â‚¬, wr 75 % mais asymÃ©trie dÃ©favorable). Cf `docs/audits/20260423_audit_valorisation/report_segments.md`.

Nicolas a identifiÃ© deux racines fonctionnelles :

1. **AG4 (analyse des news) ne tague pas l'impact gÃ©ographique.** Une news sur l'inflation US, une news sur la BCE, une news sur un conflit gÃ©opolitique arrivent au PM (AG1) sans Ã©tiquette de rÃ©gion. RÃ©sultat : le bias `currencies_bullish` / `sectors_bullish` ne suffit pas pour distinguer *sur quel marchÃ© d'actions* une news joue. Le PM mÃ©lange les signaux et ouvre des positions sur des tickers US Ã  partir de news qui pilotent en rÃ©alitÃ© le marchÃ© europÃ©en.

2. **Le Forex partage la mÃªme base de news que les actions.** Or le trading FX demande un sourcing et une cadence diffÃ©rents (bulletins BCE/Fed, calendrier Ã©co minute par minute, gÃ©opolitique Ã©nergie). Une base dÃ©diÃ©e permet de spÃ©cialiser l'alimentation et la synthÃ¨se vers un PM Forex dÃ©diÃ©.

### 1.2 DÃ©cisions validÃ©es par Nicolas

1. **Garder la base `ag4_v3.duckdb` comme base principale** et la faire Ã©voluer (ALTER additif).
2. **CrÃ©er une nouvelle base `ag4_forex_v1.duckdb` dÃ©diÃ©e au Forex**, alimentÃ©e par (a) les news globales filtrÃ©es `impact_asset_class âˆˆ {FX, Mixed}`, (b) de nouveaux canaux de sourcing spÃ©cifiquement forex.
3. **Le workflow AG4 (analyse LLM des news) est adaptÃ© pour produire 4 champs de plus** (rÃ©gion, classe d'actif, magnitude, paires FX concernÃ©es) + un `tagger_version`.
4. **Pour chaque news ingÃ©rÃ©e** : Ã©crire dans `ag4_v3` systÃ©matiquement ; Ã©crire en complÃ©ment dans `ag4_forex_v1` si la news a un impact FX dÃ©tectÃ©.
5. **Backfill 3 mois** des news existantes avec les nouveaux tags (job batch one-shot, idempotent).
6. **Nicolas ne dispose pas de 3 mois de sandbox avant live** â†’ le plan doit permettre un passage en prod progressif dÃ¨s que les nouveaux tags sont stabilisÃ©s (â‰ˆ 2-3 semaines de donnÃ©es taguÃ©es).

### 1.3 Ce qui est HORS scope de ce brief

- Le PM Forex dÃ©diÃ© (AG1_Forex avec 10 kâ‚¬ et effet de levier simulÃ©) : spec sÃ©parÃ©e, viendra aprÃ¨s la stabilisation de AG4_Forex.
- La correction des bugs relevÃ©s dans l'audit valorisation (fees=0, drawdown=0, cash_ledger vide depuis 02/03, position_lots incohÃ©rent). Ces bugs sont tracÃ©s sÃ©parÃ©ment et ne bloquent pas ce chantier.
- La refonte de l'extraction `source` (8681 news avec `source="unknown"`). Ã€ traiter Ã  part.

---

## 2. Inventaire existant (Ã  ne PAS casser)

### 2.1 Base `ag4_v3.duckdb`

Chemin : `/home/nicolas/Trader_IA/bases/ag4_v3.duckdb` (adapter au chemin de prod rÃ©el si diffÃ©rent â€” le snapshot auditÃ© est `/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag4_v3.duckdb`).

Tables existantes :
- `main.news_history` â€” **12 026 lignes**, 31 colonnes. C'est la table Ã  faire Ã©voluer.
- `main.news_errors` â€” 8 557 lignes (rate d'erreur Ã  investiguer Ã  part).
- `main.ag4_fx_macro` â€” 305 lignes, vue rÃ©gime macro (USD/EUR/JPY/GBP/CHF/AUD/CAD/NZD bias + `market_regime`).
- `main.ag4_fx_pairs` â€” 0 ligne, squelette prÃªt pour biais directionnels par paire.
- `main.run_log` â€” 456 lignes, traÃ§age des runs AG4.

**Colonnes actuelles de `news_history`** (Ã  ne pas renommer, Ã  ne pas supprimer) :

```
dedupe_key, event_key, run_id, canonical_url, published_at, title, source, feed_url,
symbols, type, notes, impact_score, confidence, urgency, snippet, first_seen_at,
strategy, losers, winners, sectors_bullish, sectors_bearish,
currencies_bullish, currencies_bearish, theme, regime,
analyzed_at, last_seen_at, source_tier, action, reason, created_at, updated_at
```

Exemple rÃ©el d'une ligne analysÃ©e (pour repÃ¨re â€” format courant produit par l'analyseur LLM actuel) :

```
title               : Intentions d'embauches en baisse de 6,5%
source              : unknown            â† bug connu, hors scope
type                : macro
impact_score        : 3
confidence          : 0.7
currencies_bullish  : USD, JPY, CHF
currencies_bearish  : AUD, CAD
sectors_bullish     : Healthcare
theme               : Croissance/Recession
regime              : Risk-Off
action              : analyze
reason              : new_or_material
```

**Observation clÃ©** : l'analyseur LLM produit dÃ©jÃ  du contenu structurÃ© riche. Les nouveaux champs demandÃ©s sont **additifs** â€” pas de refonte du prompt, juste une extension.

### 2.2 Workflow n8n AG4_V3

RÃ©fÃ©rencÃ© dans `project_architecture_6agents.md`. IngÃ¨re les news depuis N flux RSS/API, les dÃ©duplique sur `dedupe_key`, appelle un LLM (OpenAI ou Ã©quivalent) pour produire l'analyse structurÃ©e, Ã©crit dans `news_history`.

Le workflow est Ã  faire Ã©voluer selon deux axes :
- Ã‰tendre le prompt et le schema de sortie du LLM (cf Â§4).
- Ajouter un second Ã©criture conditionnelle vers `ag4_forex_v1.duckdb` quand `impact_asset_class` inclut `FX` (cf Â§7).

---

## 3. Changements de schÃ©ma

### 3.1 ALTER de `ag4_v3.main.news_history`

ExÃ©cuter **dans cet ordre**. Toutes les colonnes sont nullable pour permettre une coexistence avec les lignes non encore re-taguÃ©es.

```sql
-- Script : infra/migrations/ag4_v3/20260425_add_geo_tagging.sql
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_region VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_asset_class VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_magnitude VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_fx_pairs VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS tagger_version VARCHAR;

-- Index pour filtrage rapide cÃ´tÃ© AG4_Forex et AG1
CREATE INDEX IF NOT EXISTS idx_news_impact_asset_class ON main.news_history(impact_asset_class);
CREATE INDEX IF NOT EXISTS idx_news_impact_region ON main.news_history(impact_region);
CREATE INDEX IF NOT EXISTS idx_news_tagger_version ON main.news_history(tagger_version);
```

### 3.2 Taxonomie des 4 nouveaux champs

**Convention globale** : listes multi-valeurs stockÃ©es en **VARCHAR CSV** (mÃªme pattern que les colonnes existantes `currencies_bullish = "USD, JPY, CHF"`), pas de JSON, pas de tableau natif DuckDB. Respect de la cohÃ©rence existante.

#### `impact_region` â€” OÃ¹ la news joue-t-elle ?
Valeur unique ou CSV. Valeurs autorisÃ©es :

| Valeur | Description |
|---|---|
| `Global` | Impact mondial (Fed en premier lieu, grandes crises, OPEP) |
| `US` | Ã‰tats-Unis (donnÃ©es macro US, Fed, SEC, grandes US corps) |
| `EU` | Zone euro dans son ensemble (BCE, donnÃ©es Ã©co EU) |
| `France` | SpÃ©cifique France |
| `UK` | Royaume-Uni (BoE, data UK) |
| `APAC` | Asie-Pacifique (BoJ, PBoC, data Chine/Japon) |
| `Emerging` | MarchÃ©s Ã©mergents hors APAC (LatAm, EMEA Ã©mergents, Turquie) |
| `Other` | Autre / non identifiÃ© |

**RÃ¨gles** :
- Si la news parle explicitement de plusieurs rÃ©gions : CSV (`"US, EU"`).
- Si la news est transversale sans zone dominante : `Global`.
- Si indÃ©terminable : `Other` (et baisser `confidence`).

#### `impact_asset_class` â€” Quelle classe d'actif ?
Valeur unique ou CSV. Valeurs autorisÃ©es :

| Valeur | Description |
|---|---|
| `Equity` | Actions (indices et single names) |
| `FX` | Forex / devises |
| `Commodity` | MatiÃ¨res premiÃ¨res (pÃ©trole, or, agri) |
| `Bond` | Obligations / taux |
| `Crypto` | Crypto-actifs |
| `Mixed` | Impact sur plusieurs classes majeures simultanÃ©ment |
| `None` | Pas d'impact marchÃ© notable |

**RÃ¨gle d'activation AG4_Forex** : une news est Ã©crite dans `ag4_forex_v1` **si et seulement si** `impact_asset_class` contient `FX` OU `Mixed`.

#### `impact_magnitude` â€” Quel poids ?
Valeur unique. Valeurs autorisÃ©es : `Low`, `Medium`, `High`.

CorrÃ©lÃ© Ã  (mais distinct de) `impact_score`. Le LLM doit fixer :
- `Low` : impact probable < 0,3 % sur l'actif pivot, ou Ã©vÃ©nement anecdotique.
- `Medium` : 0,3-1 % attendu, Ã©vÃ©nement notable mais pas dÃ©cisif.
- `High` : > 1 % attendu, ou Ã©vÃ©nement potentiellement trend-changer (dÃ©cision centrale, crise majeure, surprise macro > 2Ïƒ).

**RÃ¨gle pratique** : `impact_magnitude = High â‡’ urgency â‰¥ 0.7`. Ã€ vÃ©rifier en validation (cf Â§9).

#### `impact_fx_pairs` â€” Quelles paires sont concernÃ©es ?
CSV de paires au format internal `XXXYYY` (pas de slash). Vide si `impact_asset_class` ne contient pas `FX`/`Mixed`.

Exemples :
- DÃ©cision Fed : `"USDEUR, USDJPY, USDGBP, USDCHF"`
- Inflation zone euro : `"EURUSD, EURGBP, EURJPY"`
- Crise livre : `"GBPUSD, GBPEUR, GBPJPY"`

**Liste autorisÃ©e** (paires du projet, validÃ©es) :
```
EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD,
EURGBP, EURJPY, EURCHF, EURAUD, EURCAD, EURNZD,
GBPJPY, GBPCHF, GBPAUD, GBPCAD,
AUDJPY, AUDNZD, AUDCAD,
NZDJPY, NZDCAD,
CADJPY, CHFJPY, CADCHF,
CHFCAD, JPYNZD
```
(Codex : si besoin d'ajout, mettre Ã  jour ET la taxonomie ici ET le prompt LLM â€” un seul endroit de vÃ©ritÃ©.)

#### `tagger_version` â€” Idempotence backfill
Valeur : string type `geo_v1`, `geo_v2`, etc. Permet au job de backfill de savoir quelles lignes re-taguer (ex. migration vers une `v2` du schÃ©ma).

### 3.3 CrÃ©ation de `ag4_forex_v1.duckdb`

**Chemin cible** : `/home/nicolas/Trader_IA/bases/ag4_forex_v1.duckdb`.

Script complet :

```sql
-- Script : infra/migrations/ag4_forex_v1/20260425_init.sql

-- Ã€ exÃ©cuter sur une NOUVELLE base vide ag4_forex_v1.duckdb.

CREATE SCHEMA IF NOT EXISTS main;

-- Table principale : une ligne par news FX
CREATE TABLE IF NOT EXISTS main.fx_news_history (
    dedupe_key              VARCHAR PRIMARY KEY,   -- mÃªme clÃ© que ag4_v3 pour jointure
    event_key               VARCHAR,
    run_id                  VARCHAR,
    origin                  VARCHAR,               -- 'global_base' ou 'fx_channel'
    canonical_url           VARCHAR,
    published_at            TIMESTAMP,
    title                   VARCHAR,
    source                  VARCHAR,
    source_tier             VARCHAR,
    snippet                 VARCHAR,

    -- Champs repris ou recalculÃ©s de l'analyse LLM
    impact_region           VARCHAR,
    impact_magnitude        VARCHAR,               -- Low|Medium|High
    impact_fx_pairs         VARCHAR,               -- CSV des paires
    currencies_bullish      VARCHAR,               -- CSV devises (repris de ag4_v3)
    currencies_bearish      VARCHAR,
    regime                  VARCHAR,               -- Risk-On|Risk-Off|Neutre
    theme                   VARCHAR,
    urgency                 DOUBLE,
    confidence              DOUBLE,
    impact_score            INTEGER,

    -- FX-spÃ©cifique (enrichi par le workflow AG4_Forex)
    fx_narrative            VARCHAR,               -- 1-2 phrases expliquant le driver FX
    fx_directional_hint     VARCHAR,               -- CSV de hints "EURUSD:BUY", "USDJPY:SELL"
    tagger_version          VARCHAR,

    first_seen_at           TIMESTAMP,
    last_seen_at            TIMESTAMP,
    analyzed_at             TIMESTAMP,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fxnh_published      ON main.fx_news_history(published_at);
CREATE INDEX IF NOT EXISTS idx_fxnh_magnitude      ON main.fx_news_history(impact_magnitude);
CREATE INDEX IF NOT EXISTS idx_fxnh_pairs          ON main.fx_news_history(impact_fx_pairs);
CREATE INDEX IF NOT EXISTS idx_fxnh_origin         ON main.fx_news_history(origin);

-- Snapshot quotidien/horaire du rÃ©gime FX (reprise du format existant ag4_fx_macro de ag4_v3)
CREATE TABLE IF NOT EXISTS main.fx_macro (
    run_id                  VARCHAR,
    as_of                   TIMESTAMP,
    market_regime           VARCHAR,
    drivers                 VARCHAR,               -- CSV des 3-5 drivers du jour
    confidence              DOUBLE,
    usd_bias                DOUBLE,
    eur_bias                DOUBLE,
    jpy_bias                DOUBLE,
    gbp_bias                DOUBLE,
    chf_bias                DOUBLE,
    aud_bias                DOUBLE,
    cad_bias                DOUBLE,
    nzd_bias                DOUBLE,
    bias_json               VARCHAR,
    source_window_days      INTEGER,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, as_of)
);

-- Snapshot par paire avec biais directionnel (pour briefer AG1_Forex)
CREATE TABLE IF NOT EXISTS main.fx_pairs (
    id                      VARCHAR PRIMARY KEY,
    run_id                  VARCHAR,
    pair                    VARCHAR,                -- format 'EURUSD'
    symbol_internal         VARCHAR,                -- format interne du broker
    directional_bias        VARCHAR,                -- LONG|SHORT|FLAT
    rationale               VARCHAR,                -- 1 phrase
    confidence              DOUBLE,
    urgent_event_window     BOOLEAN,
    as_of                   TIMESTAMP,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fxp_pair ON main.fx_pairs(pair);
CREATE INDEX IF NOT EXISTS idx_fxp_asof ON main.fx_pairs(as_of);

-- Run log dÃ©diÃ©
CREATE TABLE IF NOT EXISTS main.run_log (
    run_id                  VARCHAR PRIMARY KEY,
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP,
    news_ingested           INTEGER,
    news_from_global        INTEGER,               -- nb news remontÃ©es de ag4_v3 via le filtre
    news_from_fx_channels   INTEGER,               -- nb news remontÃ©es des canaux FX dÃ©diÃ©s
    pairs_written           INTEGER,
    errors                  INTEGER,
    notes                   VARCHAR
);

-- Log d'erreurs d'ingestion
CREATE TABLE IF NOT EXISTS main.news_errors (
    run_id                  VARCHAR,
    occurred_at             TIMESTAMP,
    source                  VARCHAR,
    feed_url                VARCHAR,
    error_type              VARCHAR,
    error_detail            VARCHAR
);
```

**Note** : la `fx_news_history` conserve `dedupe_key` identique Ã  `ag4_v3.main.news_history` pour qu'on puisse toujours joindre les deux bases (`ATTACH` en lecture seule cÃ´tÃ© requÃªtes de synthÃ¨se pour AG1_Forex).

---

## 4. Mise Ã  jour du prompt LLM de l'analyseur AG4

### 4.1 Emplacement

Le prompt se trouve dans le node LLM du workflow n8n `AG4_V3`. Codex : identifier le node et modifier le system prompt + la spec du schÃ©ma JSON de sortie.

### 4.2 Nouveau system prompt (additif, FR)

Ajouter Ã  la fin du system prompt existant (ne PAS retirer ce qui existe) le bloc suivant :

```
Tu produis AUSSI, pour chaque news, les champs d'impact suivants :

1. impact_region : une valeur ou une liste CSV parmi
   {Global, US, EU, France, UK, APAC, Emerging, Other}.
   - "Global" si impact transversal mondial (Fed est en premier lieu Global, pas seulement US,
     sauf si la news concerne une action politique US pure).
   - Sinon, la ou les zones explicitement concernÃ©es.
   - "Other" si indÃ©terminable (et abaisser confidence en consÃ©quence).

2. impact_asset_class : une valeur ou CSV parmi
   {Equity, FX, Commodity, Bond, Crypto, Mixed, None}.
   - "Mixed" si 3+ classes sont concurremment impactÃ©es.
   - "None" si pas d'impact marchÃ© notable.

3. impact_magnitude : une SEULE valeur parmi {Low, Medium, High}.
   - Low   : mouvement attendu < 0,3 % sur l'actif pivot, ou Ã©vÃ©nement anecdotique.
   - Medium: 0,3 Ã  1 %, Ã©vÃ©nement notable non dÃ©cisif.
   - High  : > 1 %, ou event susceptible de changer la tendance (dÃ©cision centrale,
     crise majeure, surprise macro > 2Ïƒ).

4. impact_fx_pairs : CSV de paires FX concernÃ©es, format XXXYYY (pas de slash).
   - Vide si impact_asset_class ne contient ni FX ni Mixed.
   - Sinon, lister les paires les plus directement affectÃ©es.
   - Paires autorisÃ©es : EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD,
     EURGBP, EURJPY, EURCHF, EURAUD, EURCAD, EURNZD, GBPJPY, GBPCHF, GBPAUD, GBPCAD,
     AUDJPY, AUDNZD, AUDCAD, NZDJPY, NZDCAD, CADJPY, CHFJPY, CADCHF, CHFCAD, JPYNZD.

Contraintes :
- RÃ©ponds UNIQUEMENT en JSON valide selon le schÃ©ma imposÃ©.
- CohÃ©rence attendue : si impact_magnitude = High, alors urgency â‰¥ 0,7.
- CohÃ©rence attendue : si impact_asset_class contient FX, alors impact_fx_pairs est non vide.
- Tu dois TOUJOURS renseigner les 4 nouveaux champs mÃªme si impact_asset_class = None
  (dans ce cas : impact_fx_pairs = "", impact_magnitude = "Low", impact_region = "Other").
```

### 4.3 SchÃ©ma JSON de sortie

Ã‰tendre le `response_format` (ou le parser JSON) pour inclure :

```json
{
  "impact_region": "string (CSV, values from enum)",
  "impact_asset_class": "string (CSV, values from enum)",
  "impact_magnitude": "string (enum: Low|Medium|High)",
  "impact_fx_pairs": "string (CSV of pair codes, may be empty)"
}
```

Le workflow n8n devra Ã©crire ces 4 champs plus `tagger_version = "geo_v1"` dans `news_history`.

### 4.4 Guardrails cÃ´tÃ© code

Dans le node de post-traitement LLM (juste avant l'INSERT), ajouter une validation lÃ©gÃ¨re :

```python
ALLOWED_REGIONS = {"Global","US","EU","France","UK","APAC","Emerging","Other"}
ALLOWED_CLASSES = {"Equity","FX","Commodity","Bond","Crypto","Mixed","None"}
ALLOWED_MAG = {"Low","Medium","High"}
ALLOWED_PAIRS = {"EURUSD","GBPUSD","USDJPY","USDCHF","AUDUSD","NZDUSD","USDCAD",
    "EURGBP","EURJPY","EURCHF","EURAUD","EURCAD","EURNZD","GBPJPY","GBPCHF",
    "GBPAUD","GBPCAD","AUDJPY","AUDNZD","AUDCAD","NZDJPY","NZDCAD","CADJPY",
    "CHFJPY","CADCHF","CHFCAD","JPYNZD"}

def sanitize_csv(raw: str, allowed: set, default: str = "Other") -> str:
    if not raw: return default
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    kept = [p for p in parts if p in allowed]
    return ",".join(kept) if kept else default

# Appliquer Ã  chaque champ reÃ§u avant INSERT. Valeurs invalides â†’ fallback silencieux
# + log dans news_errors avec error_type="taxonomy_violation".
```

---

## 5. Sourcing forex dÃ©diÃ©

Nicolas source les canaux, Codex doit prÃ©voir **la tuyauterie d'ingestion** (abonnements RSS, poll d'API, normalisation vers le format interne) pour que l'ajout de nouvelles sources soit une simple entrÃ©e de config.

### 5.1 Fichier de config sources FX

Chemin : `infra/config/sources/fx_sources.yaml` (nouveau fichier).

Format proposÃ© :
```yaml
sources:
  - id: forexlive_main
    type: rss
    url: https://www.forexlive.com/feed/news
    tier: A
    enabled: true

  - id: dailyfx_analysis
    type: rss
    url: https://www.dailyfx.com/feeds/market-news
    tier: A
    enabled: false   # dÃ©sactivÃ© par dÃ©faut, Nicolas active aprÃ¨s review

  - id: fxstreet_news
    type: rss
    url: https://www.fxstreet.com/rss/news
    tier: A
    enabled: false

  - id: investing_econ_calendar
    type: api
    url: https://api.investing.com/api/financialdata/economic-calendar
    tier: A
    enabled: false
    params:
      countries: [US, EU, JP, GB, CH, AU, CA, NZ]
      importance: [2, 3]   # medium et high seulement

  - id: bis_press
    type: rss
    url: https://www.bis.org/rss/press.xml
    tier: S
    enabled: false

  - id: fed_statements
    type: rss
    url: https://www.federalreserve.gov/feeds/press_monetary.xml
    tier: S
    enabled: false

  - id: ecb_press
    type: rss
    url: https://www.ecb.europa.eu/rss/press.html
    tier: S
    enabled: false

  - id: boj_statements
    type: rss
    url: https://www.boj.or.jp/en/rss/whatsnew.xml
    tier: S
    enabled: false
```

**Convention `tier`** : `S` = source primaire officielle (banque centrale), `A` = financier spÃ©cialisÃ© FX, `B` = gÃ©nÃ©raliste.

### 5.2 Normalisation

Chaque source produit le mÃªme payload normalisÃ© avant passage au LLM :

```json
{
  "dedupe_key": "sha1(canonical_url or title+pub)",
  "source": "forexlive_main",
  "source_tier": "A",
  "published_at": "ISO8601",
  "title": "...",
  "snippet": "...",
  "canonical_url": "...",
  "feed_url": "...",
  "origin": "fx_channel"
}
```

Le champ `origin="fx_channel"` permet dans `ag4_forex_v1.fx_news_history` de distinguer les news issues du sourcing dÃ©diÃ© (`fx_channel`) de celles remontÃ©es de la base globale (`global_base`).

---

## 6. Backfill des 3 derniers mois

Job one-shot, idempotent. Cible : re-taguer les â‰ˆ 12 000 lignes de `news_history` sans `tagger_version` (ou avec une version antÃ©rieure Ã  la cible).

### 6.1 Script

Chemin : `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py`.

Logique :
1. Lecture par batch de 200 lignes WHERE `tagger_version IS NULL OR tagger_version < 'geo_v1'`.
2. Pour chaque ligne, appeler le LLM avec le **mÃªme prompt** que le pipeline temps rÃ©el mais en mode "completion only" (on garde tel quel ce qui est dÃ©jÃ  analysÃ© â€” impact_score, theme, regime, etc.) et on ne demande que les 4 nouveaux champs.
3. UPDATE `news_history SET impact_region=?, impact_asset_class=?, impact_magnitude=?, impact_fx_pairs=?, tagger_version='geo_v1', updated_at=now() WHERE dedupe_key=?`.
4. **Pour chaque news oÃ¹ `impact_asset_class âˆˆ {FX, Mixed}`** : INSERT OR REPLACE dans `ag4_forex_v1.fx_news_history` avec `origin='global_base'`.

### 6.2 Idempotence

- `tagger_version = 'geo_v1'` est le marqueur de completion. Relancer le script **ne retraite pas** les lignes dÃ©jÃ  taguÃ©es en `geo_v1`.
- L'INSERT dans `fx_news_history` utilise `INSERT OR REPLACE ON CONFLICT (dedupe_key)`.
- Stocker l'Ã©tat du backfill dans `infra/maintenance/ag4_geo_backfill/state.json` (dernier `dedupe_key` traitÃ©, nombre d'erreurs, date du dernier run) pour reprise aprÃ¨s crash.

### 6.3 ContrÃ´le de coÃ»t LLM

Environ 12 000 news Ã— ~400 tokens d'input / 80 tokens d'output. Estimer le coÃ»t a priori et **demander confirmation interactive** avant lancement (print du coÃ»t estimÃ© en â‚¬ pour les 12k news + requis confirmation `y/N`). Budget indicatif : viser < 20 â‚¬ pour le backfill complet.

### 6.4 ObservabilitÃ©

Le script Ã©crit un log structurÃ© dans `infra/maintenance/ag4_geo_backfill/log_YYYYMMDD.jsonl` et une ligne de rÃ©sumÃ© final :
- nb de lignes traitÃ©es
- nb de lignes basculÃ©es dans `fx_news_history`
- nb d'erreurs (taxonomy_violation + LLM timeout)
- coÃ»t effectif LLM (via rÃ©ponse API)

---

## 7. Modifications des workflows n8n

### 7.1 Workflow `AG4_V3` (existant, Ã  Ã©tendre)

Trois changements :

1. **Node "LLM Analyzer"** : injecter le bloc de system prompt de Â§4.2, Ã©tendre le schÃ©ma JSON attendu.
2. **Node "Sanitize & Write"** (nouveau, juste avant l'INSERT) : appliquer les guardrails de Â§4.4.
3. **Node "FX Conditional Write"** (nouveau, aprÃ¨s l'INSERT dans `ag4_v3`) :
   - Si `impact_asset_class` contient `FX` ou `Mixed` :
     - Connexion additionnelle Ã  `ag4_forex_v1.duckdb`
     - INSERT OR REPLACE INTO `main.fx_news_history` avec `origin='global_base'`
   - Sinon : ne rien faire.

### 7.2 Workflow `AG4_Forex` (nouveau)

Nouveau workflow n8n sÃ©parÃ©. ResponsabilitÃ©s :

1. **Ingestion depuis `fx_sources.yaml`** : poller les sources marquÃ©es `enabled: true` (Â§5.1).
2. **DÃ©duplication** sur `dedupe_key`.
3. **Analyse LLM** : mÃªme prompt qu'AG4_V3 (rÃ©utilisation), mais la sortie Ã©crit dans `fx_news_history` avec `origin='fx_channel'`.
4. **AgrÃ©gation horaire** (frÃ©quence Ã  fixer par Nicolas, proposition : toutes les 30 min) :
   - Recalcul de `fx_macro` (rÃ©gime FX global, biais par devise) via synthÃ¨se LLM sur les N derniÃ¨res news FX de la fenÃªtre.
   - Recalcul de `fx_pairs` (biais directionnel par paire) via synthÃ¨se LLM.

### 7.3 Ordre d'exÃ©cution

Le workflow `AG4_Forex` peut tourner indÃ©pendamment de `AG4_V3`. Il n'a pas de dÃ©pendance bloquante vers `AG4_V3`.

En revanche, la **synthÃ¨se pour AG1_Forex** (hors scope) ira lire DANS LES DEUX bases via un `ATTACH` read-only :

```sql
ATTACH '/home/nicolas/Trader_IA/bases/ag4_v3.duckdb'     AS ag4v3  (READ_ONLY);
ATTACH '/home/nicolas/Trader_IA/bases/ag4_forex_v1.duckdb' AS agfx  (READ_ONLY);

-- Brief pour AG1_Forex : synthÃ¨se pondÃ©rÃ©e globale + spÃ©cifique
-- (SQL concret Ã  dÃ©finir dans le spec AG1_Forex ultÃ©rieur).
```

---

## 8. Ordre de dÃ©ploiement (path to live)

Nicolas ne dispose pas de 3 mois de sandbox avant live. Le plan doit permettre de **commencer Ã  collecter du tag geo dÃ¨s le prochain run AG4** puis passer en live progressivement.

| Phase | DurÃ©e indicative | Livrable | Impact prod |
|---|---|---|---|
| **P0** | 1 h | Backup `ag4_v3.duckdb` + init `ag4_forex_v1.duckdb` | nul |
| **P1** | 30 min | ALTER `news_history` (script Â§3.1) | nul, colonnes nullable |
| **P2** | 2-3 h | MAJ workflow AG4_V3 (prompt, guardrails, FX conditional write) | dÃ¨s le prochain tick, les nouvelles news arrivent taguÃ©es |
| **P3** | 1-2 h run + monitoring | Backfill 3 mois (script Â§6) | aucun, tourne en batch hors-ligne |
| **P4** | 2-3 h | Workflow AG4_Forex + ingestion 2-3 sources FX pour commencer (Nicolas active progressivement dans `fx_sources.yaml`) | nul cÃ´tÃ© AG4_V3, ajoute un flux parallÃ¨le |
| **P5** | validation | VÃ©rification des invariants (Â§9) sur 48 h de donnÃ©es live-taguÃ©es | go/no-go pour passer Ã  AG1_Forex |

**CritÃ¨res de go pour P5 â†’ AG1_Forex** (Ã  poser avant de commencer) :
- â‰¥ 95 % des nouvelles lignes de `news_history` ont un `tagger_version` non nul.
- `impact_fx_pairs` est cohÃ©rent avec `impact_asset_class` (pas de FX/Mixed sans paires, pas de paires sans FX/Mixed) Ã  â‰¥ 98 %.
- `fx_news_history` contient au moins 200 news sur 48 h roulantes (preuve que la condition de routage FX dÃ©clenche suffisamment).

Tant que ces 3 critÃ¨res ne sont pas verts, **pas de passage en live sur un broker**.

---

## 9. Validation â€” requÃªtes Ã  lancer aprÃ¨s dÃ©ploiement

### 9.1 Invariants schÃ©ma

```sql
-- 1. Toutes les colonnes existent
DESCRIBE main.news_history;
-- Doit lister impact_region, impact_asset_class, impact_magnitude, impact_fx_pairs, tagger_version

-- 2. Index prÃ©sents
SELECT * FROM duckdb_indexes() WHERE table_name = 'news_history';
```

### 9.2 Invariants sÃ©mantiques (aprÃ¨s 48 h de live)

```sql
-- 3. Taux de couverture des nouveaux tags sur les nouvelles news
SELECT
    SUM(CASE WHEN tagger_version IS NOT NULL THEN 1 ELSE 0 END)::DOUBLE /
    NULLIF(COUNT(*), 0) AS pct_tagged
FROM main.news_history
WHERE analyzed_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours';
-- Attendu : >= 0,95

-- 4. CohÃ©rence impact_fx_pairs â†” impact_asset_class
SELECT
    SUM(CASE
        WHEN impact_asset_class LIKE '%FX%' OR impact_asset_class LIKE '%Mixed%'
        THEN CASE WHEN impact_fx_pairs IS NULL OR impact_fx_pairs = ''
                  THEN 1 ELSE 0 END
        ELSE CASE WHEN impact_fx_pairs IS NULL OR impact_fx_pairs = ''
                  THEN 0 ELSE 1 END
    END)::DOUBLE / NULLIF(COUNT(*), 0) AS pct_incoherent
FROM main.news_history
WHERE tagger_version = 'geo_v1';
-- Attendu : <= 0,02

-- 5. CohÃ©rence magnitude vs urgency
SELECT impact_magnitude,
       AVG(urgency) AS avg_urgency,
       COUNT(*) AS n
FROM main.news_history
WHERE tagger_version = 'geo_v1'
GROUP BY impact_magnitude;
-- Attendu : High >> Medium >> Low

-- 6. RÃ©partition gÃ©ographique (sanity check qualitatif)
SELECT impact_region, COUNT(*) n
FROM main.news_history
WHERE tagger_version = 'geo_v1'
GROUP BY impact_region
ORDER BY n DESC;
-- Attendu : US + Global > 50 %, EU + France > 20 %, APAC visible, Other < 5 %

-- 7. Routage FX effectif
ATTACH '/home/nicolas/Trader_IA/bases/ag4_forex_v1.duckdb' AS agfx (READ_ONLY);
SELECT
    (SELECT COUNT(*) FROM main.news_history
     WHERE tagger_version='geo_v1'
       AND (impact_asset_class LIKE '%FX%' OR impact_asset_class LIKE '%Mixed%')) AS fx_in_global,
    (SELECT COUNT(*) FROM agfx.main.fx_news_history WHERE origin='global_base') AS fx_in_forex_base;
-- Attendu : Ã©galitÃ© ou Ã©cart < 1 %
```

### 9.3 Invariants backfill

```sql
-- 8. Backfill terminÃ©
SELECT COUNT(*) AS not_tagged
FROM main.news_history
WHERE tagger_version IS NULL
  AND published_at >= '2026-01-24';
-- Attendu : 0 (aprÃ¨s run P3)
```

---

## 10. Rollback

Si P2 ou P4 pose problÃ¨me :

```sql
-- Rollback soft : remettre le prompt LLM en version prÃ©cÃ©dente (garder les colonnes,
-- elles restent nullables). Zero perte de donnÃ©es.

-- Rollback dur (uniquement si ALTER lui-mÃªme pose problÃ¨me, cas rarissime en DuckDB) :
ALTER TABLE main.news_history DROP COLUMN IF EXISTS impact_region;
ALTER TABLE main.news_history DROP COLUMN IF EXISTS impact_asset_class;
ALTER TABLE main.news_history DROP COLUMN IF EXISTS impact_magnitude;
ALTER TABLE main.news_history DROP COLUMN IF EXISTS impact_fx_pairs;
ALTER TABLE main.news_history DROP COLUMN IF EXISTS tagger_version;
-- (Restaurer depuis le backup P0 si besoin.)

-- Rollback de la base AG4_Forex : simplement supprimer le fichier.
-- Aucun lien FK externe ne pointe vers elle en P1-P4.
```

---

## 11. Points d'attention / caveats

1. **`source` est "unknown"** pour 8 681 lignes dans `ag4_v3` â€” bug d'ingestion connu, ne pas s'en servir comme clÃ© de routage tant qu'il n'est pas corrigÃ©. Ã€ traiter dans un chantier sÃ©parÃ©.

2. **`news_errors` contient 8 557 lignes** â€” rate d'erreur d'ingestion historique Ã©levÃ©. Investiguer si le backfill gÃ©nÃ¨re des erreurs sur les mÃªmes dedupe_key pour filtrer la cause racine.

3. **Ne PAS toucher Ã  `core.position_lots`, `core.cash_ledger`, etc. depuis ce chantier** â€” ils sont dans les bases `ag1_v3_*.duckdb` (une par IA), pas dans `ag4_v3`. Les incohÃ©rences relevÃ©es dans `audit_valorisation_20260423.md` sont tracÃ©es sÃ©parÃ©ment.

4. **CoÃ»t LLM du backfill** : valider avec Nicolas avant lancement (cf Â§6.3). Si le modÃ¨le actuel est trop cher, envisager GPT-4o-mini ou Claude Haiku pour le **seul backfill** (pas pour le temps rÃ©el oÃ¹ la qualitÃ© compte davantage).

5. **Le workflow AG4_V3 Ã©crit dÃ©jÃ  en pipe vers un LLM** â€” la latence d'ajout des 4 champs est marginale (+ quelques tokens en output), pas de risque de throttling significatif.

6. **La convention CSV des champs multi-valeurs** (plutÃ´t qu'un JSON ou un array DuckDB natif) est imposÃ©e par cohÃ©rence avec `currencies_bullish` / `sectors_bullish` dÃ©jÃ  en place. **Ne pas dÃ©river vers `LIST(VARCHAR)` ou JSON** â€” mÃªme si plus propre, Ã§a casserait les requÃªtes existantes.

7. **`impact_fx_pairs` format `XXXYYY` sans slash** : convention du projet (cf `ag4_fx_pairs.pair` existant). Ne pas introduire le format `EUR/USD`.

8. **`tagger_version` comparaison lexicographique** : `geo_v1 < geo_v2 < geo_v10`. Si on anticipe `v10+`, utiliser zero-pad (`geo_v001`). Peu probable Ã  horizon 12 mois, acceptÃ© en l'Ã©tat.

---

## 12. RÃ©sumÃ© opÃ©rationnel (TL;DR pour Codex)

1. Backup DBs + init `ag4_forex_v1.duckdb` (Â§3.3).
2. Run `infra/migrations/ag4_v3/20260425_add_geo_tagging.sql` (Â§3.1).
3. Ouvrir le workflow n8n `AG4_V3`, Ã©tendre le prompt LLM (Â§4.2) et le schÃ©ma JSON (Â§4.3).
4. Ajouter le node "Sanitize & Write" (Â§4.4) puis "FX Conditional Write" (Â§7.1.3).
5. CrÃ©er `infra/config/sources/fx_sources.yaml` (Â§5.1), garder la plupart `enabled: false` pour dÃ©marrer.
6. CrÃ©er le workflow n8n `AG4_Forex` (Â§7.2) â€” consomme `fx_sources.yaml` + Ã©crit dans `fx_news_history` avec `origin='fx_channel'`.
7. Ã‰crire et exÃ©cuter `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py` (Â§6). Demander confirmation de coÃ»t avant d'appeler le LLM.
8. Laisser tourner 48 h et lancer les 8 requÃªtes de validation (Â§9).
9. Livrer Ã  Nicolas un rapport court (pourcentages obtenus vs attendus + Ã©chantillons de news taguÃ©es pour revue qualitative) avant toute dÃ©cision de passage live sur AG1_Forex.

---

**RÃ©fÃ©rences** :
- Audit valorisation : `docs/audits/20260423_audit_valorisation/report.md`
- Audit segments : `docs/audits/20260423_audit_valorisation/report_segments.md`
- Architecture 6 agents : `docs/architecture/...` (cf memory `project_architecture_6agents.md`)
- Snapshot d'audit : `snapshots/duckdb_20260422/ag4_v3.duckdb`
