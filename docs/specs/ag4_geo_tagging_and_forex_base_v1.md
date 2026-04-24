# Brief d'implémentation — AG4 Geo-Tagging + Base AG4_Forex

**Pour** : Codex5.4 (agent d'implémentation)
**Auteur spec** : Nicolas × Claude (session d'audit 23-24/04/2026)
**Statut** : ✅ **Implémenté le 2026-04-24** (commits `53b4dd3`, `147f912`, `08cd363`)
**Priorité** : P0 (bloque décision live broker)
**Version** : v1.0
**Dernière mise à jour** : 2026-04-24

---

## Statut d'implémentation (2026-04-24)

La livraison Codex a été vérifiée section par section contre ce brief (voir `docs/architecture/historique_issues.md` issues #11 et #12). Tous les livrables sont conformes :

- ✅ Migrations additives `infra/migrations/ag4_v3/20260425_add_geo_tagging.sql` et `infra/migrations/ag4_forex_v1/20260425_init.sql`.
- ✅ Guardrails taxonomie dans `agents/AG4-V3/nodes/10_parse_llm_output.js` (ALLOWED_REGIONS / CLASSES / MAG / PAIRS, dérivation FX pairs depuis `currencies_bullish/bearish`, `tagger_version = 'geo_v1'`).
- ✅ Dual-write conditionnel via `agents/AG4-V3/nodes/14_write_fx_news_duckdb.py` (filtre `impact_asset_class ∈ {FX, Mixed}`, `origin='global_base'`).
- ✅ Prompt LLM et JSON schema étendus dans `AG4-V3-workflow.json`.
- ✅ Workflow `AG4-Forex` complet (7 nodes, cron `*/30 7-20 * * 1-5`, `origin='fx_channel'`).
- ✅ Config `infra/config/sources/fx_sources.yaml` (forexlive_main enabled par défaut, 7 autres sources prêtes à activer).
- ✅ Backfill idempotent `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py` + runner VPS.

**Reste à faire côté opérationnel (non couvert par la livraison code)** :

1. Déployer les migrations sur le VPS via `infra/maintenance/ag4_geo_backfill/run_ag4_geo_forex_migration_vps.sh`.
2. Lancer le backfill 3 mois (`python backfill_geo_tags.py --since 90`) et vérifier la couverture via les requêtes §9.
3. Laisser tourner AG4-V3 enrichi pendant ~48 h avant d'activer AG4-Forex puis, plus tard, un PM Forex dédié.
4. Activer progressivement les sources FX supplémentaires (`enabled: true` dans `fx_sources.yaml`) après contrôle qualité.

---

## 1. Contexte et motivation

### 1.1 D'où ça vient

L'audit du 23/04/2026 a montré que la perf V2 pure (22/02 → 21/04) sur les actions US est incohérente entre les 3 IA (Gemini −435 €, ChatGPT 0/3, Grok 2/2 sur AAPL — trop peu d'échantillon pour conclure). Sur le Forex, seule Gemini trade effectivement (4 lots fermés, −39 €, wr 75 % mais asymétrie défavorable). Cf `docs/audits/20260423_audit_valorisation/report_segments.md`.

Nicolas a identifié deux racines fonctionnelles :

1. **AG4 (analyse des news) ne tague pas l'impact géographique.** Une news sur l'inflation US, une news sur la BCE, une news sur un conflit géopolitique arrivent au PM (AG1) sans étiquette de région. Résultat : le bias `currencies_bullish` / `sectors_bullish` ne suffit pas pour distinguer *sur quel marché d'actions* une news joue. Le PM mélange les signaux et ouvre des positions sur des tickers US à partir de news qui pilotent en réalité le marché européen.

2. **Le Forex partage la même base de news que les actions.** Or le trading FX demande un sourcing et une cadence différents (bulletins BCE/Fed, calendrier éco minute par minute, géopolitique énergie). Une base dédiée permet de spécialiser l'alimentation et la synthèse vers un PM Forex dédié.

### 1.2 Décisions validées par Nicolas

1. **Garder la base `ag4_v3.duckdb` comme base principale** et la faire évoluer (ALTER additif).
2. **Créer une nouvelle base `ag4_forex_v1.duckdb` dédiée au Forex**, alimentée par (a) les news globales filtrées `impact_asset_class ∈ {FX, Mixed}`, (b) de nouveaux canaux de sourcing spécifiquement forex.
3. **Le workflow AG4 (analyse LLM des news) est adapté pour produire 4 champs de plus** (région, classe d'actif, magnitude, paires FX concernées) + un `tagger_version`.
4. **Pour chaque news ingérée** : écrire dans `ag4_v3` systématiquement ; écrire en complément dans `ag4_forex_v1` si la news a un impact FX détecté.
5. **Backfill 3 mois** des news existantes avec les nouveaux tags (job batch one-shot, idempotent).
6. **Nicolas ne dispose pas de 3 mois de sandbox avant live** → le plan doit permettre un passage en prod progressif dès que les nouveaux tags sont stabilisés (≈ 2-3 semaines de données taguées).

### 1.3 Ce qui est HORS scope de ce brief

- Le PM Forex dédié (AG1_Forex avec 10 k€ et effet de levier simulé) : spec séparée, viendra après la stabilisation de AG4_Forex.
- La correction des bugs relevés dans l'audit valorisation (fees=0, drawdown=0, cash_ledger vide depuis 02/03, position_lots incohérent). Ces bugs sont tracés séparément et ne bloquent pas ce chantier.
- La refonte de l'extraction `source` (8681 news avec `source="unknown"`). À traiter à part.

---

## 2. Inventaire existant (à ne PAS casser)

### 2.1 Base `ag4_v3.duckdb`

Chemin : `/home/nicolas/Trader_IA/bases/ag4_v3.duckdb` (adapter au chemin de prod réel si différent — le snapshot audité est `/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag4_v3.duckdb`).

Tables existantes :
- `main.news_history` — **12 026 lignes**, 31 colonnes. C'est la table à faire évoluer.
- `main.news_errors` — 8 557 lignes (rate d'erreur à investiguer à part).
- `main.ag4_fx_macro` — 305 lignes, vue régime macro (USD/EUR/JPY/GBP/CHF/AUD/CAD/NZD bias + `market_regime`).
- `main.ag4_fx_pairs` — 0 ligne, squelette prêt pour biais directionnels par paire.
- `main.run_log` — 456 lignes, traçage des runs AG4.

**Colonnes actuelles de `news_history`** (à ne pas renommer, à ne pas supprimer) :

```
dedupe_key, event_key, run_id, canonical_url, published_at, title, source, feed_url,
symbols, type, notes, impact_score, confidence, urgency, snippet, first_seen_at,
strategy, losers, winners, sectors_bullish, sectors_bearish,
currencies_bullish, currencies_bearish, theme, regime,
analyzed_at, last_seen_at, source_tier, action, reason, created_at, updated_at
```

Exemple réel d'une ligne analysée (pour repère — format courant produit par l'analyseur LLM actuel) :

```
title               : Intentions d'embauches en baisse de 6,5%
source              : unknown            ← bug connu, hors scope
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

**Observation clé** : l'analyseur LLM produit déjà du contenu structuré riche. Les nouveaux champs demandés sont **additifs** — pas de refonte du prompt, juste une extension.

### 2.2 Workflow n8n AG4_V3

Référencé dans `project_architecture_6agents.md`. Ingère les news depuis N flux RSS/API, les déduplique sur `dedupe_key`, appelle un LLM (OpenAI ou équivalent) pour produire l'analyse structurée, écrit dans `news_history`.

Le workflow est à faire évoluer selon deux axes :
- Étendre le prompt et le schema de sortie du LLM (cf §4).
- Ajouter un second écriture conditionnelle vers `ag4_forex_v1.duckdb` quand `impact_asset_class` inclut `FX` (cf §7).

---

## 3. Changements de schéma

### 3.1 ALTER de `ag4_v3.main.news_history`

Exécuter **dans cet ordre**. Toutes les colonnes sont nullable pour permettre une coexistence avec les lignes non encore re-taguées.

```sql
-- Script : infra/migrations/ag4_v3/20260425_add_geo_tagging.sql
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_region VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_asset_class VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_magnitude VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_fx_pairs VARCHAR;
ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS tagger_version VARCHAR;

-- Index pour filtrage rapide côté AG4_Forex et AG1
CREATE INDEX IF NOT EXISTS idx_news_impact_asset_class ON main.news_history(impact_asset_class);
CREATE INDEX IF NOT EXISTS idx_news_impact_region ON main.news_history(impact_region);
CREATE INDEX IF NOT EXISTS idx_news_tagger_version ON main.news_history(tagger_version);
```

### 3.2 Taxonomie des 4 nouveaux champs

**Convention globale** : listes multi-valeurs stockées en **VARCHAR CSV** (même pattern que les colonnes existantes `currencies_bullish = "USD, JPY, CHF"`), pas de JSON, pas de tableau natif DuckDB. Respect de la cohérence existante.

#### `impact_region` — Où la news joue-t-elle ?
Valeur unique ou CSV. Valeurs autorisées :

| Valeur | Description |
|---|---|
| `Global` | Impact mondial (Fed en premier lieu, grandes crises, OPEP) |
| `US` | États-Unis (données macro US, Fed, SEC, grandes US corps) |
| `EU` | Zone euro dans son ensemble (BCE, données éco EU) |
| `France` | Spécifique France |
| `UK` | Royaume-Uni (BoE, data UK) |
| `APAC` | Asie-Pacifique (BoJ, PBoC, data Chine/Japon) |
| `Emerging` | Marchés émergents hors APAC (LatAm, EMEA émergents, Turquie) |
| `Other` | Autre / non identifié |

**Règles** :
- Si la news parle explicitement de plusieurs régions : CSV (`"US, EU"`).
- Si la news est transversale sans zone dominante : `Global`.
- Si indéterminable : `Other` (et baisser `confidence`).

#### `impact_asset_class` — Quelle classe d'actif ?
Valeur unique ou CSV. Valeurs autorisées :

| Valeur | Description |
|---|---|
| `Equity` | Actions (indices et single names) |
| `FX` | Forex / devises |
| `Commodity` | Matières premières (pétrole, or, agri) |
| `Bond` | Obligations / taux |
| `Crypto` | Crypto-actifs |
| `Mixed` | Impact sur plusieurs classes majeures simultanément |
| `None` | Pas d'impact marché notable |

**Règle d'activation AG4_Forex** : une news est écrite dans `ag4_forex_v1` **si et seulement si** `impact_asset_class` contient `FX` OU `Mixed`.

#### `impact_magnitude` — Quel poids ?
Valeur unique. Valeurs autorisées : `Low`, `Medium`, `High`.

Corrélé à (mais distinct de) `impact_score`. Le LLM doit fixer :
- `Low` : impact probable < 0,3 % sur l'actif pivot, ou événement anecdotique.
- `Medium` : 0,3-1 % attendu, événement notable mais pas décisif.
- `High` : > 1 % attendu, ou événement potentiellement trend-changer (décision centrale, crise majeure, surprise macro > 2σ).

**Règle pratique** : `impact_magnitude = High ⇒ urgency ≥ 0.7`. À vérifier en validation (cf §9).

#### `impact_fx_pairs` — Quelles paires sont concernées ?
CSV de paires au format internal `XXXYYY` (pas de slash). Vide si `impact_asset_class` ne contient pas `FX`/`Mixed`.

Exemples :
- Décision Fed : `"USDEUR, USDJPY, USDGBP, USDCHF"`
- Inflation zone euro : `"EURUSD, EURGBP, EURJPY"`
- Crise livre : `"GBPUSD, GBPEUR, GBPJPY"`

**Liste autorisée** (paires du projet, validées) :
```
EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD,
EURGBP, EURJPY, EURCHF, EURAUD, EURCAD, EURNZD,
GBPJPY, GBPCHF, GBPAUD, GBPCAD,
AUDJPY, AUDNZD, AUDCAD,
NZDJPY, NZDCAD,
CADJPY, CHFJPY, CADCHF,
CHFCAD, JPYNZD
```
(Codex : si besoin d'ajout, mettre à jour ET la taxonomie ici ET le prompt LLM — un seul endroit de vérité.)

#### `tagger_version` — Idempotence backfill
Valeur : string type `geo_v1`, `geo_v2`, etc. Permet au job de backfill de savoir quelles lignes re-taguer (ex. migration vers une `v2` du schéma).

### 3.3 Création de `ag4_forex_v1.duckdb`

**Chemin cible** : `/home/nicolas/Trader_IA/bases/ag4_forex_v1.duckdb`.

Script complet :

```sql
-- Script : infra/migrations/ag4_forex_v1/20260425_init.sql

-- À exécuter sur une NOUVELLE base vide ag4_forex_v1.duckdb.

CREATE SCHEMA IF NOT EXISTS main;

-- Table principale : une ligne par news FX
CREATE TABLE IF NOT EXISTS main.fx_news_history (
    dedupe_key              VARCHAR PRIMARY KEY,   -- même clé que ag4_v3 pour jointure
    event_key               VARCHAR,
    run_id                  VARCHAR,
    origin                  VARCHAR,               -- 'global_base' ou 'fx_channel'
    canonical_url           VARCHAR,
    published_at            TIMESTAMP,
    title                   VARCHAR,
    source                  VARCHAR,
    source_tier             VARCHAR,
    snippet                 VARCHAR,

    -- Champs repris ou recalculés de l'analyse LLM
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

    -- FX-spécifique (enrichi par le workflow AG4_Forex)
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

-- Snapshot quotidien/horaire du régime FX (reprise du format existant ag4_fx_macro de ag4_v3)
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

-- Run log dédié
CREATE TABLE IF NOT EXISTS main.run_log (
    run_id                  VARCHAR PRIMARY KEY,
    started_at              TIMESTAMP,
    finished_at             TIMESTAMP,
    news_ingested           INTEGER,
    news_from_global        INTEGER,               -- nb news remontées de ag4_v3 via le filtre
    news_from_fx_channels   INTEGER,               -- nb news remontées des canaux FX dédiés
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

**Note** : la `fx_news_history` conserve `dedupe_key` identique à `ag4_v3.main.news_history` pour qu'on puisse toujours joindre les deux bases (`ATTACH` en lecture seule côté requêtes de synthèse pour AG1_Forex).

---

## 4. Mise à jour du prompt LLM de l'analyseur AG4

### 4.1 Emplacement

Le prompt se trouve dans le node LLM du workflow n8n `AG4_V3`. Codex : identifier le node et modifier le system prompt + la spec du schéma JSON de sortie.

### 4.2 Nouveau system prompt (additif, FR)

Ajouter à la fin du system prompt existant (ne PAS retirer ce qui existe) le bloc suivant :

```
Tu produis AUSSI, pour chaque news, les champs d'impact suivants :

1. impact_region : une valeur ou une liste CSV parmi
   {Global, US, EU, France, UK, APAC, Emerging, Other}.
   - "Global" si impact transversal mondial (Fed est en premier lieu Global, pas seulement US,
     sauf si la news concerne une action politique US pure).
   - Sinon, la ou les zones explicitement concernées.
   - "Other" si indéterminable (et abaisser confidence en conséquence).

2. impact_asset_class : une valeur ou CSV parmi
   {Equity, FX, Commodity, Bond, Crypto, Mixed, None}.
   - "Mixed" si 3+ classes sont concurremment impactées.
   - "None" si pas d'impact marché notable.

3. impact_magnitude : une SEULE valeur parmi {Low, Medium, High}.
   - Low   : mouvement attendu < 0,3 % sur l'actif pivot, ou événement anecdotique.
   - Medium: 0,3 à 1 %, événement notable non décisif.
   - High  : > 1 %, ou event susceptible de changer la tendance (décision centrale,
     crise majeure, surprise macro > 2σ).

4. impact_fx_pairs : CSV de paires FX concernées, format XXXYYY (pas de slash).
   - Vide si impact_asset_class ne contient ni FX ni Mixed.
   - Sinon, lister les paires les plus directement affectées.
   - Paires autorisées : EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD,
     EURGBP, EURJPY, EURCHF, EURAUD, EURCAD, EURNZD, GBPJPY, GBPCHF, GBPAUD, GBPCAD,
     AUDJPY, AUDNZD, AUDCAD, NZDJPY, NZDCAD, CADJPY, CHFJPY, CADCHF, CHFCAD, JPYNZD.

Contraintes :
- Réponds UNIQUEMENT en JSON valide selon le schéma imposé.
- Cohérence attendue : si impact_magnitude = High, alors urgency ≥ 0,7.
- Cohérence attendue : si impact_asset_class contient FX, alors impact_fx_pairs est non vide.
- Tu dois TOUJOURS renseigner les 4 nouveaux champs même si impact_asset_class = None
  (dans ce cas : impact_fx_pairs = "", impact_magnitude = "Low", impact_region = "Other").
```

### 4.3 Schéma JSON de sortie

Étendre le `response_format` (ou le parser JSON) pour inclure :

```json
{
  "impact_region": "string (CSV, values from enum)",
  "impact_asset_class": "string (CSV, values from enum)",
  "impact_magnitude": "string (enum: Low|Medium|High)",
  "impact_fx_pairs": "string (CSV of pair codes, may be empty)"
}
```

Le workflow n8n devra écrire ces 4 champs plus `tagger_version = "geo_v1"` dans `news_history`.

### 4.4 Guardrails côté code

Dans le node de post-traitement LLM (juste avant l'INSERT), ajouter une validation légère :

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

# Appliquer à chaque champ reçu avant INSERT. Valeurs invalides → fallback silencieux
# + log dans news_errors avec error_type="taxonomy_violation".
```

---

## 5. Sourcing forex dédié

Nicolas source les canaux, Codex doit prévoir **la tuyauterie d'ingestion** (abonnements RSS, poll d'API, normalisation vers le format interne) pour que l'ajout de nouvelles sources soit une simple entrée de config.

### 5.1 Fichier de config sources FX

Chemin : `infra/config/sources/fx_sources.yaml` (nouveau fichier).

Format proposé :
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
    enabled: false   # désactivé par défaut, Nicolas active après review

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

**Convention `tier`** : `S` = source primaire officielle (banque centrale), `A` = financier spécialisé FX, `B` = généraliste.

### 5.2 Normalisation

Chaque source produit le même payload normalisé avant passage au LLM :

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

Le champ `origin="fx_channel"` permet dans `ag4_forex_v1.fx_news_history` de distinguer les news issues du sourcing dédié (`fx_channel`) de celles remontées de la base globale (`global_base`).

---

## 6. Backfill des 3 derniers mois

Job one-shot, idempotent. Cible : re-taguer les ≈ 12 000 lignes de `news_history` sans `tagger_version` (ou avec une version antérieure à la cible).

### 6.1 Script

Chemin : `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py`.

Logique :
1. Lecture par batch de 200 lignes WHERE `tagger_version IS NULL OR tagger_version < 'geo_v1'`.
2. Pour chaque ligne, appeler le LLM avec le **même prompt** que le pipeline temps réel mais en mode "completion only" (on garde tel quel ce qui est déjà analysé — impact_score, theme, regime, etc.) et on ne demande que les 4 nouveaux champs.
3. UPDATE `news_history SET impact_region=?, impact_asset_class=?, impact_magnitude=?, impact_fx_pairs=?, tagger_version='geo_v1', updated_at=now() WHERE dedupe_key=?`.
4. **Pour chaque news où `impact_asset_class ∈ {FX, Mixed}`** : INSERT OR REPLACE dans `ag4_forex_v1.fx_news_history` avec `origin='global_base'`.

### 6.2 Idempotence

- `tagger_version = 'geo_v1'` est le marqueur de completion. Relancer le script **ne retraite pas** les lignes déjà taguées en `geo_v1`.
- L'INSERT dans `fx_news_history` utilise `INSERT OR REPLACE ON CONFLICT (dedupe_key)`.
- Stocker l'état du backfill dans `infra/maintenance/ag4_geo_backfill/state.json` (dernier `dedupe_key` traité, nombre d'erreurs, date du dernier run) pour reprise après crash.

### 6.3 Contrôle de coût LLM

Environ 12 000 news × ~400 tokens d'input / 80 tokens d'output. Estimer le coût a priori et **demander confirmation interactive** avant lancement (print du coût estimé en € pour les 12k news + requis confirmation `y/N`). Budget indicatif : viser < 20 € pour le backfill complet.

### 6.4 Observabilité

Le script écrit un log structuré dans `infra/maintenance/ag4_geo_backfill/log_YYYYMMDD.jsonl` et une ligne de résumé final :
- nb de lignes traitées
- nb de lignes basculées dans `fx_news_history`
- nb d'erreurs (taxonomy_violation + LLM timeout)
- coût effectif LLM (via réponse API)

---

## 7. Modifications des workflows n8n

### 7.1 Workflow `AG4_V3` (existant, à étendre)

Trois changements :

1. **Node "LLM Analyzer"** : injecter le bloc de system prompt de §4.2, étendre le schéma JSON attendu.
2. **Node "Sanitize & Write"** (nouveau, juste avant l'INSERT) : appliquer les guardrails de §4.4.
3. **Node "FX Conditional Write"** (nouveau, après l'INSERT dans `ag4_v3`) :
   - Si `impact_asset_class` contient `FX` ou `Mixed` :
     - Connexion additionnelle à `ag4_forex_v1.duckdb`
     - INSERT OR REPLACE INTO `main.fx_news_history` avec `origin='global_base'`
   - Sinon : ne rien faire.

### 7.2 Workflow `AG4_Forex` (nouveau)

Nouveau workflow n8n séparé. Responsabilités :

1. **Ingestion depuis `fx_sources.yaml`** : poller les sources marquées `enabled: true` (§5.1).
2. **Déduplication** sur `dedupe_key`.
3. **Analyse LLM** : même prompt qu'AG4_V3 (réutilisation), mais la sortie écrit dans `fx_news_history` avec `origin='fx_channel'`.
4. **Agrégation horaire** (fréquence à fixer par Nicolas, proposition : toutes les 30 min) :
   - Recalcul de `fx_macro` (régime FX global, biais par devise) via synthèse LLM sur les N dernières news FX de la fenêtre.
   - Recalcul de `fx_pairs` (biais directionnel par paire) via synthèse LLM.

### 7.3 Ordre d'exécution

Le workflow `AG4_Forex` peut tourner indépendamment de `AG4_V3`. Il n'a pas de dépendance bloquante vers `AG4_V3`.

En revanche, la **synthèse pour AG1_Forex** (hors scope) ira lire DANS LES DEUX bases via un `ATTACH` read-only :

```sql
ATTACH '/home/nicolas/Trader_IA/bases/ag4_v3.duckdb'     AS ag4v3  (READ_ONLY);
ATTACH '/home/nicolas/Trader_IA/bases/ag4_forex_v1.duckdb' AS agfx  (READ_ONLY);

-- Brief pour AG1_Forex : synthèse pondérée globale + spécifique
-- (SQL concret à définir dans le spec AG1_Forex ultérieur).
```

---

## 8. Ordre de déploiement (path to live)

Nicolas ne dispose pas de 3 mois de sandbox avant live. Le plan doit permettre de **commencer à collecter du tag geo dès le prochain run AG4** puis passer en live progressivement.

| Phase | Durée indicative | Livrable | Impact prod |
|---|---|---|---|
| **P0** | 1 h | Backup `ag4_v3.duckdb` + init `ag4_forex_v1.duckdb` | nul |
| **P1** | 30 min | ALTER `news_history` (script §3.1) | nul, colonnes nullable |
| **P2** | 2-3 h | MAJ workflow AG4_V3 (prompt, guardrails, FX conditional write) | dès le prochain tick, les nouvelles news arrivent taguées |
| **P3** | 1-2 h run + monitoring | Backfill 3 mois (script §6) | aucun, tourne en batch hors-ligne |
| **P4** | 2-3 h | Workflow AG4_Forex + ingestion 2-3 sources FX pour commencer (Nicolas active progressivement dans `fx_sources.yaml`) | nul côté AG4_V3, ajoute un flux parallèle |
| **P5** | validation | Vérification des invariants (§9) sur 48 h de données live-taguées | go/no-go pour passer à AG1_Forex |

**Critères de go pour P5 → AG1_Forex** (à poser avant de commencer) :
- ≥ 95 % des nouvelles lignes de `news_history` ont un `tagger_version` non nul.
- `impact_fx_pairs` est cohérent avec `impact_asset_class` (pas de FX/Mixed sans paires, pas de paires sans FX/Mixed) à ≥ 98 %.
- `fx_news_history` contient au moins 200 news sur 48 h roulantes (preuve que la condition de routage FX déclenche suffisamment).

Tant que ces 3 critères ne sont pas verts, **pas de passage en live sur un broker**.

---

## 9. Validation — requêtes à lancer après déploiement

### 9.1 Invariants schéma

```sql
-- 1. Toutes les colonnes existent
DESCRIBE main.news_history;
-- Doit lister impact_region, impact_asset_class, impact_magnitude, impact_fx_pairs, tagger_version

-- 2. Index présents
SELECT * FROM duckdb_indexes() WHERE table_name = 'news_history';
```

### 9.2 Invariants sémantiques (après 48 h de live)

```sql
-- 3. Taux de couverture des nouveaux tags sur les nouvelles news
SELECT
    SUM(CASE WHEN tagger_version IS NOT NULL THEN 1 ELSE 0 END)::DOUBLE /
    NULLIF(COUNT(*), 0) AS pct_tagged
FROM main.news_history
WHERE analyzed_at >= CURRENT_TIMESTAMP - INTERVAL '48 hours';
-- Attendu : >= 0,95

-- 4. Cohérence impact_fx_pairs ↔ impact_asset_class
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

-- 5. Cohérence magnitude vs urgency
SELECT impact_magnitude,
       AVG(urgency) AS avg_urgency,
       COUNT(*) AS n
FROM main.news_history
WHERE tagger_version = 'geo_v1'
GROUP BY impact_magnitude;
-- Attendu : High >> Medium >> Low

-- 6. Répartition géographique (sanity check qualitatif)
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
-- Attendu : égalité ou écart < 1 %
```

### 9.3 Invariants backfill

```sql
-- 8. Backfill terminé
SELECT COUNT(*) AS not_tagged
FROM main.news_history
WHERE tagger_version IS NULL
  AND published_at >= '2026-01-24';
-- Attendu : 0 (après run P3)
```

---

## 10. Rollback

Si P2 ou P4 pose problème :

```sql
-- Rollback soft : remettre le prompt LLM en version précédente (garder les colonnes,
-- elles restent nullables). Zero perte de données.

-- Rollback dur (uniquement si ALTER lui-même pose problème, cas rarissime en DuckDB) :
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

1. **`source` est "unknown"** pour 8 681 lignes dans `ag4_v3` — bug d'ingestion connu, ne pas s'en servir comme clé de routage tant qu'il n'est pas corrigé. À traiter dans un chantier séparé.

2. **`news_errors` contient 8 557 lignes** — rate d'erreur d'ingestion historique élevé. Investiguer si le backfill génère des erreurs sur les mêmes dedupe_key pour filtrer la cause racine.

3. **Ne PAS toucher à `core.position_lots`, `core.cash_ledger`, etc. depuis ce chantier** — ils sont dans les bases `ag1_v3_*.duckdb` (une par IA), pas dans `ag4_v3`. Les incohérences relevées dans `audit_valorisation_20260423.md` sont tracées séparément.

4. **Coût LLM du backfill** : valider avec Nicolas avant lancement (cf §6.3). Si le modèle actuel est trop cher, envisager GPT-4o-mini ou Claude Haiku pour le **seul backfill** (pas pour le temps réel où la qualité compte davantage).

5. **Le workflow AG4_V3 écrit déjà en pipe vers un LLM** — la latence d'ajout des 4 champs est marginale (+ quelques tokens en output), pas de risque de throttling significatif.

6. **La convention CSV des champs multi-valeurs** (plutôt qu'un JSON ou un array DuckDB natif) est imposée par cohérence avec `currencies_bullish` / `sectors_bullish` déjà en place. **Ne pas dériver vers `LIST(VARCHAR)` ou JSON** — même si plus propre, ça casserait les requêtes existantes.

7. **`impact_fx_pairs` format `XXXYYY` sans slash** : convention du projet (cf `ag4_fx_pairs.pair` existant). Ne pas introduire le format `EUR/USD`.

8. **`tagger_version` comparaison lexicographique** : `geo_v1 < geo_v2 < geo_v10`. Si on anticipe `v10+`, utiliser zero-pad (`geo_v001`). Peu probable à horizon 12 mois, accepté en l'état.

---

## 12. Résumé opérationnel (TL;DR pour Codex)

1. Backup DBs + init `ag4_forex_v1.duckdb` (§3.3).
2. Run `infra/migrations/ag4_v3/20260425_add_geo_tagging.sql` (§3.1).
3. Ouvrir le workflow n8n `AG4_V3`, étendre le prompt LLM (§4.2) et le schéma JSON (§4.3).
4. Ajouter le node "Sanitize & Write" (§4.4) puis "FX Conditional Write" (§7.1.3).
5. Créer `infra/config/sources/fx_sources.yaml` (§5.1), garder la plupart `enabled: false` pour démarrer.
6. Créer le workflow n8n `AG4_Forex` (§7.2) — consomme `fx_sources.yaml` + écrit dans `fx_news_history` avec `origin='fx_channel'`.
7. Écrire et exécuter `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py` (§6). Demander confirmation de coût avant d'appeler le LLM.
8. Laisser tourner 48 h et lancer les 8 requêtes de validation (§9).
9. Livrer à Nicolas un rapport court (pourcentages obtenus vs attendus + échantillons de news taguées pour revue qualitative) avant toute décision de passage live sur AG1_Forex.

---

**Références** :
- Audit valorisation : `docs/audits/20260423_audit_valorisation/report.md`
- Audit segments : `docs/audits/20260423_audit_valorisation/report_segments.md`
- Architecture 6 agents : `docs/architecture/...` (cf memory `project_architecture_6agents.md`)
- Snapshot d'audit : `snapshots/duckdb_20260422/ag4_v3.duckdb`
