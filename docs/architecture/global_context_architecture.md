# Architecture du contexte global AG5–AG9

## Statut

Architecture implémentée le 2026-08-05 et remédiée côté qualité des données le
2026-08-06. En production, AG5–AG8 et le contexte consultatif sont actifs ; AG9
reste dormant. L'ensemble ne possède aucun transport d'ordre.

## Flux et responsabilités

```text
FRED / CFTC / World Bank / yfinance       World Monitor MCP ou REST
                 |                                  |
                 v                                  v
        macro-data-api                     worldmonitor-adapter
       writer unique AG5-AG8                  writer unique AG9
                 |                                  |
       macro_data.duckdb                  worldmonitor_v1.duckdb
                 +------------------+---------------+
                                    v
                         global-context-synthesizer
                         writer unique / transaction
                                    |
                         global_context_v1.duckdb
                           /                    \
                  lecture seule               HTTP pack exact
                     dashboard               AG1 V4 avant fan-out
                                                    |
                                  même objet -> 3 branches LLM
                                                    |
                                consensus / Risk / broker inchangés
```

Les workflows n8n AG5–AG9 sont de minces déclencheurs HTTP. Ils n'écrivent
jamais directement dans DuckDB. Le dashboard ne recalcule ni score, ni
fraîcheur, ni exposition : il lit uniquement les vues persistées.

## Bases, writers et lecteurs

| Base | Writer unique | Lecteurs | DuckDB maximal |
|---|---|---|---:|
| `macro_data.duckdb` | `macro-data-api` | synthèse, dashboard | 1.1.3 actuellement |
| `worldmonitor_v1.duckdb` | `worldmonitor-adapter` | synthèse, dashboard | 1.4.3 |
| `global_context_v1.duckdb` | `global-context-synthesizer` | AG1, dashboard, replay | 1.4.3 |
| `ag1_v4_consensus.duckdb` | workflows AG1/MTM existants | dashboard, replay | 1.4.3 |

Toutes les lectures inter-base utilisent `read_only=True` et un retry borné
sur lock. Les publications AG9 et contexte global sont transactionnelles. Un
échec conserve le dernier snapshot valide et écrit un run d'erreur séparé.

## Frontières métier

- AG5 décrit la macro et les flux structurels.
- AG6 décrit uniquement la valorisation relative FX et le risque de change ; il
  ne remplace pas AG3 Actions.
- AG7 décrit le positionnement CFTC et les attentes ; le sens COT est
  contrarian, sans recommandation.
- AG8 décrit les régimes taux/liquidité et des pressions cross-asset ; il ne
  produit aucun ordre.
- AG9 transforme des événements globaux structurés en risques explicables.
- Le synthétiseur juxtapose ces horizons et publie couverture/confiance. Il ne
  fabrique pas une moyenne directionnelle AG5–AG9.

AG4 reste `AG4_NEWS_SENTIMENT`; AG9 reste `AG9_GLOBAL_RISK`. Une empreinte
normalisée URL/titre+jour et la lineage `derived_from` rendent les doublons
visibles sans transformer deux sources corrélées en deux confirmations.

## Injection AG1 V4

`Fetch Global Advisory Pack` et `Attach Advisory Pack` s'insèrent après la
construction du brief et avant le preflight existant. Un seul pack canonique est
attaché à l'item, puis le nœud de preflight distribue le même item aux trois
branches. Le pack est `advisory_only=true`; aucune règle déterministe de quantité,
de gate ou de consensus ne le lit.

Le snapshot complet `AG1_GLOBAL_CONTEXT_PACK_V1` reste persisté dans
`global_context_v1.duckdb` pour l'audit et le dashboard. L'endpoint
`POST /ag1-pack` le compacte à la lecture en `AG1_GLOBAL_CONTEXT_LLM_V2` : il
ne transmet aux LLM que les devises réellement présentes dans le portefeuille
ou les opportunités, résume les expositions inconnues et omet les détails dont
la fraîcheur ou la confiance est insuffisante. La politique `use_policy` est
explicite : `IGNORE`, `CAVEAT_ONLY`, `CAUTION` ou `NORMAL`. Un contexte global
dégradé ne peut donc plus être interprété comme un signal détaillé valide.

En indisponibilité, l'attache fournit `GLOBAL_CONTEXT_UNAVAILABLE` ou
`GLOBAL_CONTEXT_DISABLED`. L'ancien chemin décisionnel reste donc utilisable.
Le workflow shadow retire en plus preflight, safety, broker et writer : il se
termine juste après consensus dans `Shadow Capture (NO BROKER)`.

## Versions de contrats

- `AG5_MACRO_V2` / méthode `AG5_MACRO_V3`
- `AG6_FX_VALUATION_V2` / méthode `AG6_FX_VALUATION_V3`
- `AG7_POSITIONING_V2`
- `AG8_RATES_V2` / méthode `AG8_RATES_V3`
- `AG9_GLOBAL_RISK_V1` / `AG9_EVENT_RISK_V1`
- `GLOBAL_CONTEXT_V1` / `GLOBAL_CONTEXT_SYNTHESIS_V2`
- `AG1_GLOBAL_CONTEXT_PACK_V1` (snapshot complet persisté)
- `AG1_GLOBAL_CONTEXT_LLM_V2` / `GLOBAL_CONTEXT_LLM_COMPACTION_V3` (payload LLM)

Les identifiants de snapshot, millésimes, âges, versions de méthode et hashes
SHA-256 sont persistés pour chaque run AG1 enrichi.
