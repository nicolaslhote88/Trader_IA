# Contrats de données AG5–AG9

## Invariants communs

Une observation transporte, directement ou dans sa lineage :

```json
{
  "observation_time": "date économique ou événementielle réelle",
  "publication_time": "date de publication si connue",
  "ingestion_time": "date de collecte",
  "calculation_time": "date du score",
  "source": "source primaire",
  "unit": "unité explicite",
  "confidence": 0.0,
  "freshness_status": "fresh|aging|stale|missing",
  "is_proxy": false,
  "method_version": "version immuable"
}
```

`null` signifie indisponible. Il ne signifie jamais zéro ou neutre. Les scores
sont bornés dans `[-1,1]` pour AG5–AG8 et `[0,1]` pour le risque AG9. Les champs
JSON de lineage, entrées manquantes, poids normalisés et contributions restent
consultables dans les vues.

## AG5 — `components.ag5_macro`

Clé logique : `(component_snapshot_id, entity_id)`. Sortie : `macro_score`,
`subscores_json`, `coverage_ratio`, `confidence`, fraîcheur, entrées manquantes
et périmées, poids, contributions, lineage. La balance courante et le solde
budgétaire sont en `% du PIB`. Les taux neutres viennent de
`cfg.neutral_rates`, avec méthode, incertitude et faible confiance quand il
s'agit d'une estimation interne.

## AG6 — `components.ag6_fx_valuation`

Clé logique : `(component_snapshot_id, currency)`. Sortie : carry nominal et
réel, écart PPP seulement si spot et juste valeur existent, écart REER, termes
de l'échange, score, statut de chaque entrée et lineage. Portée contractuelle :
`FX_RELATIVE_VALUATION_ONLY`.

## AG7 — `components.ag7_positioning`

Clé logique : `(component_snapshot_id, entity_id)`. Le `report_date` est la date
hebdomadaire CFTC réelle et ne devient pas la date du run quotidien. Le score
contrarian vaut `clamp(-z/2)`. USD peut être une combinaison des contrats
contreparties : `is_proxy=true`, source `CFTC_SYNTHETIC_USD_BASKET`, confiance
plafonnée à `0,60`, contributeurs et poids visibles.

## AG8 — `components.ag8_rates_liquidity`

Clé logique : `(component_snapshot_id, currency)`. Les jambes 2Y/10Y, leurs
dates, la pente et la variation proche de J-30 sont persistées. La qualification
`bull_steepening`/`bear_steepening` nécessite aussi la direction des rendements ;
sinon le régime vaut `unknown`. Les sorties sont des régimes et pressions, jamais
des prescriptions `long/short`.

## AG9 — `worldmonitor_v1.duckdb`

Schémas :

- `raw.api_responses` : une réponse brute auditée par `request_id`, hash de
  payload, contrat d'outil, statut et erreur expurgée ;
- `cfg.tool_registry`, `cfg.entity_mappings`, `cfg.event_decay` : découverte et
  configurations versionnées ;
- `core.events` : événements normalisés, fingerprint, entités, score, decay,
  déduplication AG4 et lineage ;
- `core.snapshots`, `country_risk`, `chokepoint_status`, `energy_risk`,
  `supply_chain_risk`, `cyber_risk`, `sanctions`, `signal_convergence`,
  `temporal_anomalies`, `asset_impacts`, `sector_impacts`, `source_health`,
  `run_log` ;
- vues `main.v_latest_ag9_global_risk`, `v_latest_events`,
  `v_latest_country_risk`, `v_latest_sector_impacts`, `v_source_health`.

Les payloads bruts restent locaux, ne sont jamais placés dans Git et ne doivent
pas contenir de clé. Un pays ou actif non mappable demeure explicitement sans
exposition (`NO_RELIABLE_*_MAPPING`).

## Synthèse — `global_context_v1.duckdb`

`core.snapshots` référence exactement un snapshot de chaque composant disponible
via `component_snapshot_ids_json`, leurs millésimes et âges. La transaction écrit
en même temps `component_status`, `global_regime`, contextes pays/devise/secteur/
actif, événements critiques et lineage. Vues publiques :

- `main.v_latest_global_context`
- `main.v_component_health`
- `main.v_latest_country_context`
- `main.v_latest_currency_context`
- `main.v_latest_sector_context`
- `main.v_latest_asset_context`
- `main.v_latest_critical_events`
- `main.v_ag1_global_context_pack`

Le pack AG1 contient les cinq régimes séparés, overlays bornés, expositions du
portefeuille avant les opportunités, warnings, `advisory_only`, version et hash.
Le budget par défaut est 12 000 caractères.
