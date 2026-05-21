# Macro Data API

Service central du framework Forex 3 Piliers. Il alimente les tables `macro.*`, `cot.*`, `rates.*` et `pillars.*` dans `macro_data.duckdb`.

## Devises supportees

| Devise | Statut scoring | Macro/policy | Valorisation | Positionnement | Courbe 2Y/10Y | Notes |
|---|---|---|---|---|---|---|
| USD, EUR, JPY, GBP, CHF, CAD, AUD, NZD | Core legacy | high | high | high via `CFTC_COT` | high si courbe disponible | Scores historiques preserves. |
| MXN | Extension scorables | high via FRED | high si CPI/policy disponibles | high via `CFTC_COT` (`095741`) | high via Banxico si configure, sinon override manuel/proxy policy | Priorite hors G8. |
| SEK, NOK | Extension conditionnelle | high via FRED | high si CPI/policy disponibles | medium si proxy `OPTION_RR_25D` ou `CME_OI` charge | high si courbe disponible | Sans proxy positionnement, statut `data_incomplete`. |
| KRW | Macro-only | high/medium selon series FRED | non score par defaut | non active sans source NDF fiable | high si courbe disponible | Peut enrichir le contexte macro, mais ne produit pas de score structurel tant que le positionnement est absent. |

## PositioningRecord

Le positionnement expose maintenant la notion de source et de confiance :

```python
class PositioningRecord:
    currency: str
    net_specs: float
    timestamp: datetime
    source: Literal["CFTC_COT", "OPTION_RR_25D", "ETF_FLOWS", "CME_OI"]
    confidence: Literal["high", "medium", "low"]
```

Mapping de confiance :

| Source | Confidence |
|---|---|
| `CFTC_COT` | high |
| `OPTION_RR_25D` | medium |
| `CME_OI` | medium |
| `ETF_FLOWS` | low |

## Gating de completude

Une devise etendue est consideree scorables si le plancher de confiance entre macro, valorisation, positionnement et courbe des taux est au moins `medium`.

Les champs ajoutes a `pillars.currency_scores` sont :

- `data_completeness`: `complete` ou `data_incomplete`
- `score_status`: `scored`, `scored_legacy` ou `data_incomplete`
- `confidence_floor`: `high`, `medium`, `low` ou `missing`
- `missing_inputs`: liste JSON des familles manquantes

Les 8 devises historiques conservent leurs scores existants avec le statut `scored_legacy` si une famille annexe manque, afin d'eviter une regression brutale de production.

## Migration DB

Migration idempotente : `infra/migrations/macro_data/20260520_add_non_g8_currency_confidence.sql`.

`MacroDB` applique aussi ces ajouts au demarrage pour les fichiers DuckDB deja existants.

## Sources MXN yield curve

FRED ne fournit pas de courbe MXN 2Y/10Y suffisamment fiable pour ce module. Le chemin production est Banxico SIE, configurable par variables :

- `BANXICO_API_TOKEN`
- `BANXICO_MXN_YIELD_2Y_SERIES_ID`
- `BANXICO_MXN_YIELD_10Y_SERIES_ID`

Fallback operationnel possible :

- `MXN_YIELD_2Y_PCT`
- `MXN_YIELD_10Y_PCT`
- `MXN_YIELD_SOURCE`
