# Macro Data API

Service central des données macro. Il conserve le framework Forex historique et
est désormais le writer unique des composants consultatifs AG5–AG8 dans
`macro_data.duckdb`.

Statut live vérifié le 2026-08-06 : API `2.1.0`, AG5/AG6/AG7/AG8 actifs,
contrats `AG5_MACRO_V3`, `AG6_FX_VALUATION_V3`, `AG7_POSITIONING_V2` et
`AG8_RATES_V3`. Les données absentes restent `null`, les proxies/fréquences sont
tracés et les workflows n8n n'écrivent pas directement en base.

Endpoints opérationnels : `/components/health`, `/components/{component}` et
les quatre refresh/compute `/components/ag5/refresh`, `/ag6/compute`,
`/ag7/refresh`, `/ag8/compute`. Architecture et qualité :
`docs/architecture/global_context_architecture.md` et
`docs/operations/20260806_ag5_ag8_data_quality_remediation.md`.

## Devises supportees

| Devise | Statut scoring | Macro/policy | Valorisation | Positionnement | Courbe 2Y/10Y | Notes |
|---|---|---|---|---|---|---|
| USD, EUR, JPY, GBP, CHF, CAD, AUD, NZD | Core legacy | high | high | high via `CFTC_COT`; USD est derive par panier inverse COT si absent | high si courbe disponible | Scores historiques preserves. |
| MXN | Extension scorables | high via FRED | high si CPI/policy disponibles | high via `CFTC_COT` (`095741`) | high via Banxico si configure, sinon override manuel/proxy policy | Priorite hors G8. |
| SEK, NOK | Extension proxy utilisable | medium/high via FRED | high si CPI/policy disponibles | medium si `OPTION_RR_25D`/`CME_OI`; sinon low via `RATE_CARRY_PROXY` | high si courbe disponible, medium via proxy policy | Le cube reste exploitable en taille reduite quand seul le proxy low-confidence est disponible. |
| KRW | Macro-only | high/medium selon series FRED | non score par defaut | non active sans source NDF fiable | high si courbe disponible | Peut enrichir le contexte macro, mais ne produit pas de score structurel tant que le positionnement est absent. |

## PositioningRecord

Le positionnement expose maintenant la notion de source et de confiance :

```python
class PositioningRecord:
    currency: str
    net_specs: float
    timestamp: datetime
    source: Literal[
        "CFTC_COT",
        "CFTC_COT_SYNTHETIC_USD_BASKET",
        "OPTION_RR_25D",
        "ETF_FLOWS",
        "CME_OI",
        "RATE_CARRY_PROXY",
    ]
    confidence: Literal["high", "medium", "low"]
```

Mapping de confiance :

| Source | Confidence |
|---|---|
| `CFTC_COT` | high |
| `CFTC_COT_SYNTHETIC_USD_BASKET` | medium |
| `OPTION_RR_25D` | medium |
| `CME_OI` | medium |
| `ETF_FLOWS` | low |
| `RATE_CARRY_PROXY` | low |

## Gating de completude

Une devise etendue est consideree scorables si le plancher de confiance entre macro, valorisation, positionnement et courbe des taux est au moins `medium`.

Exception controlee : si seule la jambe positionnement est disponible via
`RATE_CARRY_PROXY`, la devise recoit `data_completeness=proxy_complete` et
`score_status=scored_proxy`. Le score structurel est alors calcule pour ne pas
supprimer inutilement l'espace d'opportunite d'AG1, mais AG1 ne peut l'utiliser
qu'en taille reduite et doit citer la limite de confiance.

Les champs ajoutes a `pillars.currency_scores` sont :

- `data_completeness`: `complete`, `proxy_complete` ou `data_incomplete`
- `score_status`: `scored`, `scored_proxy`, `scored_legacy` ou `data_incomplete`
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

## Fallbacks de taux directeurs

Quand FRED ne publie pas d'observation utilisable pour une banque centrale, le
service accepte un override operationnel :

- `{CCY}_POLICY_RATE_PCT`
- `{CCY}_POLICY_RATE_AS_OF`
- `{CCY}_POLICY_RATE_SOURCE`

Un fallback audite est embarque pour SEK (`Riksbank_official_static`,
`1.75%`, applicable au `2026-05-13`) parce que la serie FRED disponible dans le
catalogue ne renvoie pas d'observation exploitable via l'API. Ce fallback est
classe comme source explicite et peut etre remplace par variable d'environnement.

## Notes operationnelles AG1 cube

Le brief AG1 expose maintenant, pour chaque paire, `structural_data_quality`,
`structural_confidence_floor` et `structural_proxy_used` dans `decision.cube`.

- `official_or_medium` : les deux jambes ont au moins une confiance medium.
- `proxy_usable` : le signal Z existe, mais au moins une devise depend d'un proxy low-confidence.
- `incomplete` : pas de decision structurelle fiable.

Les nouvelles ouvertures restent interdites hors zones
`convergence_multi_horizon_*`. Si la zone converge mais que la qualite est
`proxy_usable`, le validateur applique le mode `REDUCED_SIZE_ONLY`.
