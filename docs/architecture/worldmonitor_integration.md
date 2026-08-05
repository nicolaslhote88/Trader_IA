# Intégration World Monitor

## Choix technique et licence

Le projet World Monitor consulté le 2026-08-05 est distribué sous
`AGPL-3.0-only`. Une licence commerciale distincte est annoncée pour les usages
propriétaires hors AGPL. Trader_IA ne copie aucun code, composant UI ou asset de
World Monitor : `services/worldmonitor-adapter/` communique uniquement avec son
interface MCP/REST. Ce découplage réduit le risque de dérivation logicielle,
mais ne remplace pas un avis juridique pour un usage professionnel/commercial.

## Endpoints et authentification

- MCP privilégié : `https://worldmonitor.app/mcp`.
- Header de clé documenté : `X-WorldMonitor-Key`.
- Modes : `mcp`, `api`, `self_hosted` via URL configurable.
- Aucun secret dans le JSON n8n, les logs, DuckDB de config ou le dashboard.

Le catalogue anonyme du 2026-08-05 annonce protocole `2025-03-26`, serveur
`worldmonitor 1.15.0` et 59 outils. Le registry local conserve nom, schéma,
description disponible, hash de contrat, compatibilité et date de découverte.
Les outils absents/incompatibles ne sont jamais remplacés silencieusement.
`POST /admin/discover?catalog_only=true` permet de versionner ce catalogue sans
activer les appels de données ni configurer une clé; la collecte demeure bloquée.

Capacités ciblées : conflits, risque pays, sanctions, posture militaire,
convergence, anomalies, focal points, news intelligence, cyber, chokepoints,
maritime, espace aérien, énergie, supply chain, catastrophes, infrastructure,
tarifs, macro pays et marchés. Les candidats exacts sont versionnés dans
`config/capabilities.json`; `get_supply_chain_data` reflète le catalogue observé.

## Transport et panne

Les appels ont timeout borné, retries configurables, classification explicite
des erreurs auth/quota/transport et redaction. Une collecte sans réponse valide
échoue avec `AG9_ZERO_VALID_EVENTS`; elle n'écrase pas le dernier snapshot. Une
panne AG9 devient `AG9_UNAVAILABLE`/`DEGRADED` dans la synthèse et ne casse pas
AG1.

À la date de l'implémentation, `tools/list` était accessible sans credential,
mais `tools/call` et `describe_tool` demandaient un abonnement/credential. Les
tests réels de données et de quota restent donc bloqués ; les tests CI utilisent
14 fixtures, jamais le service distant.

## Normalisation

```text
severity [0,1]
confidence [0,1]
source_diversity = min(1, sources_distinctes / minimum)
freshness_decay = exp(-ln(2) * age_heures / demi_vie_type)
effective_score = severity * confidence * source_diversity
                  * freshness_decay * relevance
aggregate = 1 - product(1 - effective_score_i)
```

Le score n'existe pas si sévérité ou date d'événement manque. Les demi-vies
varient de 12 h (marché) à 1 440 h (risque pays); sanctions 1 080 h, conflit
336 h, cyber 48 h. Les seuils sont versionnés dans `event_decay.json`.

Déduplication : URL normalisée, sinon titre normalisé, plus jour événementiel.
Les contributeurs sont fusionnés dans `derived_from`; l'agrégation est bornée.
Une empreinte présente dans AG4 est marquée `ag4_duplicate=true` et n'est pas
présentée comme une seconde confirmation indépendante : elle reste dans
`core.events` pour audit mais est exclue de l'agrégat et des événements critiques
AG9 transmis au PM.

## Références officielles vérifiées

- MCP Quickstart : <https://www.worldmonitor.app/docs/mcp-quickstart>
- API reference : <https://www.worldmonitor.app/docs/api-reference>
- Agent discovery : <https://www.worldmonitor.app/docs/agent-discovery>
- Authentification : <https://www.worldmonitor.app/docs/usage-auth>
- Rate limits : <https://www.worldmonitor.app/docs/usage-rate-limits>
