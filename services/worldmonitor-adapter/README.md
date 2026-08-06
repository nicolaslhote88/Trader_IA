# World Monitor Adapter / AG9

Adaptateur FastAPI faiblement couplé à World Monitor MCP/REST. Il découvre les
contrats d'outils, normalise les événements, trace les réponses brutes expurgées
et serait le writer unique de `worldmonitor_v1.duckdb`.

## Statut production

AG9 est dormant : workflow non publié, conteneur arrêté,
`WORLD_MONITOR_ENABLED=false`, aucune clé ni dépense. Le catalogue, les fixtures
et les tests restent disponibles pour une qualification future explicitement
autorisée. AG1 et la synthèse fonctionnent normalement sans AG9.

Endpoints quand le service est lancé : `GET /health`, `POST /admin/discover`,
`POST /ag9/refresh`, `GET /ag9/latest`, `/ag9/source-health`, `/ag9/runs`.

Ne jamais versionner de clé, payload réel ou base DuckDB. Références :
`docs/architecture/worldmonitor_integration.md` et
`docs/operations/20260805_ag9_dormant_free_tier.md`.

Tests :

```bash
python -m pytest -q services/worldmonitor-adapter/tests
```
