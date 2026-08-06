# Global Context Synthesizer

Service FastAPI qui sélectionne les snapshots exacts AG5–AG8/AG9, publie une
synthèse atomique dans `global_context_v1.duckdb` et expose le pack consultatif
destiné à AG1 V4.

## Statut live

- version API : `1.2.0` ;
- composants de production : `AG5,AG6,AG7,AG8` ;
- AG9 : dormant et exclu des poids ;
- contrat persisté : `GLOBAL_CONTEXT_V1/GLOBAL_CONTEXT_SYNTHESIS_V2` ;
- sortie LLM : `AG1_GLOBAL_CONTEXT_LLM_V2`, méthode
  `GLOBAL_CONTEXT_LLM_COMPACTION_V3`, 4 000 caractères maximum ;
- invariants : `advisory_only=true`, fail-open, aucun ordre ni quantité.

Endpoints : `GET /health`, `POST /synthesize`, `GET /latest`,
`GET|POST /ag1-pack`, `GET /runs`.

Le service est le seul writer de sa base. Les lecteurs (AG1, dashboard, replay)
restent en lecture seule. Voir `docs/architecture/global_context_architecture.md`
et `docs/operations/ag5_ag9_runbook.md`.

Tests :

```bash
python -m pytest -q services/global-context-synthesizer/tests
```
