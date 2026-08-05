# AG5 à AG8 — wrappers n8n historiques

Les identifiants n8n historiques sont conservés pour permettre une mise à jour
in-place, mais les formules et écritures DuckDB ont été supprimées de ces
workflows. Les workflows générés appellent désormais les endpoints canoniques de
`services/macro-data-api/`, seul writer de `macro_data.duckdb`.

La documentation commune et les miroirs des workflows sont dans
`agents/common/global-context/`. Le Forex et tous ses Portfolio Managers restent
désactivés; AG5, AG7 et AG8 sont des composants communs, AG6 reste explicitement
une valorisation relative des devises.

Régénération :

```powershell
python agents/trading-forex/build_three_pillars_workflows.py
```
