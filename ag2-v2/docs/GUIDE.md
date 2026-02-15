# AG2-V2 - Technical Analyst Pipeline

## Etat des workflows locaux

Le dossier contient maintenant **2 exports n8n** :

- `ag2-v2/AG2-V2-workflow.json`
  - Snapshot **fidele au workflow actuel n8n** fourni (initialisation + indicateurs + analyse IA + sync Sheets).
  - La branche vectorisation est presente mais **non cablee** au flux principal.

- `ag2-v2/AG2-V2-workflow.vector-wired-proposed.json`
  - Variante **proposee** avec cablage vectoriel fonctionnel.
  - Les sorties `Extract AI + Write` **et** `Hydrate AI from cache` passent par `IF Vectorize?` puis Qdrant si `should_vectorize=true`.

## Scripts de noeuds synchronises

Les scripts sous `ag2-v2/nodes/` sont alignes avec la version n8n courante :

- `01_init_config.js`
- `02_duckdb_init.py`
- `03a_wrap_h1.js`
- `03b_wrap_d1.js`
- `04_compute.py`
- `05_snapshot.js`
- `06a_merge_ai.js`
- `06_extract_ai.py`
- `07_hydrate_ai_cache.py` (ajoute)
- `08_prep_vector.js`
- `09_mark_vector.py`
- `10_finalize.py`
- `11_sync_sheets.py`

## Proposition de cablage vectoriel (recommandee)

### Pourquoi

Dans le workflow actuel, la partie vectorielle ne recoit aucun item du flux principal.
Resultat: `Prep Vector Text` / `Qdrant Upsert` / `Mark Vectorized` ne tournent pas pendant l'execution AG2.

### Design recommande

- Ajouter un `IF Vectorize?` base sur:
  - `={{ $json.should_vectorize.toString().trim().toBoolean() }}`
- Brancher vers ce IF depuis:
  - `Extract AI + Write`
  - `Hydrate AI from cache`
- Sortie `true`:
  - `Prep Vector Text -> Qdrant Upsert -> Mark Vectorized -> Loop Symbols`
- Sortie `false`:
  - retour direct `Loop Symbols`

Ce design garde la logique symbole-par-symbole, met a jour `vector_status` au fil de l'eau, et evite d'attendre la fin de run.

## Build / regeneration

`ag2-v2/build_workflow.py` gere 2 variantes:

```bash
# Export courant (snapshot n8n actuel)
python ag2-v2/build_workflow.py > ag2-v2/AG2-V2-workflow.json

# Export propose avec vectorisation cablee
python ag2-v2/build_workflow.py --variant vector-wired > ag2-v2/AG2-V2-workflow.vector-wired-proposed.json

# Ecrit les 2 fichiers d'un coup
python ag2-v2/build_workflow.py --write-files
```

## Notes d'exploitation

- Base DuckDB: `/files/duckdb/ag2_v2.duckdb`
- YFinance API: `http://yfinance-api:8080`
- Collection Qdrant: `financial_tech_v1`
- Le calcul de `should_vectorize` est deja alimente par:
  - `Extract AI + Write`
  - `Hydrate AI from cache`

