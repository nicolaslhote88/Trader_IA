# AG2-V2 - Workflow Final (Canonical)

## Source canonique

Le workflow de référence est:

- `ag2-v2/AG2-V2-workflow.final-loop-vector-test.json`

Les fichiers suivants sont des miroirs du canonique:

- `ag2-v2/AG2-V2-workflow.json`
- `ag2-v2/AG2-V2-workflow.vector-wired-proposed.json`

## Scripts de noeuds synchronisés

Le dossier `ag2-v2/nodes/` est aligné sur le workflow final:

- `01_init_config.js`
- `02_duckdb_init.py`
- `03a_wrap_h1.js`
- `03b_wrap_d1.js`
- `04_compute.py`
- `05_snapshot.js`
- `06a_merge_ai.js`
- `06_extract_ai.py`
- `07_hydrate_ai_cache.py`
- `09_mark_vector.py`
- `10_finalize.py`
- `11_sync_sheets.py`
- `12_build_vector_docs_final_loop.py`

Note: `08_prep_vector.js` est conservé en fichier legacy (non utilisé par le workflow final).

## Commandes utiles

Depuis `ag2-v2/`:

```bash
# Afficher/exporter le workflow canonique
python build_workflow.py > AG2-V2-workflow.json

# Resynchroniser les scripts nodes/* depuis le workflow canonique
python build_workflow.py --sync-nodes

# Mettre à jour les deux JSON miroir depuis le canonique
python build_workflow.py --sync-workflows

# Tout resynchroniser d'un coup
python build_workflow.py --write-files
```

## Paramètres clés du workflow final

- DuckDB: `/files/duckdb/ag2_v2.duckdb`
- YFinance API: `http://yfinance-api:8080`
- Qdrant collection: `financial_tech_v1`
- Rotation batch DuckDB: `BATCH_SIZE = 1` (dans `nodes/02_duckdb_init.py`)
