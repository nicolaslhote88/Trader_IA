# AG4-V2 - News Watcher (DuckDB-first)

## Objectif
AG4-V2 collecte les flux RSS, dedupe les news, analyse les impacts marche/secteurs pour Agent #1, et utilise DuckDB (VPS) comme source de verite.
Google Sheets reste un miroir de controle humain.

## Architecture
1. Chargement des sources RSS depuis `Source_RSS`.
2. Initialisation DuckDB (`/files/duckdb/ag4_v2.duckdb`) + `run_id`.
3. Lecture de l'index historique depuis DuckDB (pas depuis Sheets).
4. Traitement news: normalisation, dedupe, clustering, pre-score, analyse IA si necessaire.
5. Ecriture continue dans DuckDB:
   - `news_history` pour news analysees/skipped
   - `news_errors` pour erreurs RSS
6. Fin de run:
   - maj `run_log`
   - extraction des lignes du run depuis DuckDB
   - sync vers Google Sheets `News_History`

## Tables DuckDB
- `news_history`
- `news_errors`
- `run_log`

## Fichiers
- `AG4-V2/AG4-V2-workflow.json` : workflow n8n final
- `AG4-V2/build_workflow.py` : regenere le JSON depuis `nodes/`
- `AG4-V2/nodes/*.js` : logique JS
- `AG4-V2/nodes/*.py` : logique DuckDB / run lifecycle

## Prerequis
Google Sheet document:
`1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I`

Onglets utilises:
- `Source_RSS` (`gid=1628829420`)
- `Universe` (`gid=1078848687`)
- `News_History` (`gid=1351236563`)

## Regeneration
```bash
python AG4-V2/build_workflow.py
```

Puis importer `AG4-V2/AG4-V2-workflow.json` dans n8n.

## Notes
- DuckDB est la reference pour dedupe et historique.
- `News_History` est alimente en sortie pour supervision humaine.
- Les erreurs RSS sont aussi synchronisees dans `News_History` avec `type=rss_error`.
