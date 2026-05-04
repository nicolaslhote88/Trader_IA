# AG4-V3 - News Watcher (DuckDB-first)

## Objectif
AG4-V3 collecte les flux RSS, dedupe les news, analyse les impacts marche/secteurs/devise pour Agent #1, et utilise DuckDB (VPS) comme source de verite.
Google Sheets est utilise uniquement pour les entrees de configuration.

## Architecture
1. Declenchement 4 fois par jour ouvré (`01:45`, `06:45`, `10:45`, `18:45`, Europe/Paris) pour eviter les chevauchements de runs longs et les verrous DuckDB.
2. Chargement des sources RSS depuis `Source_RSS`.
3. Initialisation DuckDB (`/files/duckdb/ag4_v3.duckdb`) + `run_id`.
4. Lecture de l'index historique depuis DuckDB (pas depuis Sheets).
5. Traitement news: normalisation, dedupe, clustering, pre-score, analyse IA si necessaire.
6. Ecriture continue dans DuckDB:
   - `news_history` pour news analysees/skipped
   - `news_errors` pour erreurs RSS
7. Fin de run:
   - maj `run_log`
   - consolidation des sorties dans DuckDB

## Tables DuckDB
- `news_history` (inclut `currencies_bullish` / `currencies_bearish`)
- `news_errors`
- `run_log`

## Fichiers
- `AG4-V3/AG4-V3-workflow.json` : workflow n8n final
- `AG4-V3/build_workflow.py` : regenere le JSON depuis `nodes/`
- `AG4-V3/nodes/*.js` : logique JS
- `AG4-V3/nodes/*.py` : logique DuckDB / run lifecycle

## Prerequis
Google Sheet document:
`1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I`

Onglets utilises:
- `Source_RSS` (`gid=1628829420`)
- `Universe` (`gid=1078848687`)

## Regeneration
```bash
python AG4-V3/build_workflow.py
```

Puis importer `AG4-V3/AG4-V3-workflow.json` dans n8n.

## Notes
- DuckDB est la reference pour dedupe et historique.
- Les sorties AG4 restent persistantes dans DuckDB et exploitables par le dashboard/agents.
