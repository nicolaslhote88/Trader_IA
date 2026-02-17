# AG4_Spe-V2 - News specifique par symbole (DuckDB-first)

## Objectif
AG4_Spe-V2 collecte les actualites Boursorama **specifiques a chaque valeur** (symbol), dedupe les articles, applique une analyse IA ciblee par societe, puis stocke le resultat directement dans DuckDB.

Ce workflow remplace la logique Google Sheets `news_raw_Symbol` par une base durable.

## Architecture
1. Init DuckDB + creation schema (`universe_symbols`, `news_history`, `news_errors`, `run_log`).
2. Chargement de l'univers depuis DuckDB (`universe_symbols`, fallback `universe` AG2).
3. Queue rotative (batch 20 symboles par run).
4. Scraping de la page `cours/actualites/<ref>/` pour chaque symbole.
5. Extraction + normalisation des URLs article.
6. Dedupe via DuckDB (`news_id = sha1(symbol|canonical_url)`).
7. Fetch article HTML + parsing titre/date/snippet/texte.
8. Filtre age news + analyse IA (JSON schema strict).
9. Upsert dans `news_history`, erreurs dans `news_errors`, stats de run dans `run_log`.

## Tables DuckDB
- `universe_symbols`
- `news_history`
- `news_errors`
- `run_log`

DB par defaut: `/files/duckdb/ag4_spe_v2.duckdb`

## Preparer l'univers (option recommandee)
```sql
INSERT OR REPLACE INTO universe_symbols
(symbol, name, enabled, boursorama_ref, exchange, currency, country, asset_class)
VALUES
('AI.PA', 'AIR LIQUIDE', TRUE, '1rPAI', 'Euronext Paris', 'EUR', 'FR', 'Equity');
```

Le workflow peut aussi fallback sur la table `universe` (AG2) si `universe_symbols` est vide.

## Fichiers
- `AG4-SPE-V2/build_workflow.py` : genere le workflow n8n final
- `AG4-SPE-V2/nodes/*.js` : logique parsing/routage/LLM
- `AG4-SPE-V2/nodes/*.py` : logique DuckDB / dedupe / run lifecycle
- `AG4-SPE-V2/AG4-SPE-V2-workflow.json` : workflow a importer dans n8n

## Regeneration
```bash
python AG4-SPE-V2/build_workflow.py
```

Puis importer `AG4-SPE-V2/AG4-SPE-V2-workflow.json` dans n8n.

