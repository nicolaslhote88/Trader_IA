# Contexte global AG5 à AG9

Ce dossier contient les déclencheurs n8n minces et les contrats communs. Les
services sont les seules sources de logique métier et les seuls writers :

- `macro-data-api` : AG5, AG6, AG7, AG8 → `macro_data.duckdb` ;
- `worldmonitor-adapter` : AG9 → `worldmonitor_v1.duckdb` ;
- `global-context-synthesizer` : snapshot atomique → `global_context_v1.duckdb`.

AG6 est une valorisation relative FX et n'est jamais présenté comme un moteur de
valorisation Actions. Aucun de ces workflows ne possède de nœud broker ou ordre.

Les fichiers JSON sont générés par
`agents/trading-forex/build_three_pillars_workflows.py`.
