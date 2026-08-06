# Contexte global AG5 à AG9

Statut live vérifié le 2026-08-06 : AG5–AG8 et la synthèse sont actifs ; AG9
reste en sommeil (`active=0`, aucun poids, aucun appel payant). Le pack compact
`AG1_GLOBAL_CONTEXT_LLM_V2` est attaché avant le fan-out des trois LLM AG1 et
reste `advisory_only=true`.

Ce dossier contient les déclencheurs n8n minces et les contrats communs. Les
services sont les seules sources de logique métier et les seuls writers :

- `macro-data-api` : AG5, AG6, AG7, AG8 → `macro_data.duckdb` ;
- `worldmonitor-adapter` : AG9 → `worldmonitor_v1.duckdb` ;
- `global-context-synthesizer` : snapshot atomique → `global_context_v1.duckdb`.

AG6 est une valorisation relative FX et n'est jamais présenté comme un moteur de
valorisation Actions. Aucun de ces workflows ne possède de nœud broker ou ordre.

Les fichiers JSON sont générés par
`agents/trading-forex/build_three_pillars_workflows.py`.

Contrats live : `AG5_MACRO_V2/AG5_MACRO_V3`,
`AG6_FX_VALUATION_V2/AG6_FX_VALUATION_V3`, `AG7_POSITIONING_V2`,
`AG8_RATES_V2/AG8_RATES_V3`, synthèse `GLOBAL_CONTEXT_SYNTHESIS_V2` et
compaction `GLOBAL_CONTEXT_LLM_COMPACTION_V3`.

Références : `docs/architecture/global_context_architecture.md`,
`docs/operations/20260806_ag5_ag8_data_quality_remediation.md` et
`docs/operations/ag5_ag9_runbook.md`.
