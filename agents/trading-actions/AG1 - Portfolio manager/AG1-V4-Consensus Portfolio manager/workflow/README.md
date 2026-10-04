# AG1 V4 Workflow Pack

Socle de generation: `AG1_workflow_template_v4.json`.

Export importable dans n8n et source de verite operationnelle:
`AG1_workflow_v4_consensus.json`.

## Generation

```bash
python "agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/build_v4_workflow.py"
```

Le script injecte les codes extraits dans le workflow, configure les trois
branches actives `gpt-6-sol` / `deepseek-v4-pro` / `claude-opus-5-5`, route
`AG1.00` en parallele, ajoute le merge 4 entrees puis le node de consensus.

Depuis le 2026-09-25, Claude utilise un préparateur Code, le nœud HTTP
`Anthropic Messages - Opus5.5` avec le credential Anthropic existant, puis un
décodeur qui valide le schéma métier complet avant l’extracteur. Le mode
`adaptive` remplace le nœud Anthropic incompatible de n8n 2.3.5. GPT-6 Sol
a été validé avec Responses API et effort `medium` explicites. La dernière
sauvegarde live retire ces deux options OpenAI : elle est conservée telle quelle
dans cet export. Cette configuration a depuis été rejouée avec succès lors du
raccordement historique du 2026-10-04 (voir sa note de déploiement).
La transformation est centralisée dans `migrate_models_20260925.py`.

La branche DeepSeek utilise une `Basic LLM Chain` avec parseur structure et
retry, car le node Agent peut convertir le schema en appel d'outil et echouer
avant l'extracteur sur des arguments JSON concatenes. Les extracteurs valident
la forme metier et distinguent explicitement `UPSTREAM_ERROR`,
`INVALID_SHAPE` et les sorties JSON valides.

Les cles DuckDB historiques `chatgpt52`, `grok41_reasoning` et
`claude_sonnet46` sont conservees pour la compatibilite du ledger. Les champs
`model_name` et `model_id` portent les modeles reels actuels.

## Contrat DuckDB

- Base : `/files/duckdb/ag1_v4_consensus.duckdb`
- Writer : `/files/AG1-V4-EXPORT/nodes/post_agent/duckdb_writer.py`
- Schema : `/files/AG1-V4-EXPORT/sql/portfolio_ledger_schema_v4.sql`
- Capital initial seed : 10 000 EUR dans `cfg.portfolio_config` et
  `core.cash_ledger`.

Le node 9 V4 refuse le vieux chemin partage `ag1_v3.duckdb` et privilegie les
variables `AG1_V4_*`.

## Import n8n

`AG1_workflow_v4_consensus.json` reflète le graphe publié, avec son état actif.
Un import n8n désactive le workflow : republier explicitement après validation,
puis vérifier `active=1` et la version publiée après redémarrage des runners.

## Faits historiques — live le 4 octobre 2026

Deux nœuds après `AG1.V4 — Liquidity Preflight` récupèrent et attachent les
comptes historiques datés avant les trois modèles et le merge de contexte.
Builder ciblé : `outils/scripts/build_ag1_history_live.py` à la racine du dépôt.
Source JS : `nodes/agent_input/historical_evidence_attach.code.js`.
Les nœuds modèles existants sont préservés et ont été rejoués dans cette version.
Les probabilités prédictives ne sont pas transmises. Voir la note
`docs/operations/20261004_ag1_historical_evidence_live.md` pour les preuves,
la version publiée et le retour arrière. Le builder historique général ci-dessus
ne remplace pas ce patch ciblé sur un export publié récent.
