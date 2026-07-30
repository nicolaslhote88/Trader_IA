# AG1 V4 Workflow Pack

Socle de generation: `AG1_workflow_template_v4.json`.

Export importable dans n8n et source de verite operationnelle:
`AG1_workflow_v4_consensus.json`.

## Generation

```bash
python "agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/build_v4_workflow.py"
```

Le script injecte les codes extraits dans le workflow, configure les trois
branches actives `gpt-5.6-sol` / `deepseek-v4-pro` / `claude-opus-4-8`, route
`AG1.00` en parallele, ajoute le merge 4 entrees puis le node de consensus.

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

Importer `AG1_workflow_v4_consensus.json`. Le workflow est exporte inactif par
defaut pour eviter une activation involontaire pendant le deploiement.
