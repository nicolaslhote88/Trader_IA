# AG1 V4 Consensus Portfolio Manager

AG1 V4 remplace la concurrence entre trois workflows actions par un workflow
unique. Le meme brief d'entree est envoye en parallele a GPT, Grok et Gemini,
puis un node de consensus autorise une intention d'ordre seulement si au moins
deux modeles sur trois votent le meme symbole et le meme intent executable.

## Principes

- Perimetre : actions/ETF uniquement cote execution, avec rejet explicite du
  Forex (`FX:`, `=X`, asset class devise).
- Base separee : `/files/duckdb/ag1_v4_consensus.duckdb`.
- Capital initial : 10 000 EUR.
- Aucun import historique AG1 V3.
- IBKR reste dry-run par defaut. Le passage paper/live depend des gates
  `IBKR_DRY_RUN`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED` et
  `AG1_V4_ACTIONS_IBKR_ENABLED_MODELS`.

## Contenu

- `workflow/AG1_workflow_template_v4.json` : socle de generation issu de la
  topologie V3; le script y injecte les nodes V4 et le consensus.
- `workflow/AG1_workflow_v4_consensus.json` : export n8n importable, source de
  verite operationnelle.
- `workflow/build_v4_workflow.py` : regenere l'export depuis le template et les
  nodes extraits.
- `workflow/nodes/post_agent/06_build_consensus_v4.code.js` : moteur de vote 2/3.
- `workflow/sql/portfolio_ledger_schema_v4.sql` : schema ledger V4 avec tables
  `core.model_proposals`, `core.consensus_votes` et
  `core.consensus_decisions`.

## Flux

`2B/4B/4C/AG4/R8/Calcul/Merge7/AG1.00`
-> fan-out GPT/Grok/Gemini
-> extracteurs tagues par modele
-> `AG1.V4 - Build Consensus`
-> `7 - Validate & Enforce Safety`
-> `07b - IBKR Send Orders`
-> `8/9/10 DuckDB`.
