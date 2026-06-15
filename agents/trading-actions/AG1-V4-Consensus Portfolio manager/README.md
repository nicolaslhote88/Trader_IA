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
- Les fills confirmes IBKR alimentent `core.fills` et `core.fill_costs`.
  `core.fill_costs` reprend le format Forex pour suivre commissions brutes,
  devises, source IBKR et P&L net dans le dashboard Actions.

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

En live, `07b - IBKR Send Orders` ne cree pas de fill optimiste: il soumet
l'ordre puis interroge `/fills` pendant la fenetre `IBKR_FILL_CONFIRM_SECONDS`.
Un ordre non confirme reste `SUBMITTED` et n'impacte pas le ledger tant qu'un
fill IBKR n'est pas rattache.

Si IBKR refuse un ordre ou demande une confirmation non admissible, le node garde
l'ordre dans le bundle DuckDB avec `ibkrStatus=error/not_sent` ; le writer le
classe en `core.orders.status='REJECTED'` avec le motif dans `reason`. Cela
permet au dashboard Actions de montrer les tentatives bloquees sans creer de
position ni de fill.

Le broker peut confirmer automatiquement uniquement les prompts IBKR de
contrainte prix (`Percentage constraint`) quand
`IBKR_AUTO_CONFIRM_PRICE_WARNINGS=true` et que le prix limite reste dans le seuil
configure par `IBKR_PRICE_GUARD_MAX_DEVIATION_PCT` par rapport au quote
`yfinance-api`.

En mode live (`IBKR_DRY_RUN=false` et `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true`),
le node `2B - Init Run Context` interroge `/health` sur `ibkr-broker` avant les
agents LLM. Si `account_alignment.aligned` n'est pas `true` ou si le gateway
IBKR expose seulement un compte paper `DU...` alors que `IBKR_ACCOUNT_ID` vise un
compte réel `U...`, le workflow s'arrete avant toute consommation de tokens IA.
