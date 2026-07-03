# AG1 V4 - Fix SELL/DECREASE targetWeightPct hors opportunity_pack (2026-06-22)

## Contexte
- Incident observe le 2026-06-22 apres-midi : consensus vendeur `ELEC.PA` non transforme en ordre.
- Run concerne : `RUN_20260622_160005_19307` / execution n8n `19307` a 16:00 Paris.
- Broker IBKR sain au diagnostic : `authenticated=true`, `dry_run=false`, compte live `U25651155` aligne, aucune approbation pendante.

## Cause racine
- `AG1.V4 - Build Consensus` convertissait `targetWeightPct` en `targetQty` uniquement via `opportunity_pack.rows[].entry`.
- Les ventes de positions detenues peuvent porter sur des lignes absentes du `opportunity_pack`.
- Dans ce cas, `matrix={}` et `matrix.entry=NULL`, donc les votes `DECREASE targetWeightPct=4` etaient rejetes avant broker :
  `REJECTED_MISSING_TARGET_WEIGHT`.

## Correctif deploye
- Fichier : `agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/post_agent/06_build_consensus_v4.code.js`
  - Ajout `pickPositionPrice(portfolioSummary, symbol)`.
  - Pour `intent === "SELL"`, fallback de reference prix : `matrix.entry` puis prix position (`LastPrice` / `lastPrice` / `MarketValue / Quantity`).
  - Ajout `priceHint` dans l'action consensus pour le node safety.
- Fichier : `agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/build_v4_workflow.py`
  - Ajout de `AG1.V4 - Build Consensus` dans `CODE_MAP`, sinon le builder ne recopiait pas le node modifie dans l'export JSON.
- Fichier : `agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/tests/smoke_post_agent_v4.js`
  - Ajout d'un cas SELL held hors pack : deux votes `DECREASE targetWeightPct=4` sur `ELEC.PA`, position 8 titres a 95 EUR, attendu ordre SELL 4.
- Export regenere :
  `agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/AG1_workflow_v4_consensus.json`.

## Validation
- Smoke test execute dans `root-n8n-1` avec copie temporaire :
  `buyDecision=TRADE`, `sellHeldDecision=TRADE`, `sellHeldOrders=1`, `sellHeldQuantity=4`.
- Verif post-deploiement n8n :
  - `AG1V4CONSENSUS active=1`
  - `versionId == activeVersionId == 80a32f9f-fbae-4e47-9744-30496b33e769`
  - `workflow_entity` et `workflow_history` contiennent `pickPositionPrice` et `priceHint: selectedLimit`.
- Verif broker post-deploiement :
  - `authenticated=true`
  - `dry_run=false`
  - compte `U25651155`
  - `approvals.pending=[]`

## Deploiement
Backup live :
`/tmp/ag1v4_backups/AG1V4CONSENSUS_backup_20260622_220903.json`
et copie locale `.codex-tmp/AG1V4CONSENSUS_backup_20260622_220903.json`.

Commandes executees :
```bash
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG1_workflow_v4_consensus_fix.json
docker exec root-n8n-1 n8n publish:workflow --id=AG1V4CONSENSUS
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

## Rollback
```bash
docker cp /tmp/ag1v4_backups/AG1V4CONSENSUS_backup_20260622_220903.json root-n8n-1:/tmp/AG1V4CONSENSUS_rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG1V4CONSENSUS_rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG1V4CONSENSUS_rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=AG1V4CONSENSUS
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

## Notes
- Aucun ordre n'a ete place manuellement pendant l'investigation.
- Le prochain consensus vendeur pourra generer l'ordre ; ne pas declencher manuellement AG1 en live sans decision explicite.
- Une erreur d'activation SIGA (`SIGA - Telegram Batch Processor v8`) apparait au restart n8n, liee a un token OAuth expire et hors perimetre Trader_IA.
