# AG4_Spé-V2 — correction des crashes heap n8n (2026-07-24)

## Symptôme

Les cinq exécutions n8n `20369`, `20381`, `20388`, `20396` et `20405` ont été
marquées `crashed`. Elles ont démarré entre le 22/07 15:05 UTC et le 23/07
15:05 UTC et ont duré environ 69 à 80 minutes.

## Cause validée

Le workflow métier avait terminé correctement avant chaque crash : les cinq
runs correspondants dans `ag4_spe_v2.duckdb.run_log` sont `SUCCESS`, avec 111 à
147 articles analysés et `finished_at` renseigné.

Le processus n8n tombait immédiatement après `S24 - Finalize Run`, pendant la
clôture/sérialisation de l'exécution. Les logs contiennent :

```text
FATAL ERROR: CALL_AND_RETRY_LAST Allocation failed - JavaScript heap out of memory
```

Le heap atteignait environ 1,7 à 1,9 Gio. Le dernier run encore sauvegardé avec
succès (`20359`) occupait déjà 173 396 182 octets dans
`execution_data.data`. Les réponses HTML des nodes HTTP sont conservées dans le
graphe d'exécution; leur volume a augmenté avec le nombre d'articles. Le
conteneur `root-n8n-1` a redémarré exactement cinq fois et n8n a ensuite marqué
les exécutions inachevées `crashed`.

Ce n'était ni une erreur DuckDB, ni une panne Boursorama, ni un timeout runner,
ni un OOM-kill du noyau.

## Correctif live

Version publiée : `e928e453-0ed6-446b-83a5-ab6acb02d92e`.

Le setting suivant est appliqué uniquement à `AG4_Spé-V2` :

```json
"saveDataSuccessExecution": "none"
```

La vérité métier et le détail utile restent persistés dans DuckDB
(`news_history`, `news_errors`, `run_log`). n8n conserve toujours le statut de
l'exécution, mais ne sérialise plus les centaines de mégaoctets de sorties HTML
à la fin d'un run réussi.

Le patch de déploiement a été construit depuis l'export publié live. Hors
`settings`, l'objet importé est strictement identique à l'export : 38 nodes,
35 connexions, IDs et code conservés. Le correctif dates du 13/07 dans
`S16 - Parse Article` a été explicitement vérifié après publication.

Miroir repo :

- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/AG4-SPE-V2-workflow.json`
- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/build_workflow.py`

## Vérifications après déploiement

- workflow `active=1`;
- `versionId == activeVersionId`;
- `/healthz` n8n : `ok`;
- runners reconnectés et tâches Python AG2 exécutées après redémarrage;
- broker authentifié et compte aligné;
- `dry_run=false` inchangé, Forex désactivé, aucune approbation en attente;
- aucun ordre et aucun garde d'exécution modifiés.

Le premier test complet à l'échelle réelle est le cron du 24/07 à 08:05
Europe/Paris. Le critère de validation est un statut n8n `success`, un run
DuckDB finalisé et l'absence de nouveau `FATAL ERROR ... heap out of memory`.

## Rollback

Export pré-correction durable :

```text
/opt/trader-ia/.codex-tmp/ag4_spe_v2_pre_heap_fix_20260724.json
```

Commandes :

```bash
docker cp /opt/trader-ia/.codex-tmp/ag4_spe_v2_pre_heap_fix_20260724.json root-n8n-1:/tmp/ag4_rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/ag4_rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/ag4_rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=H0cfY1coMx8dvMuXScMc_
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```
