# AG2 — correction de la rotation des lots (2026-08-06)

## Faits validés avant correction

- Les exécutions n8n AG2 étaient vertes, mais les curseurs DuckDB ne bougeaient plus :
  - `last_index_actions_held_core=36`, dernière mise à jour le 2026-07-24 ;
  - `last_index_actions_watchlist=280`, dernière mise à jour le 2026-07-26.
- Held+Core répétait le lot démarrant à 36 ; Watchlist répétait la fin de rotation
  démarrant à 280. Cela expliquait une part importante des données H1/D1 périmées
  consommées par AG1.
- Cause : `DuckDB Init Schema` produisait bien `batch_info`, mais
  `Compute + Filter + Write` le supprimait. À la sortie de `Loop Symbols`,
  `Finalize Run` n'avait donc plus le contexte nécessaire pour écrire `batch_state`.
  L'ancien code terminait néanmoins le run en `SUCCESS`.

## Correction publiée

- `Compute + Filter + Write` préserve désormais `batch_info` sur les sorties
  normales et en erreur.
- `Finalize Run` valide le contrat du lot, écrit le curseur dans la même transaction,
  le relit, puis expose `cursor_advanced` et le prochain index.
- Un lot complet `SUCCESS` ou `PARTIAL` avance. Un échec total n'avance pas.
- Une absence de contexte, une taille incohérente ou un curseur non persisté lève
  `AG2_CURSOR_GUARD_FAILED` au lieu de produire un faux succès.
- Les crons, connexions, modèles, prompts, seuils, univers et garde-fous de trading
  n'ont pas changé.

Workflows live :

- Held+Core `AG2V3HELDCORE20260619` : version publiée
  `fdd97775-7665-4f31-bebc-76c055bb7275` ;
- Watchlist `AG2V3WATCHNIGHT20260619` : version publiée
  `dc8717e1-7c51-461c-b14e-677f26449fea`.

## Validation

- 13 tests unitaires/contrat : OK.
- Builder exécuté deux fois avec sorties identiques.
- Diff live → candidat : exactement deux nœuds modifiés dans chaque workflow,
  `Compute + Filter + Write` et `Finalize Run`. Les 21 nœuds, connexions et
  réglages restent sinon identiques.
- Replay shadow sur copies de `ag2_v3.duckdb` avec les vrais lots du 2026-08-06 :
  - exécution Held+Core `20795` : 27/27, curseur vérifié `36 → 54` ;
  - exécution Watchlist `20784` : 5/5, curseur vérifié `280 → 0`.
- Après publication : `active=1`, `versionId=activeVersionId`, contenu des nœuds,
  connexions et réglages identiques aux candidats ; n8n `/healthz` = `ok`.
- Broker après déploiement : authentifié et aligné sur le compte live ; aucune
  approbation en attente. Aucun ordre n'a été soumis ou confirmé.

## Réamorçage des curseurs

La base a été sauvegardée avant toute écriture :

- `/local-files/.codex-tmp/ag2_rotation_fix_20260806/ag2_v3.pre_fix.duckdb`
- SHA-256 original et copie :
  `a87c05fe55f82f5062de24770512b79897bbe73f13995c8ffebbd7e71ca8c238`

Le script dédié `outils/scripts/repair_ag2_batch_cursors.py` a appliqué sous
transaction, avec préconditions sur les anciennes valeurs :

- Held+Core `36 → 0` ;
- Watchlist `280 → 0`.

La relecture post-transaction confirme `0/0`. Le prochain run repart donc du
début de chaque rotation au lieu de retraiter la queue figée.

## Sauvegardes et rollback

Exports n8n pré-déploiement :

- `/local-files/.codex-tmp/ag2_rotation_fix_20260806/rollback_held_core.export.json`
- `/local-files/.codex-tmp/ag2_rotation_fix_20260806/rollback_watchlist.export.json`

Rollback des workflows :

```bash
docker exec root-n8n-1 n8n import:workflow --input=/files/.codex-tmp/ag2_rotation_fix_20260806/rollback_held_core.export.json
docker exec root-n8n-1 n8n import:workflow --input=/files/.codex-tmp/ag2_rotation_fix_20260806/rollback_watchlist.export.json
docker exec root-n8n-1 n8n publish:workflow --id=AG2V3HELDCORE20260619
docker exec root-n8n-1 n8n publish:workflow --id=AG2V3WATCHNIGHT20260619
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

Ne pas restaurer automatiquement toute la sauvegarde DuckDB après de nouveaux
runs : cela supprimerait les écritures intervenues depuis le 2026-08-06.

## Validation post-déploiement

Le premier run manuel Held+Core post-correction (`execution 20812`) s'est
terminé en succès le 2026-08-06 : 27/27 symboles, `batch_start=0`, curseur
persisté `0 → 18`. À cette lecture, 538 des 563 derniers signaux avaient des
âges H1/D1 stockés ≤96 h.

Le premier run Watchlist post-publication reste programmé à 22:00 Paris. Son
chemin exact a déjà été validé en replay sur les cinq éléments live de
`execution 20784`, avec conservation de `batch_info` et curseur `280 → 0`.
