# Correctif AG2 runtime `base` indéfini — 2026-07-30

## Incident validé

Depuis le premier run post-déploiement du 2026-07-26 à 20:00 UTC, les workflows
AG2 Held+Core et Watchlist terminaient en faux succès n8n alors que tous les
symboles échouaient dans `Compute + Filter + Write` avec :

```text
NameError: name 'base' is not defined
```

Le nœud capturait l'exception par symbole et `Finalize Run` retournait un objet
`status=FAILED` sans lever d'erreur. Les exécutions n8n restaient donc vertes.
Dans `ag2_v3.duckdb.run_log`, les runs du 26 au 30 juillet étaient `FAILED`, avec
zéro ligne `technical_signals` pour ces runs. Au diagnostic, les 78 symboles du
scope Held+Core n'avaient aucune ligne de moins de 24 heures.

## Cause et correctif

La remédiation close-only du 2026-07-26 avait introduit deux lectures d'une
variable inexistante dans `nodes/04_compute.py` :

```python
exchange = base.get("exchange", "")
asset_class = base.get("asset_class", "EQUITY")
```

Elles lisent désormais le contexte courant déjà normalisé :

```python
exchange = str(d.get("exchange") or "")
asset_class = identity["asset_class"]
```

`Finalize Run` lève aussi `AG2_RUN_FAILED` lorsque tous les symboles échouent,
après avoir persisté le statut DuckDB. Une panne systémique apparaîtra donc en
rouge dans n8n au lieu d'un faux succès.

## Validation

- 10 tests unitaires/contrat passés ; ajout de tests qui exécutent le code
  complet de calcul et vérifient qu'un échec total devient une erreur n8n.
- Replay de l'entrée réelle `AI.PA` de l'exécution n8n `20585` sur une copie de
  `ag2_v3.duckdb` : `_status=ok`, écriture technique présente, identité
  `EQUITY` / `Euronext Paris` conservée.
- Diff pré-déploiement : seuls `Compute + Filter + Write` et `Finalize Run`
  changent ; crons, connexions, univers et paramètres restent identiques.

## Déploiement live

Déployé et publié le 2026-07-30 :

- Held+Core `AG2V3HELDCORE20260619` : version publiée
  `dd4e76d6-eec1-45be-a915-1bc70d25b48b` ;
- Watchlist `AG2V3WATCHNIGHT20260619` : version publiée
  `16f3e6c8-038a-4d8e-876a-d94d5fb88d13`.

Les deux workflows sont `active=1`, avec `versionId=activeVersionId`. n8n et les
trois task-runners ont été redémarrés et se sont reconnectés au task broker.
Aucun workflow AG1, garde IBKR ou ordre n'a été modifié.

## Rollback

Exports pré-correctif :

```text
/tmp/ag2_fix_20260730/AG2-Held-Core.pre_fix.json
/tmp/ag2_fix_20260730/AG2-Watchlist.pre_fix.json
```

Pour restaurer, importer l'export concerné, republier son ID, puis redémarrer
`root-n8n-1` et `root-task-runners-3/4/5`. Les versions pré-correctif étaient
respectivement `56ccdbb1-aa34-48d2-a600-9eb9a87b9d66` et
`ba27c29a-3dfc-4704-b59f-894c1ced1f37`.

## Vérification planifiée restante

Le premier run cron Held+Core post-déploiement est prévu à 13:00 Europe/Paris le
2026-07-30, avant le run AG1 de 14:00. Contrôler `run_log.status=SUCCESS`,
`symbols_ok>0`, `symbols_error=0` et la fraîcheur de `technical_signals`.
