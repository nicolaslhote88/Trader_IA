# AG2-V3 Held+Core — durcissement DuckDB

Date : 2026-06-24

## Diagnostic live

Workflow : `AG2-V3 — Technical Held+Core`
(`AG2V3HELDCORE20260619`).

Sur les 8 dernières exécutions :

- 2 timeouts de 60 s dans `DuckDB Init Schema` ;
- 1 conflit de verrou DuckDB dans `Hydrate AI from cache` ;
- les runs réussis duraient 22 à 32 minutes.

Cause : un `CHECKPOINT` était exécuté à la fermeture de plusieurs nœuds Python,
donc à chaque symbole. Chaque checkpoint prenait régulièrement 20 à 35 s et
allongeait la durée de possession du verrou. La base
`ag2_v3.duckdb` est également fragmentée (2,7 Go pour environ 12 260 signaux).

## Correctif

- suppression des checkpoints dans :
  - `DuckDB Init Schema` ;
  - `Compute + Filter + Write` ;
  - `Extract AI + Write` ;
  - `Hydrate AI from cache` ;
- checkpoint conservé une seule fois dans `Finalize Run` ;
- connexion DuckDB : 10 tentatives avec backoff plafonné à 1,5 s ;
- `Hydrate AI from cache` dégrade en `SKIP` avec
  `CACHE_DB_UNAVAILABLE` après épuisement des retries, au lieu d'arrêter tout le
  workflow ;
- au démarrage, les `run_log` `RUNNING`/NULL de plus de deux heures sont
  réconciliés en `STALE`; les nouveaux runs sont créés explicitement avec le
  statut `RUNNING` ;
- `build_split_workflows.py` synchronise désormais tous les fichiers de nœuds
  modifiés, et pas seulement l'initialisation.

## Validation shadow

Tests effectués sur une copie complète de la base live :

- `DuckDB Init Schema` : **9,4 s**, contre 24–62 s en production ;
- `Hydrate AI from cache` : **0,45 s**, contre 20–33 s ;
- compilation des cinq nœuds Python dans un wrapper n8n : OK ;
- aucune écriture dans la base métier pendant le replay.

## Déploiement

Seul `AG2V3HELDCORE20260619` est importé et publié. La variante Watchlist est
regénérée dans le dépôt mais n'est pas déployée dans cette intervention.

Après import :

```bash
n8n publish:workflow --id=AG2V3HELDCORE20260619
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

Vérifier `active=1` et `activeVersionId`, puis contrôler le prochain cron.

## Rollback

Réimporter l'export live sauvegardé avant intervention, republier le workflow,
puis redémarrer n8n et les quatre task-runners.
