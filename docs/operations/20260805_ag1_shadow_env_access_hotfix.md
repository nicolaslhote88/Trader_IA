# Hotfix n8n `$env` — AG1 Global Context Shadow

Date : 2026-08-05
Signalement : test manuel du workflow `AG1 V4 — Global Context Shadow (NO BROKER)`.

## Symptôme et diagnostic

Le nœud `AG1.GC — Fetch Advisory Pack` affichait dans l'éditeur n8n :

```text
[ERROR: access to env vars denied]
```

L'URL était construite par une expression lisant
`$env.GLOBAL_CONTEXT_SYNTHESIZER_URL`, avec une valeur Docker de repli.

Audit de l'exécution `20776` (`RUN_20260805_202803_20776`) dans la SQLite n8n :

- statut terminal n8n : `success` ;
- début : `2026-08-05 18:27:57.526Z` ;
- fin : `2026-08-05 18:32:13.830Z` ;
- 28 enregistrements d'exécution de nœud, tous `success`, sans clé `error` ;
- dernier nœud : `AG1.GC — Shadow Capture (NO BROKER)`.

Le défaut était donc une erreur réelle d'évaluation/inspection de l'expression
dans l'éditeur n8n, même si le moteur serveur avait produit une réponse et mené
ce run précis à son terme. Le contrôle de déploiement initial avait vérifié
l'appel HTTP et la version publiée, mais pas l'affichage manuel de cette
expression dans l'éditeur.

## Correction

Les URLs internes de ce lot sont désormais des valeurs Docker explicites, sans
accès `$env` dans les paramètres des nœuds HTTP :

- AG1 live et shadow :
  `http://global-context-synthesizer:8083/ag1-pack` ;
- AG5–AG8 : `http://macro-data-api:8081/...` ;
- synthèse : `http://global-context-synthesizer:8083/synthesize`.

La correction est appliquée dans les builders et dans tous les JSON générés.
Des tests interdisent désormais la réintroduction de `$env` dans ces URLs.

AG9 n'a pas été modifié : il reste inactif, non publié et son conteneur shadow
reste arrêté avec `restart=no`. Son URL devra être rendue statique avant une
éventuelle sortie de sommeil.

## Déploiement live

Release :
`/opt/trader-ia/releases/ag5-ag8-global-context-env-hotfix-20260805T184850Z`

Backup :
`/opt/trader-ia/backups/ag5-ag8-global-context-env-hotfix-20260805T184850Z`

Versions publiées :

| Workflow | `activeVersionId` |
|---|---|
| AG5 | `20c83708-e413-4ef1-a6e1-b77f9ef7a677` |
| AG6 | `fd98ba2a-37fc-4b0a-bfba-f385b5e00949` |
| AG7 | `c828e2ff-fcc9-4ab7-a436-f26f4e50fc39` |
| AG8 | `e157c4e4-af7a-4be1-b8b6-fdcf4096409b` |
| Synthèse | `75984a17-aac8-4ac9-be35-5667c649ee3f` |
| AG1 V4 live | `1daf05b8-95d9-4b4d-8fa1-a8eaa702437d` |

Le shadow a été réimporté corrigé, mais reste `active=0` avec
`activeVersionId=null`.

## Validations

- 53 tests locaux passants ;
- contenu des versions réellement publiées relu dans `workflow_history` ;
- `POST /ag1-pack` depuis le réseau Docker : `advisory_only=true`, snapshot
  `GC_20260805T181539Z_dd05f66b` ;
- n8n et les trois task-runners actifs après redémarrage ;
- AG9 inactif/non publié ; Forex désactivé ;
- broker authentifié, `dry_run=false`, zéro approbation en attente ;
- aucun run AG1 et aucun ordre déclenché par ce hotfix.

Hashes critiques inchangés :

```text
consensus  c39434c3ff5b484ba2615fa6a0ec7c722387b790c3f83c630070645d611d1316
safety     d658f005a41131e175792f5b5dea63e3445fb744f8979f347916dacc9722883d
broker     060d649426d7ad015e68734fe1cda4909ecdf89503d1158e26d77f3a7e8b5e41
```

## Rollback

1. importer les exports `*.before.json` du répertoire de backup ;
2. republier AG5–AG8, la synthèse et `AG1V4CONSENSUS` ;
3. réimporter le shadow sans le publier ;
4. redémarrer n8n et les trois task-runners ;
5. vérifier les états AG9, Forex, broker et approbations.

La sauvegarde SQLite n8n cohérente se trouve dans le même répertoire, mais ne
doit être restaurée qu'en cas de corruption démontrée.
