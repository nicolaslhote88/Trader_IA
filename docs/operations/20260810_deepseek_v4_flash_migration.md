# Migration DeepSeek V4 Pro vers V4 Flash hors AG1 (2026-08-10)

## Décision

Les cinq workflows live hors AG1 qui utilisaient `deepseek-v4-pro` passent à
`deepseek-v4-flash`. AG1 V4 conserve explicitement `deepseek-v4-pro` dans son
consensus 2/3.

Périmètre :

- `AG2V3HELDCORE20260619` — Technical Held+Core ;
- `AG2V3WATCHNIGHT20260619` — Technical Watchlist Nightly ;
- `H0cfY1coMx8dvMuXScMc_` — AG4_Spé-V2 Boursorama ;
- `hSqxVSb8YAO9Nc6A` — AG4_Spé-IBKR-V1 Portfolio News ;
- `AG4SPEFINNHUBV1` — AG4_Spé-Finnhub-V1 Global News.

## Motif

La version publique courante de DeepSeek V4 Flash présente des performances
proches de Pro sur plusieurs benchmarks de raisonnement et de code, tout en
activant 13B paramètres par token contre 49B pour Pro. L'éditeur précise que
Flash Max est comparable à Pro avec un budget de réflexion supérieur, mais
reste légèrement derrière sur la connaissance pure et les workflows
agentiques les plus complexes. L'API expose officiellement l'identifiant
stable `deepseek-v4-flash`.

Sources consultées le 2026-08-10 :

- https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash ;
- https://api-docs.deepseek.com/api/list-models/ ;
- https://api-docs.deepseek.com/quick_start/pricing .

Les usages migrés sont des validations, classifications et extractions à
schéma structuré. Le raisonnement de portefeuille et la décision
multi-facteurs d'AG1 restent donc sur le modèle Pro.

## Blast radius

Seuls l'identifiant du sous-nœud `lmChatDeepSeek` et, pour AG2, les marqueurs
de cache/lineage `ai_model` passent de `deepseek-v4-pro` à
`deepseek-v4-flash`. Prompts, schémas structurés, crons, univers, écritures
DuckDB, gardes IBKR et connexions n8n restent inchangés.

## Validation

- 16 tests AG2 : succès ;
- 12 tests contractuels AG4_Spé : succès ;
- cinq imports dans un profil n8n shadow isolé : succès, `active=0`, aucun
  `activeVersionId`, modèle Flash reconnu ;
- exports live pré-déploiement comparés aux candidats : seuls les identifiants
  du modèle et les marqueurs cache/lineage AG2 changent ;
- aucune exécution n8n active avant import ;
- import, publication et redémarrage de `root-n8n-1` et des runners 3/4/5 :
  succès ;
- après redémarrage : n8n et les trois runners `Up`, trois runners Python et
  trois runners JavaScript enregistrés, aucune erreur de démarrage ;
- broker IBKR toujours authentifié et aligné sur `U25651155`, aucune
  approbation en attente.

## État live vérifié le 2026-08-10

| Workflow | Version publiée | Modèle |
|---|---|---|
| `AG2V3HELDCORE20260619` | `e9df14e6-cdfe-4894-96c0-021facc2aa07` | `deepseek-v4-flash` |
| `AG2V3WATCHNIGHT20260619` | `71c1899b-6a38-4312-9084-7439db4f88bc` | `deepseek-v4-flash` |
| `H0cfY1coMx8dvMuXScMc_` | `193ade18-1b82-426c-aefd-2f01ab5453b8` | `deepseek-v4-flash` |
| `hSqxVSb8YAO9Nc6A` | `a102a0e4-ff4d-49b2-a910-e34d4236c5af` | `deepseek-v4-flash` |
| `AG4SPEFINNHUBV1` | `458c95bb-09a4-4181-89cb-a6974fbddbad` | `deepseek-v4-flash` |
| `AG1V4CONSENSUS` | `fd0091f9-e52f-40d5-9f43-182c1615cd73` | `deepseek-v4-pro` inchangé |

Pour les six workflows, `active=1` et `versionId=activeVersionId`.

## Rollback

Les exports pré-déploiement sont conservés sur le VPS dans
`/tmp/deepseek_flash_20260810_predeploy/` et localement dans
`.codex-tmp/deepseek_flash_20260810_predeploy/` :

- `AG2V3HELDCORE20260619.pre_flash.json` ;
- `AG2V3WATCHNIGHT20260619.pre_flash.json` ;
- `AG4SPEV2.pre_flash.json` ;
- `AG4SPEIBKR.pre_flash.json` ;
- `AG4SPEFINNHUB.pre_flash.json`.

Pour chaque workflow concerné, importer l'export `*.pre_flash.json`, republier
le même identifiant, puis redémarrer `root-n8n-1` et les trois task-runners.
