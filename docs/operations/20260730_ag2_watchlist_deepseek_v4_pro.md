# AG2 Watchlist — passage à DeepSeek v4 Pro (2026-07-30)

## Périmètre

`AG2-V3 — Technical Watchlist Nightly` (`AG2V3WATCHNIGHT20260619`) utilise
désormais `deepseek-v4-pro` pour la validation IA. Le cron, la rotation
WATCHLIST, le batch de 40 symboles, les indicateurs, le scoring et les gardes
AG1/IBKR sont inchangés. Aucun ordre n'a été créé ou confirmé.

## Architecture et traçabilité

La branche IA publiée est :

```text
Snapshot Context -> AI Validation DeepSeek -> Merge AI + Context
                         ^              ^
             DeepSeek Chat Model   Structured Output Parser
```

- modèle : `deepseek-v4-pro` ;
- credential : `DeepSeek account` (`BlSCC28mzKodkfO5`) ;
- chaîne : `@n8n/n8n-nodes-langchain.chainLlm` 1.5 ;
- modèle : `@n8n/n8n-nodes-langchain.lmChatDeepSeek` 1 ;
- parseur : `@n8n/n8n-nodes-langchain.outputParserStructured` 1.3.

Le prompt système, le prompt utilisateur et le JSON Schema sont inchangés par
rapport à GPT. Le hash de déduplication est namespacé par
`model=deepseek-v4-pro`, ce qui empêche de réutiliser une décision GPT en cache.
Les écritures de lineage utilisent `ai_model=deepseek-v4-pro`.

## Correctif d'intégration détecté avant le premier cron Watchlist

La chaîne n8n renvoie l'objet parsé sous `{output: <objet>}`. La première
version migrée savait appeler DeepSeek, mais l'ancien merge ne déballait pas
cette enveloppe et produisait un REJECT de sécurité `no_ai_json_found`.

Correction publiée sur Held+Core et Watchlist :

- `Merge AI + Context` extrait `raw.output` lorsqu'il s'agit d'un objet ;
- `Extract AI + Write` accepte aussi `ai_validation.output` et `ai_raw.output` ;
- replay de la forme live réussi avec conservation de `decision`, `validated`
  et `quality_score`.

## Validation et état live

- 10 tests unitaires/contrat : succès ;
- candidat importé dans un profil n8n isolé, inactif et sans cron : succès ;
- comparaison post-export : nœuds, connexions, settings, static data et pin data
  identiques au candidat ;
- workflow actif et publié :
  `dfb2df95-d06a-48cc-8499-4965be2aac00` ;
- aucun nœud GPT résiduel ; credential, modèle, parseur, namespace cache et
  unwrap vérifiés dans `workflow_history` actif ;
- n8n et les trois task-runners redémarrés ;
- broker IBKR authentifié et connecté sur `U25651155`, zéro approbation en
  attente.

Le premier cron Watchlist utilisant cette version reste à observer à 22:00
Europe/Paris le 2026-07-30.

## Rollback complet vers GPT

Export pré-migration :

```text
/tmp/ag2_watchlist_deepseek_20260730/AG2-Watchlist.pre_deepseek.json
version 16f3e6c8-038a-4d8e-876a-d94d5fb88d13
SHA256 6927768ADA173E352C035EB0E06E378D6A49D704C3319D571FAF51B51C5CD06C
```

```bash
docker cp /tmp/ag2_watchlist_deepseek_20260730/AG2-Watchlist.pre_deepseek.json root-n8n-1:/tmp/AG2-Watchlist.rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG2-Watchlist.rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG2-Watchlist.rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=AG2V3WATCHNIGHT20260619
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

Sauvegarde de la première version DeepSeek, antérieure au correctif d'enveloppe :

```text
/tmp/20260730_ag2_deepseek_output_unwrap/AG2-Watchlist.pre_output_unwrap.json
SHA256 E6BD24A603ECED467873F066127E7D423B68FDDB0163DD506BCB3A05C6C71DDA
```
