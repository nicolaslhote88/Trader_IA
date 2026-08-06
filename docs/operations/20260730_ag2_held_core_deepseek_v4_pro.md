# AG2 Held+Core — passage à DeepSeek v4 Pro (2026-07-30)

## Périmètre

Cette opération initiale ne concernait que `AG2-V3 — Technical Held+Core`
(`AG2V3HELDCORE20260619`). Le workflow Watchlist a ensuite été migré séparément
vers le même modèle ; voir `20260730_ag2_watchlist_deepseek_v4_pro.md`. Aucun
scoring technique, cron, segment, garde AG1/IBKR ou ordre n'est modifié.

## Architecture retenue

Le nœud OpenAI autonome a été remplacé par la topologie LangChain n8n :

```text
Snapshot Context -> AI Validation DeepSeek -> Merge AI + Context
                         ^              ^
             DeepSeek Chat Model   Structured Output Parser
```

- modèle : `deepseek-v4-pro` ;
- credential n8n : `DeepSeek account` (`BlSCC28mzKodkfO5`) ;
- chaîne : `@n8n/n8n-nodes-langchain.chainLlm` 1.5 ;
- modèle : `@n8n/n8n-nodes-langchain.lmChatDeepSeek` 1 ;
- parseur : `@n8n/n8n-nodes-langchain.outputParserStructured` 1.3.

Le prompt système, le prompt utilisateur et le JSON Schema sont identiques à
ceux de l'ancien nœud GPT. La chaîne avec parseur renvoie
`{output: <objet structuré>}` : `Merge AI + Context` extrait maintenant cet
objet, et `Extract AI + Write` possède le même fallback imbriqué. Le traitement
déterministe aval reste inchangé.

## Cache et traçabilité

Le hash de déduplication Held+Core est namespacé avec
`model=deepseek-v4-pro`. Une décision GPT mise en cache avant la bascule ne peut
donc pas être réutilisée par la branche DeepSeek. Les nouvelles écritures
`ai_model` et `ai_output_ref` portent `deepseek-v4-pro`.

## Validation

- credential présent avec le type `deepSeekApi`, sans exposition de la clé ;
- modèle et credential déjà utilisés avec succès par AG1, exécution n8n `20584` ;
- prompts et schéma comparés à la version publiée pré-bascule : égalité exacte ;
- import du candidat dans un profil n8n isolé, inactif et sans cron : succès ;
- exécution manuelle `20594` : appel DeepSeek et parseur terminés ; elle a révélé
  que l'enveloppe `output` n'était pas déballée (`no_ai_json_found`) ;
- replay local de la forme live `{output: <objet>}` après correction : les champs
  `decision`, `validated` et `quality_score` sont transmis à l'extracteur ;
- builder déterministe et idempotent ;
- 10 tests unitaires/contrat passés avant publication ;
- vérification post-publication : topologie, credential, lineage et namespace
  cache présents dans `workflow_history` publié ; aucun nœud GPT résiduel.

## Déploiement live

Version publiée Held+Core après correction de l'enveloppe structurée :

```text
7048c3f8-e603-4a1a-ae7d-631934418266
```

État vérifié : `active=1`, `versionId=activeVersionId`. n8n et les trois
task-runners ont été redémarrés. Le workflow Watchlist est également publié
avec DeepSeek sur `dfb2df95-d06a-48cc-8499-4965be2aac00`.

Sauvegarde immédiatement antérieure à la correction d'enveloppe :

```text
/tmp/20260730_ag2_deepseek_output_unwrap/AG2-Held.pre_output_unwrap.json
SHA256 DCDED65A5AE804B3F5E8F8AA8F8D7CCAD8E0F9BD0BB12FA985310A3FB97F99D5
```

### Tests manuels interrompus pendant le déploiement

Les exécutions manuelles `20594` (10:00 UTC) et `20595` (10:14 UTC) ne sont
pas des tests valides de la version finale. Elles étaient encore actives lors
des redémarrages n8n nécessaires aux publications successives. Pour `20595`,
la stack enregistrée est explicitement :
`ShutdownService -> TaskBrokerServer.stop -> stopConnectedRunners`, après
réception de `SIGTERM`; six symboles avaient été traités correctement avant
l'arrêt. La version finale `7048c3f8-e603-4a1a-ae7d-631934418266` a été publiée
après le démarrage de ces deux exécutions. Aucun correctif de `Wrap H1` n'est
donc requis sur la base de ces statuts.

## Rollback

Export live pré-bascule :

```text
/tmp/ag2_deepseek_20260730/AG2-Held-Core.pre_deepseek.json
```

Version pré-bascule : `dd4e76d6-eec1-45be-a915-1bc70d25b48b`.

```bash
docker cp /tmp/ag2_deepseek_20260730/AG2-Held-Core.pre_deepseek.json root-n8n-1:/tmp/AG2-Held-Core.rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG2-Held-Core.rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG2-Held-Core.rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=AG2V3HELDCORE20260619
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

## Vérification planifiée restante

Contrôler le premier cron Held+Core post-bascule : statut n8n, statut
`run_log`, `symbols_error=0`, nombre d'appels IA et `ai_model=deepseek-v4-pro`
sur les lignes effectivement validées par le modèle.
