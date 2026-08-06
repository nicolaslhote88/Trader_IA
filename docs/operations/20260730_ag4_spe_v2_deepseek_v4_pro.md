# AG4_Spé-V2 — passage à DeepSeek v4 Pro (2026-07-30)

## Périmètre

Le workflow Boursorama `AG4_Spé-V2` (`H0cfY1coMx8dvMuXScMc_`) utilise désormais
`deepseek-v4-pro` pour l'analyse single-stock. Les workflows
`AG4_Spé-IBKR-V1` et `AG4_Spé-Finnhub-V1` ne sont pas modifiés.

Sont également inchangés : cron, rotation, scraping, filtre d'âge, prompt
système, prompt utilisateur, JSON Schema à 12 champs, normalisation S20 et
écritures dans `/files/duckdb/ag4_spe_v2.duckdb`. Aucun ordre ni garde IBKR
n'est concerné.

## Architecture publiée

```text
S18 IF Run AI? -> S19 Analyze with DeepSeek -> S19M Merge -> S20 Parse
                              ^        ^
                  DeepSeek Model   Structured Parser
```

- modèle : `deepseek-v4-pro` ;
- credential : `DeepSeek account` (`BlSCC28mzKodkfO5`) ;
- chaîne : `@n8n/n8n-nodes-langchain.chainLlm` 1.5 ;
- modèle : `@n8n/n8n-nodes-langchain.lmChatDeepSeek` 1 ;
- parseur : `@n8n/n8n-nodes-langchain.outputParserStructured` 1.3.

`S20 - Parse LLM Output` traite directement la sortie structurée
`{output: <objet>}`. L'ancienne enveloppe OpenAI
`output[0].content[0].text` reste acceptée pour le rollback.

## Validation

- workflow live pré-bascule actif, sans exécution n8n en cours ;
- prompts et schéma comparés à la version pré-bascule par SHA-256 : identité
  exacte ;
- 4 tests de contrat : succès ;
- builder idempotent : succès ;
- replay dans le runtime Node n8n : enveloppes DeepSeek et OpenAI produisent
  les mêmes `summary`, `impactScore` et `suggestedSignal` ;
- import shadow dans un profil n8n isolé : `active=0`, aucun cron, trois nœuds
  DeepSeek reconnus ;
- contrôle d'inactivité global avant redémarrage : aucune exécution n8n active ;
- export post-publication identique au candidat pour les nœuds, connexions,
  settings, static data et pin data ;
- broker IBKR authentifié/attaché à `U25651155`, zéro approbation en attente.

## État live

```text
workflow : H0cfY1coMx8dvMuXScMc_
version publiée : a821068f-4a28-435f-93d4-9ee8dc4e69c4
active : 1
versionId = activeVersionId
```

Le dernier run pré-bascule DuckDB
`AG4SPEV2_20260730090511` est `SUCCESS` (60 articles analysés, zéro erreur).
Le premier run DeepSeek planifié reste à contrôler à 14:05 Europe/Paris le
2026-07-30. Les exécutions réussies n8n ne sont pas conservées pour ce workflow
(`saveDataSuccessExecution=none`) ; la validation métier doit donc lire
`run_log` et `news_history` dans DuckDB.

## Rollback

Export pré-bascule :

```text
/tmp/ag4_spe_v2_deepseek_20260730/AG4-SPE-V2.pre_deepseek.json
version e928e453-0ed6-446b-83a5-ab6acb02d92e
SHA256 09BAA16CF37708280AEE6B6E2EF55583445068D65A181573DDA99ADBE71B1831
```

Avant tout redémarrage, vérifier qu'aucune exécution n8n n'est active.

```bash
docker cp /tmp/ag4_spe_v2_deepseek_20260730/AG4-SPE-V2.pre_deepseek.json root-n8n-1:/tmp/AG4-SPE-V2.rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG4-SPE-V2.rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG4-SPE-V2.rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=H0cfY1coMx8dvMuXScMc_
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```
