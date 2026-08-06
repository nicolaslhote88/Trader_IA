# AG4_Spé-IBKR-V1 — passage à DeepSeek v4 Pro (2026-07-30)

## Périmètre

Le workflow `AG4_Spé-IBKR-V1 — Portfolio News` (`hSqxVSb8YAO9Nc6A`)
utilise désormais `deepseek-v4-pro` pour analyser les news des positions
détenues remontées par le broker.

L'endpoint `GET /news/portfolio`, le cron n8n, la sélection des positions, la
déduplication, les champs `provider` / `news_article_id` / `ibkr_sentiment` et
les écritures dans `ag4_spe_v2.duckdb` sont inchangés. Aucun ordre, aucune
approbation et aucun garde d'exécution IBKR ne sont concernés.

## Architecture

```text
/news/portfolio -> Normalize/Dedup -> S18 IF -> S19 Analyze with DeepSeek -> Merge -> S20 Parse -> DuckDB
                                                   ^             ^
                                         DeepSeek Model   Structured Parser
```

- modèle : `deepseek-v4-pro` ;
- credential : `DeepSeek account` (`BlSCC28mzKodkfO5`) ;
- chaîne : `@n8n/n8n-nodes-langchain.chainLlm` 1.5 ;
- modèle : `@n8n/n8n-nodes-langchain.lmChatDeepSeek` 1 ;
- parseur : `@n8n/n8n-nodes-langchain.outputParserStructured` 1.3.

Le prompt système, le prompt utilisateur et le JSON Schema sont strictement
identiques à la version GPT. `S20` traite la sortie structurée
`{output: <objet>}` et conserve le fallback OpenAI historique.

## Validation

- les 10 dernières exécutions IBKR avant migration étaient en succès ;
- export de la version publiée pré-bascule et comparaison au miroir repo :
  nœuds, connexions, settings, static data et pin data identiques ;
- comparaison des prompts et du schéma par SHA-256 : identité exacte ;
- 12 tests contractuels AG4 Boursorama + Finnhub + IBKR : succès ;
- builder IBKR idempotent : succès ;
- replay dans le runtime Node n8n : enveloppes DeepSeek et OpenAI équivalentes,
  avec conservation de `source`, `provider`, `newsArticleId` et
  `ibkrSentiment` ;
- import shadow dans un profil n8n isolé : `active=0`, sans cron, trois nœuds
  DeepSeek reconnus ;
- blast radius : S19, S20, deux sous-nœuds LangChain et leurs connexions ;
- import/publication/restart effectués seulement après confirmation de
  l'inactivité globale n8n ;
- export post-publication comparé au candidat : nœuds, connexions, settings,
  static data et pin data identiques ;
- après déploiement : broker authentifié et connecté, compte aligné
  `U25651155`, aucune approbation en attente.

## État live

```text
workflow : hSqxVSb8YAO9Nc6A
version publiée : 66575c3b-be4f-4385-800c-ee921404aa78
active : 1
versionId = activeVersionId
```

Le premier run IBKR DeepSeek planifié reste à contrôler à 16:00
Europe/Paris le 2026-07-30 via l'exécution n8n puis les lignes
`source='ibkr'` dans `news_history`.

## Rollback

```text
/tmp/ag4_spe_ibkr_deepseek_20260730/AG4-SPE-IBKR.pre_deepseek.json
version ee732412-d0e9-4004-9b3c-8360240ab246
SHA256 A8DD5A1CF7B080BEB91CE79F557CE975340FE87127AFA5B22C72AB16C9100A31
```

Avant le redémarrage, vérifier qu'aucune exécution n8n n'est active.

```bash
docker cp /tmp/ag4_spe_ibkr_deepseek_20260730/AG4-SPE-IBKR.pre_deepseek.json root-n8n-1:/tmp/AG4-SPE-IBKR.rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG4-SPE-IBKR.rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG4-SPE-IBKR.rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=hSqxVSb8YAO9Nc6A
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```
