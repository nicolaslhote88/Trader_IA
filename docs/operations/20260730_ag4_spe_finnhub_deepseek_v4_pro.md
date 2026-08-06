# AG4_Spé-Finnhub-V1 — passage à DeepSeek v4 Pro (2026-07-30)

## Périmètre

Le workflow `AG4_Spé-Finnhub-V1 — Global News` (`AG4SPEFINNHUBV1`) utilise
désormais `deepseek-v4-pro` pour analyser les articles présents dans
`news_finnhub_staging`.

Le collecteur `/opt/trader-ia/finnhub/`, son token, son cron hôte, le mapping
ADR/OTC, la sélection des segments, le cron n8n, le staging et les écritures
dans `ag4_spe_v2.duckdb` sont inchangés. Les workflows Boursorama et IBKR ne
sont pas modifiés. Aucun ordre ni garde IBKR n'est concerné.

## Architecture

```text
Load staging -> S18 IF -> S19 Analyze with DeepSeek -> S19M Merge -> S20 Parse -> Write
                                      ^        ^
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

- export de la version publiée pré-bascule ;
- comparaison des prompts et du schéma par SHA-256 : identité exacte ;
- 8 tests AG4 Boursorama + Finnhub : succès ;
- builder Finnhub idempotent : succès ;
- replay dans le runtime Node n8n : enveloppes DeepSeek et OpenAI équivalentes
  pour `summary`, `impactScore` et `suggestedSignal` ;
- import shadow dans un profil n8n isolé : `active=0`, sans cron, trois nœuds
  DeepSeek reconnus ;
- blast radius : S19, S20, deux sous-nœuds LangChain et leurs connexions ;
- le run OpenAI `20601` de 13:00 Paris a atteint le statut terminal `canceled`
  après 39 min (243 articles arrivés à S18, aucun résultat S19, aucune erreur
  applicative enregistrée) ; aucun import ou redémarrage pendant son exécution ;
- import/publication/restart effectués seulement après confirmation de
  l'inactivité globale n8n ;
- export post-publication comparé au candidat : nœuds, connexions, settings,
  static data et pin data identiques.

## État live

```text
workflow : AG4SPEFINNHUBV1
version publiée : a03794a1-bab3-458f-afbd-16134cb85393
active : 1
versionId = activeVersionId
```

Le premier run DeepSeek planifié reste à contrôler à 16:00 Europe/Paris le
2026-07-30 via l'exécution n8n puis les lignes `source='finnhub'` dans
`news_history`. Les 243 articles du run OpenAI annulé sont restés
`PENDING` dans `news_finnhub_staging` et seront donc repris ; la cause de
l'annulation n'est pas présente dans les logs n8n et n'est pas attribuée au
déploiement (celui-ci a commencé après le statut terminal).

## Rollback

```text
/tmp/ag4_spe_finnhub_deepseek_20260730/AG4-SPE-FINNHUB.pre_deepseek.json
version bef58923-725e-4470-9fb7-ea8faf9eb33a
SHA256 FDC8DD02EE249CDFB22A5232287C0DB5E1A6E065E8E7C8EB155777A4DC39F253
```

Avant le redémarrage, vérifier qu'aucune exécution n8n n'est active.

```bash
docker cp /tmp/ag4_spe_finnhub_deepseek_20260730/AG4-SPE-FINNHUB.pre_deepseek.json root-n8n-1:/tmp/AG4-SPE-FINNHUB.rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG4-SPE-FINNHUB.rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG4-SPE-FINNHUB.rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=AG4SPEFINNHUBV1
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```
