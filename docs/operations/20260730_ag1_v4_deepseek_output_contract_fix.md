# AG1 V4 — contrat DeepSeek et identités modèles (2026-07-30)

## Incident validé

La dernière exécution planifiée avant correction, n8n `20603`
(`RUN_20260730_140009_20603`), s'est terminée en succès technique mais avec
seulement deux propositions de modèle valides sur trois.

La proposition DeepSeek contenait :

```text
OUTPUT_PARSING_FAILURE
Unexpected non-whitespace character after JSON at position 6274
```

Le node LangChain Agent convertissait le parseur structuré en appel d'outil.
DeepSeek a produit des arguments d'outil concaténés ou mal formés ; l'erreur
est donc survenue avant que le texte brut soit disponible pour l'extracteur.
L'extracteur considérait ensuite tout objet comme valide, y compris
`{error: ...}`, et inscrivait `extractorStatus=OK_OBJECT`. Le moteur de consensus
a correctement rejeté la proposition faute de tableau `actions`, mais le
workflow n8n est resté vert.

DuckDB confirme `validModelCount=2` et la proposition
`model_key='grok41_reasoning'` avec `parse_ok=false`. Cette clé est historique
et reste volontairement utilisée pour la compatibilité du ledger.

## Correction

- la branche `deepseek-v4-pro` utilise désormais
  `@n8n/n8n-nodes-langchain.chainLlm` 1.5 au lieu du node Agent ;
- le même prompt et le même JSON Schema restent branchés au parseur structuré ;
- retry du node DeepSeek : deux tentatives, attente 2 secondes ;
- contrat explicite : un seul objet JSON, six clés racine toujours présentes ;
- les trois Information Extractor valident la structure métier, normalisent
  les champs autorisés et distinguent `UPSTREAM_ERROR`, `INVALID_SHAPE`,
  `UNPARSED_TEXT` et les statuts `OK_*` ;
- le consensus n'accepte que les statuts `OK_*` avec un tableau `actions` et
  persiste la cause d'extraction réelle en cas d'échec.

## Modèles live et identités persistées

| Branche | Modèle réel | `model_key` DuckDB |
|---|---|---|
| GPT | `gpt-5.6-sol` | `chatgpt52` |
| DeepSeek | `deepseek-v4-pro` | `grok41_reasoning` |
| Claude | `claude-opus-4-8` | `claude_sonnet46` |

Les clés canoniques historiques ne sont pas renommées afin de ne pas casser
les vues, historiques et consommateurs existants. `model_name` et `model_id`
identifient sans ambiguïté le fournisseur et le modèle actuels.

## Validation avant publication

- audit de l'exécution n8n `20603` et des payloads Agent/Extractor/Consensus ;
- audit DuckDB en lecture seule des 18 propositions récentes ;
- six tests Python de contrat et d'idempotence du builder : succès ;
- cinq cas de replay extracteur dans le runtime Node n8n : succès ;
- smoke consensus → safety → bundle en dry-run, sans broker : succès pour
  un BUY et un SELL détenu ;
- import shadow dans un profil n8n isolé : `active=0`, sans cron, trois modèles
  exacts, chaîne/parser/retry DeepSeek reconnus ;
- import/publication/restart uniquement après confirmation de l'inactivité
  globale n8n ;
- export post-publication identique au candidat pour les nœuds, connexions,
  settings, static data et pin data ;
- broker après déploiement : authentifié, connecté, compte `U25651155`
  aligné, aucune approbation en attente.

## État live

```text
workflow : AG1V4CONSENSUS
version publiée : 62d1c796-5d4d-4fcb-8d84-1d3a34e786f5
active : 1
versionId = activeVersionId
```

Le premier run naturel des trois modèles après correction est celui de 16:30
Europe/Paris le 2026-07-30. Aucun run manuel n'est déclenché par Codex car le
workflow peut envoyer des ordres sur le compte réel.

## Rollback

```text
/tmp/ag1_v4_extractors_20260730/AG1V4.pre_extractor_fix.json
version 8f00d6d0-1e51-4fc5-b706-767af8fe3f9a
SHA256 DA5C13476E0B8F70811EA2980DBD1812A307A4229368F2331438819E4AC47F9A
```

Avant tout redémarrage, vérifier qu'aucune exécution n8n n'est active.

```bash
docker cp /tmp/ag1_v4_extractors_20260730/AG1V4.pre_extractor_fix.json root-n8n-1:/tmp/AG1V4.rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG1V4.rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG1V4.rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=AG1V4CONSENSUS
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```
