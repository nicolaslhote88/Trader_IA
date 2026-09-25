# AG1 V4 — Opus 5.5 et GPT-6 Sol (2026-09-25)

## Faits validés

Déployé sur `AG1V4CONSENSUS`, actif, version publiée
`b47a3f8c-8df9-44fa-94d3-3925ffd115b9` (`versionId == activeVersionId`).

- OpenAI : `gpt-6-sol`, API Responses explicite, effort `medium` conservé.
- Claude : `claude-opus-5-5`, thinking `adaptive`, effort `medium`, plafond total
  de sortie 16 384 tokens. Le nœud Anthropic de n8n 2.3.5 envoie sinon
  `disabled` ou `enabled/budget_tokens`, tous deux incompatibles avec Opus 5.5.
- DeepSeek reste `deepseek-v4-pro`. Les clés de stockage `chatgpt52`,
  `claude_sonnet46`, `grok41_reasoning` sont conservées ; noms et IDs réels sont
  actualisés dans les extracteurs.

## Correction ciblée

La branche Claude utilise maintenant :

1. `Agent #1 - Portfolio manager2` (Code) : prépare les mêmes prompts métier,
   le modèle et les options API ; ajoute le schéma métier complet au prompt.
2. `Anthropic Messages - Opus5.5` (HTTP Request) : appelle `/v1/messages` avec
   le credential Anthropic existant, deux tentatives au maximum.
3. `AG1.V4 — Structured Output Claude` (Code) : lit uniquement les blocs texte,
   exige le modèle attendu et `stop_reason=end_turn`, parse un unique JSON et
   valide types, clés obligatoires, enums et bornes métier.
4. `Information Extractor2` : contrat de proposition et identité du modèle.

Le schéma envoyé à `output_config.format` retire les contraintes numériques,
longueurs et tailles de tableaux non supportées par Anthropic. Ces contraintes
restent intégralement vérifiées localement avant le consensus. Une réponse
tronquée, refusée, invalide ou issue d'un autre modèle devient une erreur de
branche (`UPSTREAM_ERROR`), jamais une proposition valide.

Six nœuds modifiés ; aucun nœud ajouté/supprimé. Aucun changement de prompt
métier hors identité Claude et ajout explicite du schéma. Aucune modification
des crons, gates, gardes broker, credentials ou règles de consensus.
Aucune mise à niveau globale de n8n ; les 179 autres workflows sont inchangés.

## Validation

- Replay du contexte préflight du run planifié `22435` (2026-09-24), dans un
  conteneur n8n 2.3.5 isolé, avec les deux credentials existants chiffrés.
  Graphe limité aux appels LLM et extracteurs : aucun cron, broker ou writer.
- GPT-6 Sol : `OK_OBJECT`, 8 actions proposées dans le replay final.
- Opus 5.5 : modèle retourné `claude-opus-5-5`, `end_turn`, `OK_OBJECT`,
  7 actions proposées ; 63 457 tokens entrée et 2 708 sortie, dont 1 071 thinking.
- Replay consensus puis safety : deux propositions nouvelles et proposition
  historique DeepSeek acceptées ; aucune exécution du nœud d'envoi d'ordres.
- Six tests Python du contrat/builder (dont idempotence et template) et quatre
  contrôles de contexte partagé/gardes broker réussis (ces derniers exécutés
  directement, pytest étant absent du Python local).
- Dix tests du décodeur Claude et cinq tests des extracteurs réussis dans le
  runtime Node n8n. Compilation Python et `git diff --check` réussis.
- Import/publication à n8n inactif, puis redémarrage n8n + trois runners.
  Les six runners JS/Python se sont réenregistrés.
- Graphe publié strictement identique au candidat validé.
- Avant/après : comptes et empreintes `bit_xor(hash(row))` inchangés pour
  `core.runs` (721), `orders` (164), `fills` (115), `positions_snapshot` (7 111),
  `portfolio_snapshot` (721). Broker authentifié et compte aligné ; zéro
  approbation en attente.

## Observation restante

Le prochain run planifié AG1 à 17:10 Paris n'a pas été observé pendant cette
intervention. Aucun run manuel du workflow de trading live n'a été déclenché.
Le replay valide la compatibilité technique, pas la performance financière
future des modèles.

## Sauvegarde et retour arrière

Sauvegarde locale : `.codex-tmp/models_20260925/published.json` (et `draft.json`).
Copie serveur et preuves : `/tmp/ag1_models_20260925/`.
Ancienne version : `d7cd1502-2711-4283-80cc-249077694da3`.
Le conteneur de replay et ses copies de credentials/configuration sont supprimés
après validation ; seules les preuves sans clés et les workflows sont conservés.

Après vérification de l'absence d'exécution n8n active :

```bash
docker cp /tmp/ag1_models_20260925/published.json root-n8n-1:/tmp/AG1V4.models.rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/AG1V4.models.rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/AG1V4.models.rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=AG1V4CONSENSUS
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

Vérifier ensuite `active=1`, la version publiée, l'identité des anciens modèles
et la santé des services. Ne pas rejouer le workflow live pour tester.

## Références

- [Migration Opus 5.5](https://platform.claude.com/docs/en/models/opus-5-5/migration-guide)
- [Sorties structurées Anthropic](https://platform.claude.com/docs/en/build-with-claude/structured-outputs)
- [GPT-6 Sol](https://developers.openai.com/api/docs/models/gpt-6-sol)
