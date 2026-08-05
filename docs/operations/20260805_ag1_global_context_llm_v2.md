# AG1 — Global Context LLM V2 compact

## Statut

Déployé sur le VPS le 2026-08-05 vers 21:53 Europe/Paris.

- service `global-context-synthesizer` : version `1.1.0`, healthy ;
- workflow AG1 actif : `AG1V4CONSENSUS`, version publiée
  `a5e85032-244d-4353-ba5f-5904a44695ab` ;
- shadow AG1 : inactif et non publié ;
- AG9 : inactif et non publié, conteneur shadow arrêté avec `restart=no` ;
- aucun run AG1 déclenché et aucun ordre envoyé pendant le déploiement.

## Problème validé

La sortie manuelle du nœud `AG1.GC — Fetch Advisory Pack` contenait 11 703
caractères. Elle répétait 21 lignes d'exposition inconnue (9 portefeuille et 12
opportunités), alors que le snapshot était globalement `DEGRADED`, de fraîcheur
`missing`, avec une couverture de `0,584444` et une confiance de `0,400186`.
Le replay du run shadow montrait que les trois LLM aval utilisaient surtout ce
bloc comme caveat, sans bénéfice décisionnel justifiant ce volume.

## Correction

Le contrat complet `AG1_GLOBAL_CONTEXT_PACK_V1` reste persisté dans
`global_context_v1.duckdb` pour l'audit et le dashboard. Seule la réponse de
`POST /ag1-pack` destinée aux LLM est compactée en :

- schéma `AG1_GLOBAL_CONTEXT_LLM_V2` ;
- méthode `GLOBAL_CONTEXT_LLM_COMPACTION_V2` ;
- budget maximal de 4 000 caractères ;
- devises limitées à celles du portefeuille et des opportunités ;
- expositions inconnues agrégées en compteurs ;
- détails filtrés si fraîcheur ou confiance insuffisante ;
- politique explicite `IGNORE`, `CAVEAT_ONLY`, `CAUTION` ou `NORMAL`.

Seuils live : confiance détail `0,5`, confiance globale `0,5`, couverture
globale `0,6`. Avec la sortie signalée, la politique est donc
`CAVEAT_ONLY` : aucun score de composant n'est transmis ou autorisé pour
choisir, dimensionner ou justifier une action.

Les prompts des trois branches AG1 appliquent explicitement la politique et le
sens contrarian du `positioning_score`. Le consensus, la safety, le broker, les
gates et le scoring matrice sont inchangés ; aucune modification dashboard
n'est requise.

## Validation

- 55 tests passés : AG1, synthétiseur, macro AG5–AG8, World Monitor, dashboard
  et replay ;
- empreintes inchangées :
  - consensus `c39434c3ff5b484ba2615fa6a0ec7c722387b790c3f83c630070645d611d1316` ;
  - safety `d658f005a41131e175792f5b5dea63e3445fb744f8979f347916dacc9722883d` ;
  - broker `060d649426d7ad015e68734fe1cda4909ecdf89503d1158e26d77f3a7e8b5e41` ;
- replay exact de la sortie utilisateur, d'abord en conteneur isolé sur copie
  DuckDB puis sur le service live : 11 703 → 1 299 caractères (`-88,9 %`) ;
- contrôles live : `CAVEAT_ONLY`, aucun `currency_signals`, limitation
  d'exposition présente une seule fois, compteurs 9/12 exacts ;
- `workflow_history` publié et `workflow_entity` vérifiés séparément ;
- broker après déploiement : authentifié, compte aligné, `dry_run=false`, FX
  désactivé, zéro approbation en attente ;
- AG5, AG6, AG7, AG8 et le synthétiseur n8n conservent leurs versions publiées
  antérieures.

## Artefacts VPS et rollback

Release :

```text
/opt/trader-ia/releases/ag5-ag8-global-context-llm-v2-20260805-1945
```

Backup :

```text
/opt/trader-ia/backups/global-context-llm-v2-20260805-1945
```

Rollback ciblé :

```bash
cp -a /opt/trader-ia/backups/global-context-llm-v2-20260805-1945/docker-compose.yml /docker/root/docker-compose.yml
cd /docker/root
docker compose build global-context-synthesizer
docker compose up -d --no-deps global-context-synthesizer

docker cp /opt/trader-ia/backups/global-context-llm-v2-20260805-1945/ag1-live.json root-n8n-1:/tmp/ag1-live-rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/ag1-live-rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/ag1-live-rollback.json
docker exec root-n8n-1 n8n publish:workflow --id=AG1V4CONSENSUS

docker cp /opt/trader-ia/backups/global-context-llm-v2-20260805-1945/ag1-shadow.json root-n8n-1:/tmp/ag1-shadow-rollback.json
docker exec -u root root-n8n-1 chmod 644 /tmp/ag1-shadow-rollback.json
docker exec root-n8n-1 n8n import:workflow --input=/tmp/ag1-shadow-rollback.json

docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

Ne pas publier le workflow shadow. La copie DuckDB du backup est conservée en
dernier recours ; elle ne doit pas être restaurée pendant que le service est en
ligne.
