# Audit AG2 — TXN détenu mais sans analyse IA

Vérifié le 5 octobre 2026 sur les exécutions n8n, les bases live et le code déployé. Audit en lecture seule, aucune modification de production. Horaires Europe/Paris sauf mention explicite.

## Conclusion

TXN est correctement classé `HELD` et traité par AG2. Les indicateurs sont calculés, mais aucun appel LLM AG2 n'est enregistré dans les 35 traitements depuis le 17 septembre. Le statut détenu garantit l'inclusion dans la rotation, pas une revue IA.

Deux mécanismes l'expliquent : le préfiltre écarte les signaux H1 neutres sans exception portefeuille ; les autres signaux sont écartés par une fenêtre H1 de trois heures qui ne tient pas compte des séances. Le créneau de 16:35 entre en conflit avec les dix minutes de marge exigées pour les bougies closes. Une anomalie D1 indépendante a également été reproduite.

## Faits validés

- `universe_segments` : TXN, segment `HELD`, actif, priorité 1000, source `auto`, raison `portfolio_position`.
- Dernier traitement TXN : **5 octobre à 09:03:45**, exécution Held+Core **22742**, terminée en succès.
- Run : `AG2V3_20261005070035237442_54`.
- H1 `BUY`, score 2 ; D1 `NEUTRAL`, score 0 ; statuts techniques H1/D1 `OK`.
- Dernière H1 : vendredi 2 octobre **19:30 UTC**, âge calculé **59,6 h**.
- Dernière D1 : vendredi 2 octobre **00:00 UTC**, âge calculé **79,1 h**.
- `filter_reason=H1_OR_D1_OUTSIDE_AI_FRESHNESS_WINDOW`, `pass_ai=false`, `call_ai=false`, `dedup_reason=FILTERED_OUT`.
- Aucun enregistrement TXN dans `ai_dedup_cache`.
- Le nœud cache écrit alors `SKIP`, qualité 0 et le message générique `[CACHE] No AI call (TTL/filtered) and no cache record.`
- Ce 0/10 est une valeur de repli, pas une note attribuée par un modèle à TXN.
- Workflow actif et publié : `e2e8ac8d-d205-4d7c-9260-3e330538a773`.

## Pourquoi cela dure plusieurs jours

Sur les 35 lignes TXN depuis le 17 septembre :

| Cause | Nombre | Conséquence |
|---|---:|---|
| H1 `NEUTRAL` | 21 | Pas d'appel, même pour un titre détenu |
| Hors fenêtre de fraîcheur IA | 14 | Pas d'appel malgré un signal qui passe le préfiltre directionnel |

Les 71 lignes TXN conservées dans la base, du 26 juin au 5 octobre, ne contiennent aucun `call_ai=true`. Cela décrit la rétention disponible, pas une preuve exhaustive de toute l'histoire du système.

Le préfiltre ne reçoit que les résultats H1/D1. Il ne dispose ni du statut détenu ni de l'ancienneté d'une position. Il n'existe donc pas ici de règle « revue périodique obligatoire d'une position détenue ».

La fenêtre IA H1 est de **3 heures en semaine de 07 à 18 h UTC**, indépendamment de la place. Avant l'ouverture américaine et après le week-end, la dernière bougie disponible peut être parfaitement normale pour cette séance tout en étant refusée pour l'IA. La limite dure de 96 heures permet de conserver les indicateurs pour AG1 ; elle ne garantit pas l'appel LLM AG2.

## Conflit entre 16:35 et la marge de clôture

Exécution **22709**, vendredi 2 octobre : requête TXN H1 à **16:37:51**.

- Réponse HTTP enregistrée : `droppedOpen=2`, `source=cache`, dernière bougie admissible jeudi 1er octobre à 19:30 UTC.
- Le service définit une H1 close à `début + 1 heure + 10 minutes`.
- La première H1 américaine débutant à 13:30 UTC n'est donc admissible qu'à **14:40 UTC / 16:40 Paris**.
- Le traitement de TXN à 16:37 intervient trop tôt. Le code retient la veille ; l'âge devient environ 19,1 heures.
- Ce vendredi, la cause immédiate persistée est `NEUTRAL`. Les autres jours où le signal passe le préfiltre, la même ancienneté déclenche le rejet de fraîcheur, notamment les 29 septembre, 30 septembre et 1er octobre.

Reproduction en mémoire à partir de l'AST du fichier live `/app/main.py` dans `yfinance-api` : `_bar_is_closed(13:30 UTC, 1h)` retourne `false` à 14:37:51 UTC et `true` à 14:40 UTC. Aucun endpoint de collecte, aucune écriture et aucun ordre.

## Anomalie supplémentaire : clôture D1 américaine

Dans la même réponse du 2 octobre à 16:37:56, la barre D1 du **2 octobre** est marquée `closed=true`, alors que la séance américaine est en cours.

Le code convertit son timestamp normalisé `2026-10-02T00:00:00Z` vers New York, obtient la date du **1er octobre**, puis compare à la clôture de cette mauvaise journée. La reproduction avec la fonction live confirme `true` prématurément.

Ce bug n'explique pas le SKIP de ce matin, mais remet en cause la certification `closedOnly` de certaines D1 américaines intrajournalières. Il doit être corrigé avant de faciliter l'admission aux analyses IA.

SHA-256 du `main.py` live inspecté : `7048fb4f02f79bc265545e9719a138d924e4de3e039c56487c9145c3cba93ef3`.

## Étendue et effet sur AG1

Contrôle depuis le 17 septembre sur les symboles actuellement classés HELD :

| Symbole | Traitements conservés | Appels IA AG2 |
|---|---:|---:|
| TXN | 35 | 0 |
| GOOGL | 37 | 0 |
| KO | 28 | 0 |
| UNP | 24 | 0 |

Cette fenêtre ne représente pas nécessairement toute la période de détention de chaque titre. D'autres titres détenus ont des appels IA, par exemple PRX.AS et ELIS.PA.

AG1 reçoit toujours les indicateurs et évalue les positions : dans le run AG1 22746, Claude/GPT ont proposé HOLD sur TXN et DeepSeek DECREASE. Le manque identifié concerne l'avis LLM intermédiaire d'AG2, pas une absence totale de revue par AG1. Le contrat actuel traite SKIP comme neutre pour la contribution IA AG2, pas comme un REJECT.

La capture affiche également « 293,81 € » alors que les données TXN sont en USD : la vue technique du dashboard ajoute un symbole euro en dur (`services/dashboard/app.py`, ligne 17810 à l'audit). C'est une erreur d'étiquette distincte de l'absence d'analyse IA.

## Actions restantes proposées

1. Corriger la détermination de la date de séance des D1 ; conserver la garantie de bougies réellement closes.
2. Aligner la fraîcheur H1 et l'ordonnancement sur la disponibilité effective des dernières bougies de chaque marché, en tenant compte de la marge de dix minutes.
3. Donner aux positions détenues une revue IA périodique explicite, y compris quand H1 est neutre, avec cache et budget bornés ; garder les contrôles de validité des données.
4. Afficher « non analysé » et la vraie cause du SKIP, plutôt qu'une qualité de repli 0/10 et un message ambigu TTL/filtre ; corriger l'unité monétaire.
5. Valider ces changements par replay isolé, avec contrôle de parité AG1/dashboard si les règles consommées changent, puis suivre la couverture IA réelle des détenus.

Aucun correctif n'a été déployé dans cet audit.

## Preuves

Dossier gitignoré `.codex-tmp/audit-ag2-txn-20261005/` :

- `raw.json` : exécutions 22742 et 22709 et leurs définitions enregistrées.
- `22742-txn.json`, `22709-txn.json` : chaîne TXN, réponses OHLCV, calculs, filtre et cache.
- `22742-Compute + Filter + Write.txt` : code réellement enregistré avec l'exécution.
- `held-coverage.txt` : appels IA sur la fenêtre retenue.
- `extract.py` : extraction reproductible à partir des données d'exécution.

Sources de code : `agents/trading-actions/AG2 - La technique/AG2-V3/nodes/04_compute.py` et `07_hydrate_ai_cache.py`, `services/yfinance-api/main.py` (`_bar_is_closed`).
