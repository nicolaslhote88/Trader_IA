# AG2 — correction du faux `STALE` H1 et restauration des pré-tradables

Date de déploiement live : **2026-08-06**.

## Symptôme

Après plusieurs runs manuels AG2 réussis, le dashboard restait proche de 20
pré-tradables. Les runs avançaient pourtant correctement leurs curseurs et
écrivaient sans erreur dans `technical_signals`.

## Cause racine validée

`04_compute.py::check_freshness` appliquait une règle globale indépendante de
la place : en semaine entre 07:00 et 18:00 UTC, toute H1 âgée de plus de 3 h
devenait `STALE`.

Les derniers runs observés contenaient notamment :

- Euronext : dernière barre close vers 07:00 UTC, âge 4–5 h ;
- États-Unis : dernière barre de la séance précédente vers 19:30 UTC, âge
  15–16 h avant l'ouverture américaine ;
- D1 : `status=OK`, âge environ 35 h.

Ces âges étaient sous le contrat dur H1/D1 ≤96 h de R8 et du dashboard, mais
le statut `STALE` écrasait le résultat de calcul. Sur les 563 derniers signaux,
seuls 20 combinaient alors barres closes et statuts H1/D1 `OK` ; 148 autres
avaient des barres closes valides mais un H1 faussement `STALE`.

## Correction

La fraîcheur nécessaire à un nouvel appel LLM est désormais distincte de
l'utilisabilité des indicateurs :

- âge H1 > fenêtre courte AG2, mais ≤96 h : `SOFT_STALE`, pas de nouvel appel
  LLM, `h1_status` reste `OK` ;
- âge effectif H1 ou D1 >96 h : statut dur `STALE` et blocage inchangé ;
- `closed_only`, validation OHLCV, `REJECT` LLM, quote et liquidité restent
  inchangés.

Workflows publiés :

- Held+Core `AG2V3HELDCORE20260619` : version
  `23e41df0-c71a-45e5-81f6-043165731a89` ;
- Watchlist `AG2V3WATCHNIGHT20260619` : version
  `d1878ce9-ed22-4c6d-b720-efad1c054885`.

Les deux workflows sont `active=1` et leur `versionId` égale leur
`activeVersionId`.

## Réparation des dernières lignes

Script : `outils/scripts/repair_ag2_soft_stale_status.py`.

Le script est dry-run par défaut. En mode `--apply`, il cible uniquement la
dernière ligne par symbole lorsque les deux barres sont certifiées closes, D1
est `OK`, H1 est `STALE` et les âges effectifs H1/D1 restent ≤96 h.

Résultat live :

- 165 lignes réparées ;
- zéro cible restante parmi les dernières lignes ;
- backup logique :
  `maintenance_ag2_soft_stale_status_backup_20260806` (165 lignes) ;
- backup physique :
  `/local-files/duckdb/ag2_v3.duckdb.bak_20260806_pre_soft_stale_fix`.

## Validation

- 16 tests AG2 passants, dont seuils 3 h/96 h et apply/rollback du réparateur ;
- builders Held+Core/Watchlist idempotents ;
- code publié vérifié dans `workflow_history` (`SOFT_STALE` + limite 96 h) ;
- dernier run manuel antérieur au déploiement : 40/40, zéro erreur, finalisé
  avant import ;
- dashboard redémarré et health Streamlit `ok` ;
- funnel recalculé en lecture seule après réparation :
  361 symboles dans le scope entrée, 184 techniques prêtes, 163
  pré-tradables, 19 bloqués quote/liquidité et 2 bloqués par `AG2 LLM REJECT` ;
- broker authentifié et zéro approbation en attente ; aucun ordre déclenché.

## Rollback

Restaurer les statuts :

```bash
docker exec yf-enrichment python /tmp/repair_ag2_soft_stale_status.py --rollback
```

Restaurer les workflows sauvegardés dans
`/tmp/ag2_freshness_20260806/{held,watch}.json`, puis exécuter pour chacun
`n8n import:workflow`, `n8n publish:workflow` et redémarrer n8n ainsi que les
trois task-runners. Le backup physique DuckDB est une sauvegarde de dernier
recours à restaurer uniquement hors écriture.

## Observation restante

Contrôler au prochain run naturel qu'une H1 comprise entre la fenêtre courte
et 96 h est écrite avec `h1_status=OK`, le flag
`H1_OUTSIDE_AI_FRESHNESS_WINDOW` et sans appel LLM. Cette observation ne bloque
pas la restauration déjà validée du funnel.
