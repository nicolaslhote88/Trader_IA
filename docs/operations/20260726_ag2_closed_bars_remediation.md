# AG2 v3.1 — barres closes, fiabilité et traçabilité (2026-07-26)

## Objet

Remédiation de l'audit AG2 avec décision métier **close-only**. Aucun garde
d'exécution IBKR n'est modifié et aucun ordre n'est placé par cette opération.

## État live capturé avant modification

Versions n8n publiées immuables :

- Held+Core `be429ab8-455a-4bb9-bb7c-c34f0154a04d` ;
- Watchlist `88a0dab8-46cb-47f9-a2a1-d173ab34d24f` ;
- Universe Health `42a4b57d-ce62-4bc4-8022-22731ad4dba0`.

Les exports `published` et `entity` ont été sauvegardés localement sous
`.codex-tmp/ag2_live_20260726/`. Le code live yfinance, le dashboard et
l'exécutable AG1 publié ont été comparés au repo avant patch. Les workflows AG2
publiés ont été récupérés dans le repo avant reconstruction depuis les nœuds.

## Changements

- `/history` accepte place/classe d'actif et filtre les H1/D1 non closes ; grâce
  de 10 minutes, timezone IANA et DST ; close régulier conservateur pour les
  demi-séances.
- validation OHLCV centralisée avant indicateurs ; compteurs des barres rejetées.
- contrat `closed_only` obligatoire dans AG2, AG1 R8/safety et dashboard.
- seuil D1 harmonisé à 96 h ; âge UHQ recalculé au moment de l'audit.
- UHQ fail-closed et transactionnel ; disparition des `CHECKPOINT` online.
- curseur de rotation avancé seulement par `Finalize Run` après succès complet.
- cache IA réellement écrit/lu, sans réutilisation d'un REJECT ; gardes RR/stop
  déterministes.
- epsilon de neutralité, RSI plat à 50 et annualisation par session/classe.
- migration `20260726_ag2_v3_1_closed_bars`, vues explicites et lineage
  stratégie/config/prompt/modèle/exécution/hash.
- builders n8n déterministes et socle de tests hors production.

## Validation avant publication

- image yfinance shadow construite avec les versions exactes du lock ;
- tests temporalité Paris/NY/Tokyo/HK/crypto, DST, H1 et OHLCV ;
- probe `/history` sur une **copie** du cache AIR.PA : HTTP 200, 278 D1,
  dernière barre `closed=true`, `quality=VALID` ;
- tests compute : série plate, barres ouvertes, OHLC invalide, stale et
  annualisation ;
- migration sur copie de `ag2_v3.duckdb`, vues et segments vérifiés ;
- fault injection UHQ sans dépendance AG1 : erreur explicite et segments inchangés ;
- cursor replay : aucun avancement sur échec, avancement exact après commit.

Pendant le test initial de `Finalize`, son chemin DB embarqué a créé par erreur
la seule ligne de test `batch_state('shadow_cursor', 7)` dans la base live. Elle
a été détectée immédiatement puis supprimée ; contrôles avant/après : une ligne,
puis zéro. Aucun signal, run, segment, ordre ou curseur métier n'a été modifié.

## Déploiement

La séquence est : yfinance-api, workflows AG2, AG1, dashboard. Après chaque
import n8n, republier explicitement (l'import désactive), redémarrer n8n et les
task-runners, puis vérifier `active=1` et `activeVersionId`.

Le premier run planifié migre la base. L'ordre de publication est conçu pour
être fail-closed : AG1 ne considère pas une ligne technique ne portant pas le
nouveau contrat.

État vérifié après publication :

- yfinance-api `2.1.0`, image `sha256:f16bf1f78af3…`, `/health` OK ;
- Held+Core actif `56ccdbb1-aa34-48d2-a600-9eb9a87b9d66` ;
- Watchlist actif `ba27c29a-3dfc-4704-b59f-894c1ced1f37` ;
- Universe Health actif `bb3bda21-7495-4ce7-ae8b-79876643a260` ;
- AG1 actif `18f8cf38-e209-4790-9785-43708767fdc6` ;
- dashboard HTTP 200, hash repo/live
  `ee93fbe0829c0049b6c44da9d790656ff51190be5bd024c1bd0001d46350fb88` ;
- broker authentifié et aligné live, aucune approbation en attente ;
- segments inchangés : CORE_AUTO 50, CORE_MANUAL 18, HELD 11, WATCHLIST 283 ;
- aucune ligne `shadow_cursor` ; migration attendue au premier run Watchlist
  planifié à 22 h Paris le 2026-07-26.

## Rollback

1. republier les trois versions n8n pré-déploiement listées ci-dessus à partir
   des exports sauvegardés ;
2. restaurer l'image yfinance précédente (tag de rollback créé avant `up`) ;
3. restaurer `app.py` depuis sa sauvegarde horodatée et redémarrer le dashboard ;
4. republier la sauvegarde AG1 pré-déploiement ;
5. ne pas supprimer les colonnes de migration : elles sont additives et
   rétrocompatibles ;
6. vérifier `/health`, les workflows actifs et `core.runs` avant toute décision
   de reprise.

## Actions restantes hors livraison

- lease distribué/reprise par symbole (le curseur post-commit est déjà corrigé) ;
- vue dashboard dédiée au scope AG1 et alertes d'invariants de fin ;
- exercice RPO/RTO, SBOM/CVE et golden set corporate actions.
