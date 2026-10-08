# AG2 — Revue des positions détenues et clôture des bougies (5 octobre 2026)

## Problème confirmé
TXN était bien détenu et traité techniquement, mais aucun appel IA dans les 71 relevés conservés. Les filtres d'entrée H1 NEUTRAL et la fraîcheur horaire calculée en temps civil empêchaient sa revue. La normalisation des dates D1 américaines pouvait aussi admettre la bougie journalière en cours. Audit : `docs/audits/20261005_ag2_txn_missing_ai_audit.md`.

## Modifications
- AG2 initialise HELD depuis le dernier snapshot de portefeuille de moins de 96 h (lecture seule du ledger). Repli explicite sur les segments si la source est indisponible. Les nouvelles positions sont incluses sans attendre la classification nocturne.
- Une position détenue avec H1/D1 valides est éligible à une revue, même H1 NEUTRAL. Pour ce dernier cas, WATCH signifie observation, jamais autorisation d'achat. BUY/SELL restent soumis aux règles existantes ; REJECT ne signifie pas vendre.
- Fraîcheur IA H1 référencée à la dernière bougie attendue en séance régulière, fuseaux et week-ends compris. Référence fraîche ≤5 min ; écart maximal 3 h. Sans référence valide, ancien contrôle conservé. Limites dures H1/D1 96 h et marge de clôture 10 min inchangées.
- Les dates D1 à minuit UTC sont traitées comme dates de séance, sans décalage artificiel au jour précédent à New York.
- Cache HELD 4 h maximum, invalidation au changement de bougie/signature ; REJECT non réutilisé. Conservation du raisonnement, alignement, stop et R/R. Une note nulle n'est plus remplacée par 5.
- Held+Core passe de 16:35 à 16:41 Paris ; 09:00 et 13:00 inchangés. La première H1 américaine de la séance usuelle devient admissible à 16:40. AG1 reste à 17:10 (budget AG2 29 min, maximum historique 27 min).
- Dashboard : SKIP est affiché « Non analysé », avec la cause ; prix dans la devise du titre.

## Périmètre et limites
Deux workflows AG2, `services/yfinance-api/main.py` (2.2.1), dashboard technique. Aucun changement de poids/gates AG1, broker, ordres, Forex ou agents 5–9.
La référence de séance est régulière, sans calendrier des jours fériés ni clôtures exceptionnelles : approche conservatrice pouvant encore différer une revue dans ces cas, explicitée par `CONSERVATIVE_NO_HOLIDAY_EXEMPTION`. Les pauses intrajournalières ne sont pas modélisées. Aucune donnée absente n'est inventée. L'apport à la performance n'est pas démontré par ces validations fonctionnelles.

## Validation
- 19 tests AG2 et 12 tests API réussis, dont neutralité HELD, cache, limite 96 h, week-end, première H1 US et D1 US octobre/décembre.
- Replay dans le vrai sandbox Python des runners, sur une copie DuckDB : neuf positions identifiées, dont TXN et le récent TKO.PA ; TXN `call_ai=true`, `SESSION_FRESH`.
- Replay n8n isolé avec le modèle et le parseur publiés : réponse DeepSeek réelle sur TXN, extraction et cache vérifiés. Aucun accès au broker dans ce replay.
- Publication initiale : `active=1`, `versionId=activeVersionId`, graphes publiés identiques aux candidats. Les 178 autres workflows inchangés.
- Cycle réel `22751`, du 5 octobre 10:21:00 à 10:32:43 Paris : **SUCCESS**, 27/27 titres, zéro erreur, 12 appels IA, curseur **0 → 18** confirmé. Durée 11 min 43 s, dans le budget de 29 min avant AG1. Le run métier est `AG2V3_20261005082119340350_0`.
- **9/9 positions détenues analysées** : AZN, ELIS.PA, GOOGL, KO, MAU.PA, TKO.PA et UNP = WATCH (H1 NEUTRAL) ; PRX.AS et TXN = REJECT du setup d'entrée/renfort. Tous les raisonnements sont renseignés et aucune erreur d'écriture n'est présente.
- TXN : avis réellement produit à 10:24:49 Paris, qualité du setup 2/10, R/R 0,19 ; ce REJECT ne constitue pas une instruction de vente.
- Replay supplémentaire de cache : note 0 conservée à 0, `pass_pm=false`, champs de l'avis restaurés. Cette dernière correction de cache est publiée après le cycle de contrôle.
- La règle de contrôle temporaire (10:21 le 5 octobre) est retirée après le cycle ; seuls les créneaux permanents restent publiés.

## État final vérifié à 10:34 Paris
- Held+Core : `5d3db82c-5f43-4107-a44e-7ef7f336aece` ; Watchlist : `c1ae65d9-d9e2-4707-8a65-93a49fc2d018`. Tous deux actifs, `versionId=activeVersionId`, graphes exacts vérifiés après restart.
- Règle temporaire retirée ; 178 autres workflows inchangés, dont AG1 et les workflows Forex/AG9 dormants.
- Broker authentifié, compte aligné, aucune approbation en attente. API 2.2.1 OK ; dashboard OK ; runners/n8n démarrés. Ordres **175** et fills **125** inchangés pendant l'intervention ; un cycle PF planifié explique le passage des runs du ledger de 786 à 787.
- Sources hôte et candidates identiques ; dépendances, Dockerfile et module de recherche Yahoo inchangés. Profil n8n isolé et copie privée des credentials supprimés ; sauvegardes de rollback conservées.
- Restent à observer : prochain cycle AG2 16:41 en séance US et prochain passage automatique Watchlist. La performance financière n'est pas évaluée par ce test.

## Preuves et retour arrière
Dossier local : `.codex-tmp/ag2-held-fix-20261005/`. Dossier VPS : `/local-files/.codex-tmp/ag2-held-fix-20261005/`.
Sauvegardes : `before/state.json`, `before/AG2V3*.json`, `before/yfinance-main.py`, `before/dashboard-app.py`, `before/yfinance-image.txt`, copie AG2 avant changement.
Image précédente conservée sous `trader-ia/yfinance-before-ag2:20261005` ; candidate `trader-ia/yfinance-ag2-held:20261005`.
Source API hôte : `/docker/root/yfinance-api/main.py`. Dashboard monté : `/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app.py`.
Retour arrière : attendre l'absence d'exécution, restaurer uniquement ces deux sources, remettre le tag d'image précédent sur `yfinance-yfinance-api` puis recréer le seul service yfinance-api ; importer les deux exports `before/AG2V3*.json`, republier chacun puis redémarrer n8n/runners/dashboard. Vérifier les graphes publiés et les healthchecks. Ne pas restaurer aveuglément la base AG2 : elle contient les cycles ultérieurs et les nouvelles analyses réelles.
