# Déploiement de la couverture de tradabilité — 6 octobre 2026

## Périmètre effectivement livré

- Yahoo API **2.2.5** : backfill sur nombre de barres closes/valides insuffisant,
  tentative pleine fenêtre au plus une fois par 24 h (y compris réponse vide),
  contrat de qualité et `min_bars` cohérents sur cache et réseau ; aliases de
  données à ISIN constant PVL.PA→ALPVL.PA, LHYFE.PA→ALHYF.PA.
- Broker : endpoint de lecture historique `/iserver/marketdata/history` au lieu
  de l’ancien `/hmds/history` qui répondait 404 ; champ `contract_symbol` ajouté
  à la résolution pour contrôler l’identité du secours. Diff broker : deux lignes
  fonctionnelles, aucun code d’ordre ou d’approbation modifié.
- Secours D1 IBKR : `daily_fallback.py`, activé par
  `YF_DAILY_FALLBACK_BROKER_URL=http://ibkr-broker:8080`. Périmètre actions/ETF
  suffixés PA/DE/AS/MC/SW, devise attendue, symbole exact ; résolution primaire
  stricte du broker conservée. Bougies observées en séance régulière, validation
  OHLCV et fermeture +10 min. Au moins deux séances de chevauchement ; écart
  maximal OHLC ≤2 % sur toutes les séances communes. Cache JSON séparé 15 min
  sous `/data/ibkr-daily-fallback`, conid/source/date visibles dans les réponses.
  Un seul nouvel essai après 0,5 s est autorisé pour une réponse historique
  initialement vide sans erreur explicite. Sept historiques indisponibles ont
  répondu lors d’une nouvelle lecture ; la cause exacte de leur premier échec
  n’est pas démontrée. Les erreurs explicites et divergences restent bloquantes.
- AG2 `COVERAGE` : tout HELD/CORE/WATCHLIST hors quarantaine, ordre par dernière
  tentative ascendante, 80 symboles, cinq départs quotidiens. Le workflow garde
  son ID Watchlist pour la continuité. Held+Core reçoit le même code Init partagé,
  sans modification de sa sélection, de sa taille ou de ses horaires.
- Dashboard : date effective de bougie dans System Health et Analyse technique.
- Script reproductible de lecture seule : `outils/scripts/audit_tradability.py`.
- Rattrapage réel des 364 titres, après replay isolé, par les nœuds publiables
  Compute/Hydrate/Extract/Finalize. Les appels DeepSeek emploient les nœuds n8n
  modèle/prompt/parseur existants dans une instance temporaire isolée.

Aucun changement des poids, du scoring, de la règle REJECT, des seuils H1/D1 96 h,
YF 72 h, du plancher de volume ou des gardes broker. Aucun ordre placé ou confirmé.
AG1/AG3/AG4, Forex et AG9 non modifiés. Les métadonnées de publication n8n sont
synchronisées dans les JSON locaux ; l’import a généré de nouveaux versionId.

## Versions publiées et vérification

| Workflow | Avant | Publiée après |
|---|---|---|
| `AG2V3HELDCORE20260619` | `5d3db82c-5f43-4107-a44e-7ef7f336aece` | `efc4de0b-c49b-4a5c-b047-bab6f28bb131` |
| `AG2V3WATCHNIGHT20260619` | `c1ae65d9-d9e2-4707-8a65-93a49fc2d018` | `792b6f83-496b-4a97-875a-4790699f5509` |

Import puis `publish:workflow`, restart `root-n8n-1` et runners 3/4/5, contrôle
SQLite en lecture seule : `active=1`, `versionId=activeVersionId`, nœuds et
connexions de `workflow_history` identiques aux candidats, settings identiques.
**178 autres workflows inchangés**, aucun cron temporaire ajouté.
AG1 conserve `1555b344-4673-4f1c-ad10-081c9bb345ca`.

Source dashboard vérifiée par `docker inspect` :
`/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard`.
Source API : `/docker/root/yfinance-api/main.py`, image finale
`trader-ia/yfinance-tradability:20261006-warmup`, SHA image
`e5850e7aeb57b259cd1c0265765ecc0b6e939250eaa1a5ab29a2a4a06bac25f4`.
Broker : source `/opt/trader-ia/services/ibkr-broker`, image
`trader-ia/broker-tradability:20261006`, SHA
`35a33bea4bee698e8597e73342688eb8028b27e93e45d31d6b8b2ae1774df90b`.
Dépendances et lock inchangés ; Dockerfile API ajoute le module de secours.
API, broker et dashboard ont été recréés/redémarrés, ainsi que n8n/runners pour
la publication. Avant le restart broker : aucune approbation en mémoire, aucun
AG1/PF en cours ; après : compte live aligné, authentification OK, toutes les
variables d’environnement broker et les gardes identiques.

## Preuves et tests

- **61 tests réussis** : API 28, broker 7, dashboard 6, AG2 20. Compilation
  et `git diff --check` réussis. Rotation dans le vrai sandbox TaskExecutor :
  cinq lots couvrent les 364 titres distincts, sans quarantaine ni FX.
- Replay sur copies : TEAM/TXN/PLTR, 3/3 et un appel LLM réel ; puis
  BNP/AIR/ALV, 3/3 et un appel LLM réel. Le test API de secours a accepté cinq
  historiques sur six et refusé la divergence du sixième selon ses contrôles.
- Rattrapage initial `AG2V3_20261006062435214699_0` : **SUCCESS 364/364**,
  32 appels IA, 1 119 s. Rattrapage après secours
  `AG2V3_20261006071338288544_0` : **SUCCESS 260/260**, 599 s ; dernier essai
  `AG2V3_20261006072344307624_0` : **SUCCESS 7/7**, 25 s. Ces deux derniers
  lots n’ont appelé aucun LLM supplémentaire selon les filtres/cache existants.
  SUCCESS signifie traitement terminé ; la qualité peut rester bloquante.
- Mesure finale 09:24 Paris : **39→324** techniques conformes,
  **23→272** pré-tradables sur 364. Parité entre audit, les deux calculs
  dashboard et R8 publié : zéro divergence. L’indicateur d’appels IA du
  replay de vue n’est pas un registre d’appels ; les comptes ci-dessus viennent
  des `run_log` réels.
- Cron Held+Core **09:00**, exécution **22789**, terminé SUCCESS à 09:13:27
  (22 titres). Paramètres explicites des nœuds et connexions conformes aux
  candidats ; les paramètres par défaut ajoutés par n8n sont distingués.
- Univers 566, segments 365, table de quarantaine 488 lignes : contenus métier
  inchangés (hors `updated_at`). 178 autres workflows inchangés, AG1 inchangé.
- Sources locales, hôte et conteneurs : SHA-256 conformes sur sept fichiers.
  Santé finale 09:29 Paris : API 2.2.5 OK, dashboard OK, broker authentifié,
  compte live aligné, zéro approbation en attente. Comptes d’ordres et de fills
  inchangés ; seul le PF planifié de 09:15 a ajouté son run normal au ledger.
- Les deux conteneurs d’essai identifiés et leur profil privé ont été supprimés
  à 09:28 ; sauvegardes et preuves privées conservées. Aucun service d’essai
  ni credential de replay ne reste actif.

Répertoire privé de preuves et sauvegardes :
`/local-files/.codex-tmp/tradability-20261006` sur le VPS, copie locale
`.codex-tmp/tradability-20261006`. Synthèse sans secrets versionnée dans
`docs/audits/evidence/20261006_tradability.json`.

Commande pour refaire la mesure, depuis une copie du script dans `/files` :

```sh
docker exec -u root root-n8n-1 python /files/.codex-tmp/tradability-20261006/audit_tradability.py --output /files/.codex-tmp/tradability-20261006/audit-after.json
```

Le script n’écrit que son rapport JSON, jamais les bases. Comparer le même
ensemble de symboles, les mêmes seuils et l’heure de mesure. La maintenance
ponctuelle, distincte de l’audit, utilisait un venv **duckdb 1.4.4**, attendait
l’absence d’écrivain AG2/UHQ et identifiait ses lignes par un run_id unique.
L’instance n8n de replay n’avait ni accès au broker ni montage des bases métier.
Sa copie chiffrée de la credential et son profil sont supprimés après validation.

## Source du correctif historique

L’[API historique officielle IBKR](https://ibkrcampus.com/docs/web-api/v1/endpoints/market-data/historical-market-data)
utilise `/iserver/marketdata/history`. Le test direct a produit de vraies bougies
BNP/Airbus/ASML, alors que la route HMDS renvoyait 404. La réparation automatique
`yfinance repair=True` a aussi été essayée en conteneur isolé, mais n’a pas été
retenue : elle reconstruit depuis l’intraday et peut différer de la clôture et du
volume officiels ([limites documentées](https://ranaroussi.github.io/yfinance/advanced/price_repair.html)).
La route historique corrigée est aussi utilisée par les lecteurs macro existants :
leurs prochaines collectes pourront bénéficier de l’endpoint disponible. Aucun
workflow Forex n’est réactivé et aucun poids macro n’est modifié.
Aucune dépendance SciPy ajoutée à la production. Le chemin `/research/history`
d’AG3 reste distinct et inchangé.

## Sources des corrections d’identité

- Plastivaloire : [communiqué de l’émetteur, transfert Euronext Growth](https://groupe-plastivaloire.com/wp-content/uploads/2026/02/PVL_CP_Projet-transfert-Euronext-Growth-vdef-FR_compressed.pdf), ISIN FR0013252186 conservé, ALPVL.
- Lhyfe : [titre et actionnariat de l’émetteur](https://fr.lhyfe.com/investisseurs/titre-actionnariat/), ALHYF depuis le 8 septembre, ISIN FR0014009YQ1.
- Roche : [nouveau certificat de participation](https://www.roche.com/investors/updates/inv-update-2026-03-16), nouveau ROP / ISIN CH1499059983. Pas d’alias ajouté pour ROG.SW.
- ABB : [retrait NYSE](https://global.abb/group/en/investors/nyse-delisting), [ADR OTC ABBNY](https://global.abb/group/en/investors/investor-and-shareholder-resources/adrs). Pas d’alias ajouté pour ABB : le contrat EUR résolu pour ce code n’est pas l’ADR USD attendu.

Le refresh existant YF a traité PVL.PA et LHYFE.PA avec succès 2/2, run
`YFENRICH_20261006062621`. Les identités internes, les conid et les historiques
ne sont pas renommés. Les cas Roche/ABB nécessitent une migration canonique
complète ; une quote valide seule ne rend pas l’ancien instrument exécutable.

## Retour arrière ciblé

1. Attendre qu’AG2/UHQ et les exécutions n8n soient inactifs. Les sauvegardes
   `before/AG2V3HELDCORE20260619.json` et `before/AG2V3WATCHNIGHT20260619.json`
   contiennent les graphes publiés d’origine. Les copier avec permissions 644,
   importer chaque ID, publier chaque ID, redémarrer n8n/runners, puis vérifier
   `active=1` et les graphes publiés. Ne pas supposer que l’ancien UUID est réutilisé.
2. Restaurer les seuls fichiers dashboard `before/dashboard-app.py` et
   `before/ag2_funnel.py` vers le montage vérifié, puis redémarrer le dashboard.
3. Restaurer `before/yfinance-main.py`, retagger
   `trader-ia/yfinance-before-tradability:20261006` en `yfinance-yfinance-api`, puis :

```sh
docker compose -f /docker/yfinance/docker-compose.yml up -d --no-deps --no-build --force-recreate yfinance-api
```

4. Pour retirer uniquement le secours D1 : enlever la seule variable
   `YF_DAILY_FALLBACK_BROKER_URL` du compose puis recréer l’API. Pour revenir
   entièrement à la phase précédente, backups `before-phase2/api-main.py`,
   `api-Dockerfile`, `broker-app.py`, `broker-cpapi_client.py` et image
   `trader-ia/api-before-fallback:20261006` / `trader-ia/broker-before-tradability:20261006`.
   Ne restaurer le compose complet que s’il n’a pas changé depuis ; sinon patch
   ciblé de cette variable. Avant tout restart broker : recontrôler la file
   d’approbations et attendre qu’AG1/PF soient inactifs.
5. Les données collectées sont de vrais faits datés et peuvent être conservées
   lors d’un rollback de code. Si le rattrapage lui-même doit être retiré, un script
   dédié duckdb 1.4.4 peut supprimer **seulement** les `technical_signals` de son
   run_id et annoter `run_log`. Les caches IA/Yahoo et le refresh YF ont aussi reçu
   des faits valides : comparer les clés et dates avec les backups avant tout
   retour de données. **Ne jamais remplacer la base complète sur une production
   qui a repris ses écritures** ; les snapshots complets sont une assurance,
   pas une autorisation d’effacer les runs postérieurs.
6. Refaire l’audit, `/health`, `/orders/approvals/pending`, l’état publié et les
   compteurs ledger. Préserver toute activité manuelle ou planifiée concurrente.

## Limites d’exploitation

Premier départ de couverture après publication : **10:10 Paris**. La cadence
et sa durée sur un cycle complet restent à observer ; 400/j est une capacité
nominale. Les 28 tests API et 7 tests broker, la couverture sur copie, les tests AG2/dashboard et
le rattrapage ne prouvent ni une rentabilité accrue ni une disponibilité Yahoo
future. Le [rapport d’audit](../audits/20261006_tradability_audit.md) distingue les
exclusions résiduelles de source, de liquidité et d’analyse IA.
