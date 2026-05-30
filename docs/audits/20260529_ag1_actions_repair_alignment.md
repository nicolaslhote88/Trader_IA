# Intervention AG1 actions - reprise alignement bout en bout

Date: 2026-05-29

## Constat

L'audit des workflows AG1 actions a montre que la sous-performance ne venait
pas d'un seul agent, mais d'un mauvais alignement entre donnees amont, decision,
risk manager, execution et ledger:

- les trois variantes actives AG1 actions pouvaient terminer en statut n8n
  `success` alors que le writer DuckDB retournait `FATAL_ERROR`;
- le ledger AG1 actions etait bloque par une erreur SQL sur la colonne reservee
  `"asof"`, donc les performances recentes n'etaient plus correctement
  comptabilisees depuis le 8 mai 2026;
- le node R8 chargeait trop de news et de donnees non bornees, ce qui causait
  des timeouts et diluait le signal utile;
- l'enrichissement Yahoo Finance utilisait encore par defaut `ag2_v2.duckdb`,
  alors que la production actions est sur `ag2_v3.duckdb`;
- les ordres live actions pouvaient partager le meme interrupteur global que le
  Forex paper (`IBKR_DRY_RUN=false`), sans verrou specifique actions;
- le broker IBKR resolvait mal plusieurs tickers Euronext/Paris de Yahoo
  Finance, notamment via le suffixe `.PA` et l'exchange `SBF`;
- le ledger projetait les ordres soumis comme s'ils etaient deja executes,
  ce qui pouvait fabriquer une performance de portefeuille non reconciliee.

## Correctifs appliques

- Correction du schema et du writer DuckDB avec quoting explicite de `"asof"`.
- Le node `9 - Upsert Run Bundle (DuckDB)` leve maintenant une erreur n8n
  bloquante si le writer echoue, au lieu de masquer l'echec dans un JSON
  `FATAL_ERROR`.
- R8 borne les donnees chargees: fenetre macro recente, agregation SQL des
  news par symbole, limite de lignes, ages H1/D1/YF et flags de qualite par
  symbole.
- Le service YF et son workflow quotidien ciblent `ag2_v3.duckdb` par defaut.
- Le risk manager actions rejette les instruments non actions/ETF, les donnees
  trop stale, les spreads excessifs, les achats sans cash suffisant, les sells
  sans position et les ordres trop gros au regard des plafonds portefeuille,
  secteur et ordre.
- Le bundle ledger ne materialise plus les ordres live seulement `submitted`;
  seuls les ordres dry-run/simulation ou les executions/fills explicites
  modifient les snapshots et les fills.
- Le node d'envoi IBKR actions ajoute le verrou
  `AG1_ACTIONS_LIVE_ORDERS_ENABLED`; par defaut, les ordres actions sont
  bloques meme si le Forex paper tourne avec `IBKR_DRY_RUN=false`.
- Le broker IBKR accepte `isin` et `exchange`, tente une resolution ISIN, puis
  les aliases Yahoo/IBKR (`.PA`: `SBF`, `ENEXT`, `EUIBS`, `SMART`) avant de
  retourner une erreur detaillee avec exchanges disponibles.
- Le job YF quotidien cible `ag2_v3.duckdb`, borne les appels
  metadata/options/calendar et garde les quotes sur tout l'univers.
- Le service `yfinance-api` utilise un fichier temporaire unique pour ecrire
  son etat global, ce qui evite les courses concurrentes sur `_global.json.tmp`.

## Parametres de securite

- `AG1_ACTIONS_LIVE_ORDERS_ENABLED=false` par defaut.
- `AG1_R8_NEWS_LOOKBACK_DAYS=30` par defaut.
- `AG1_R8_MAX_MACRO_ROWS=2500` par defaut.
- `AG1_R8_MAX_SYMBOL_NEWS_ROWS=6000` par defaut.
- `AG1_ACTIONS_MAX_H1_AGE_HOURS=96` par defaut.
- `AG1_ACTIONS_MAX_D1_AGE_HOURS=240` par defaut.
- `AG1_ACTIONS_MAX_YF_AGE_HOURS=72` par defaut.
- `YF_MIN_SECONDS_BETWEEN_CALLS=1` par defaut VPS.
- `YF_ENRICH_METADATA_MAX_SYMBOLS=40` par defaut.
- `YF_ENRICH_QUOTE_CHUNK=40` et `YF_ENRICH_TIMEOUT_SEC=90` cote service YF.

## Validation effectuee

Deploiement VPS effectue sur les workflows actifs:

- `6QZzRb78XamsDcJ1hpdyM` - AG1 actions ChatGPT 5.2;
- `LZmpNha1QixeknWUya9iD` - AG1 actions Gemini 3.0 Pro;
- `a3j6UPo73g4U8Dbr8axCT` - AG1 actions Grok 4.3;
- `OclNuZS2_izwIgavMQloh` - YF-ENRICH-V1 daily refresh.

Controles realises:

- syntaxe Python OK pour `yf-enrichment-service`, `yfinance-api`,
  `ibkr-broker` et le writer AG1;
- syntaxe JavaScript OK pour les nodes post-agent AG1, testee dans le runtime
  Node du conteneur n8n avec enveloppe async;
- schema DuckDB OK avec colonne `"asof"` quotee;
- writer AG1 OK sur base temporaire avec ordre + fill + reconstruction de lot;
- R8 standalone sur les bases reelles: 385 lignes en environ 22 s, 318 lignes
  `Data_OK_For_Trading=true`, 67 lignes bloquees par flags de qualite;
- YF smoke: 3/3 symboles OK;
- YF production apres correction: 447 quotes OK sur 463 symboles, 16 erreurs
  residuelles liees a des symboles Yahoo invalides/delistings
  (`PriceHistory` / `_dividends`);
- garde-fou live actions confirme: avec `IBKR_DRY_RUN=false` et
  `AG1_ACTIONS_LIVE_ORDERS_ENABLED=false`, un ordre action test est bloque
  avant broker et n'entre pas dans les ordres ledger.

Un run complet AG1 ChatGPT 5.2 via conteneur n8n one-off a permis de verifier
que R8 et l'agent passent, puis a expose deux problemes corriges:

- injection SQLite de R8/9 remise de `python` vers `pythonNative`;
- writer DuckDB corrige pour caster `ts_fill` en texte et eviter la dependance
  runtime `pytz` du runner n8n.

Le run complet one-off final n'a pas ete conserve comme validation definitive,
car les task-runners refusaient le broker temporaire one-off en 403. Le controle
operationnel attendu est donc le prochain run planifie par l'instance n8n
principale, dont les runners sont a nouveau enregistres.

## Validation attendue

Les controles post-deploiement doivent confirmer:

- syntaxe Python/JavaScript OK dans le runtime du VPS;
- schema et writer DuckDB OK sur une base de test;
- workflow YF capable de lire `ag2_v3.duckdb` et de produire des lignes
  d'enrichissement recentes;
- workflows AG1 actions actifs mis a jour dans n8n;
- absence de nouvel ordre action live tant que
  `AG1_ACTIONS_LIVE_ORDERS_ENABLED=false`;
- les prochains runs AG1 actions doivent echouer visiblement si le writer ou le
  ledger cassent, au lieu de produire un faux succes.

## Suivi attendu

Apres deploiement, le KPI principal n'est pas le nombre d'ordres proposes mais
la qualite economique des ordres acceptes: cash disponible, frais estimes,
spread, taille, exposition sectorielle, fraicheur des donnees, taux de rejet du
risk manager et reconciliation entre DuckDB et IBKR.

## Verification post-deploiement du 2026-05-30

La verification des runs planifies du 2026-05-29 16:45 Europe/Paris a montre
que le ledger AG1 actions avait bien repris l'ecriture, mais qu'un garde-fou
residuel du node `8 - Build DuckDB Bundle` generait encore des fills comptables
pour des ordres `REJECTED` ou `SUBMITTED`.

Correction appliquee:

- `orderHasFillLikeEffect` n'accepte plus `dry_run` ou `broker=SIM` comme effet
  de fill par defaut;
- un fill comptable n'est produit que pour un statut explicitement
  `FILLED`/`EXECUTED`, ou pour un payload de fill broker reel non rejete;
- les trois workflows n8n actifs AG1 actions ont ete mis a jour et redemarres.

Nettoyage des donnees:

- les fills invalides des derniers runs ChatGPT, Gemini et Grok ont ete
  neutralises avec `qty=0`, `fees_eur=0`, `liquidity=QUARANTINED`;
- les snapshots `portfolio_snapshot`, `positions_snapshot` et `risk_metrics`
  lies a ces runs ont ete retires pour eviter toute lecture de performance
  polluee;
- une alerte `INVALID_FILLS_QUARANTINED` a ete ajoutee dans chaque base pour
  garder la trace d'audit.

Controles de validation:

- test isole du node 8 dans le conteneur n8n: un lot mixte
  `REJECTED`/`SUBMITTED`/`dry_run`/`FILLED` ne produit plus qu'un seul fill,
  celui de l'ordre `FILLED`;
- verification DuckDB: `invalid_positive_fills=0` sur les trois derniers runs;
- `AG1_ACTIONS_LIVE_ORDERS_ENABLED=false` confirme sur n8n et les trois
  task-runners;
- YF daily du 2026-05-30 06:15 Europe/Paris: 448 quotes OK sur 463 symboles,
  statut `PARTIAL` limite aux symboles Yahoo invalides/delistings;
- IBKR broker up et healthy, mais session IBKR non authentifiee au
  2026-05-30 09:18 Europe/Paris (`manual_login_required=true`).

## Reparation des courbes, cash et ecarts du 2026-05-30

Un second controle visuel du dashboard a montre que les courbes et l'allocation
ne refletaient pas encore la realite economique:

- le dernier snapshot ledger valide etait retombe au 2026-05-08 apres
  quarantaine du run pollue;
- le miroir `portfolio_positions_mtm_latest/history` contenait encore un point
  PFMTM du 2026-05-29 construit avant le nettoyage;
- l'historique contenait des fills positifs sur des ordres `PLANNED` et
  `SUBMITTED`, ce qui generait des courbes irrealistes et du cash negatif.

Correction de donnees appliquee sur les trois bases AG1 actions:

- mise en quarantaine de tous les fills dont l'ordre joint n'est pas
  `FILLED`/`EXECUTED`;
- reconstruction de `core.cash_ledger` avec un depot initial de 50 000 EUR et
  les flux cash issus uniquement des fills executes;
- application d'une contrainte cash-only lors du replay historique: un achat
  Grok `DSY.PA` a ete reduit de 123 a 106 titres pour eviter un cash negatif,
  et une vente sans lot disponible a ete neutralisee;
- reconstruction complete de `core.position_lots`, `core.positions_snapshot`,
  `core.portfolio_snapshot`, `core.risk_metrics`;
- reconstruction du miroir `portfolio_positions_mtm_latest/history` depuis les
  snapshots core repares avec `ag1_source_run_id`.

Etat apres reparation:

- ChatGPT: cash min 7 483,85 EUR; dernier cash 8 219,69 EUR; valeur totale
  52 049,72 EUR; aucun fill positif non execute;
- Gemini: cash min 63,15 EUR; dernier cash 76,55 EUR; valeur totale
  50 692,04 EUR; aucun fill positif non execute;
- Grok: cash min 17,07 EUR; dernier cash 2 280,27 EUR; valeur totale
  51 653,31 EUR; aucun fill positif non execute.

Correction dashboard:

- `_prepare_performance_timeseries` choisit maintenant la source de priorite la
  plus faible lorsqu'un meme timestamp existe dans plusieurs sources, ce qui
  empeche le MTM de masquer le snapshot ledger repare;
- le node `10 - Post-Run Health (DuckDB)` renseigne desormais
  `ag1_source_run_id` et `ag1_source_snapshot_ts` dans le miroir MTM pour les
  prochains runs.
