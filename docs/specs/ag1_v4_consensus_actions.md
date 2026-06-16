# AG1 V4 Consensus Actions

Date: 2026-06-10

## Objectif

AG1 V4 consolide trois Portfolio Managers actions dans un workflow unique.
GPT, Grok et Claude Sonnet recoivent le meme brief d'entree; le
workflow ne transmet une intention d'ordre au Risk Manager que si au moins deux
modeles sur trois produisent un consensus executable.

## Perimetre

- Inclus : actions et ETF pour execution IBKR, avec compatibilite de brief
  actions/ETF/crypto heritee de V3.
- Exclu : Forex. Les workflows `AG1-FX`, `AG2-FX`, `AG3-FX`, `AG4-FX`,
  `AG4-Forex` et les bases FX restent separes.
- Les symboles `FX:*`, `*=X` et les asset classes `FX`, `FOREX`, `CURRENCY`
  sont ignores par le consensus et rejetes par le safety manager.

## Base DuckDB

V4 utilise une nouvelle base:

```text
/files/duckdb/ag1_v4_consensus.duckdb
```

Cette base repart avec:

- `cfg.portfolio_config.initial_capital_eur = 10000`
- un depot initial `core.cash_ledger.INIT_CASH_AG1_V4_CONSENSUS = 10000`
- aucun import d'historique AG1 V3

Le schema V4 etend le ledger V3 avec:

- `core.model_proposals` : sortie brute par modele
- `core.consensus_votes` : votes normalises par modele/symbole/intent
- `core.consensus_decisions` : decisions consensus, approuvees ou rejetees

## Workflow

Le pipeline pre-agent est conserve:

```text
2B -> 4B -> 4C
2B -> AG4.01 -> 20J
2B -> R8 -> Calcul Matrice
Merge7 -> AG1.00
```

`AG1.00` diffuse ensuite le meme input pack vers:

- `chatgpt52`
- `grok41_reasoning`
- `claude_sonnet46`

Les trois extracteurs ajoutent `modelKey` et `modelName`, puis un merge 4
entrees collecte le contexte AG1.00 et les trois propositions.

Note 2026-06-16: la troisieme branche V4 a ete remplacee de Gemini 3.5 Flash
vers Claude Sonnet 4.6, avec output parser structure retabli. Les anciens runs
peuvent encore contenir `gemini30_pro` dans `core.model_proposals` et
`core.consensus_votes`; les nouveaux runs doivent persister `claude_sonnet46`.

## Regle De Consensus

Une intention est executable si:

1. au moins deux sorties modele sont valides;
2. au moins deux modeles votent le meme symbole;
3. ces deux modeles votent le meme intent normalise:
   - `OPEN`, `INCREASE`, `BUY` -> `BUY`
   - `DECREASE`, `CLOSE`, `SELL` -> `SELL`
4. le symbole n'est pas Forex/devise;
5. les prix limites concordants ne divergent pas de plus de 5%.

La taille retenue est prudente:

- quantite: minimum des quantites positives votees;
- poids cible: minimum des poids positifs votes;
- limite BUY: prix limite le plus bas;
- limite SELL: prix limite le plus haut.

Le consensus ne bypass jamais le Risk Manager. La sortie consensus devient
`agentDecision.actions`, puis le node `7 - Validate & Enforce Safety` applique
les controles deterministes: cash, taille, secteur, data freshness, spread,
kill switch, limite BUY obligatoire.

## Execution IBKR

Par defaut:

- `IBKR_DRY_RUN=true`
- `AG1_ACTIONS_LIVE_ORDERS_ENABLED=false`
- `AG1_V4_ACTIONS_IBKR_ENABLED_MODELS=ag1_v4_consensus`

Un ordre ne peut donc partir que si:

1. consensus 2/3;
2. validation safety;
3. gate actions ouvert;
4. modele logique V4 autorise;
5. garde-fou paper/live IBKR satisfait.

Les fills ne sont ecrits que pour des executions confirmees ou des fills
simules explicites; les ordres rejetes, soumis ou en erreur ne creent pas de
fill ledger.

Chaque fill V4 alimente aussi `core.fill_costs`, sur le meme principe que le
systeme Forex: une ligne par fill, avec commission brute, devise, montant EUR,
source de commission et JSON broker brut. Le dashboard Actions s'appuie sur
cette table pour distinguer P&L realise brut, frais IBKR et P&L realise net.

## Valorisation Latente et Universe DuckDB

Etat verifie le 2026-06-14:

- `YF-ENRICH-V1 - Daily DuckDB Refresh` alimente `yf_enrichment_v1.duckdb`
  depuis `ag2_v3.duckdb.main.universe`; il ne met pas a jour la valeur
  latente du portefeuille AG1 V4.
- `AG1-PF-V1 - Portfolio MTM (DuckDB-only, AG1-V4)` est le workflow de
  valorisation recurrente du portefeuille V4. Il lit et ecrit uniquement
  `/files/duckdb/ag1_v4_consensus.duckdb` pour la partie AG1, et enrichit les
  metadonnees de titres depuis `/files/duckdb/ag2_v3.duckdb`.
- Les workflows actions `AG2-V3`, `AG3-V2`, `AG4-V3` et `AG4_Spe-V2` lisent
  l'univers depuis `ag2_v3.duckdb.main.universe` et ne doivent plus charger
  l'onglet Google Sheets `Universe`.
- `AG4-V3` conserve Google Sheets uniquement pour la configuration des sources
  RSS (`Source_RSS`) tant qu'aucune table DuckDB equivalente n'est introduite.

Correction verifiee le 2026-06-15:

- les noeuds de lecture, normalisation, prix Yahoo 1D/1H, fusion et calcul MTM
  fonctionnaient; l'erreur etait limitee au writer final;
- l'ancienne table `portfolio_positions_mtm_latest` n'avait pas de contrainte
  unique sur `symbol`, alors que le writer utilisait `ON CONFLICT(symbol)`;
- le writer migre maintenant les colonnes manquantes et remplace chaque ligne
  par `DELETE` puis `INSERT` dans une transaction, sans dependre des anciennes
  contraintes DuckDB;
- toute ecriture partielle ou en erreur fait desormais echouer le workflow n8n
  au lieu de produire un faux statut vert;
- le run n8n `18931` a ete verifie de bout en bout avec quatre lignes ecrites,
  zero erreur et un run log DuckDB `SUCCESS`.

## Planification AG1 V4

Etat verifie le 2026-06-14:

- `AG1 V4 - Consensus Portfolio Manager` est planifie uniquement les jours
  ouvres a 14:00 Europe/Paris (`0 0 14 * * 1-5` dans le `Schedule Trigger`
  n8n).
- Le noeud `2B - Init Run Context` contient aussi une garde week-end:
  samedi/dimanche, il retourne `[]` avant le preflight IBKR et avant tout appel
  LLM, sauf si `allow_weekend_run=true` ou `AG1_V4_ALLOW_WEEKEND_RUN=true` est
  explicitement pose pour un test.

## Bascule Production IBKR

Etat verifie le 2026-06-11:

- les workflows Forex dedies sont desactives dans n8n;
- les workflows AG1 V3 actions sont desactives dans n8n;
- le workflow AG1 V4 consensus est le workflow actions actif;
- `IBKR_DRY_RUN=false`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED=true` et
  `IBKR_REQUIRE_PAPER_ACCOUNT=false` sont reserves a cette bascule live;
- `AG1_ACTIONS_IBKR_ENABLED_MODELS=__disabled_ag1_v3__` bloque l'ancien chemin
  AG1 V3 meme en cas de reactivation manuelle;
- `IBKR_FX_ORDERS_ENABLED=false` bloque `POST /orders/fx` au niveau
  `ibkr-broker`;
- la base `/files/duckdb/ag1_v4_consensus.duckdb` est propre: 10 000 EUR de
  cash initial, aucun run, aucune proposition, aucun ordre, aucun fill.

Pour basculer AG1 V4 en trading reel:

1. authentifier la Gateway IBKR sur le compte reel, pas sur le compte paper;
2. verifier que `/v1/api/iserver/accounts` retourne `isPaper=false` et un
   compte non prefixe par `DU`;
3. renseigner `IBKR_ACCOUNT_ID` avec ce compte reel, ou le laisser vide pour
   auto-detection si un seul compte reel est expose;
4. poser `IBKR_REQUIRE_PAPER_ACCOUNT=false` cote n8n seulement apres cette
   verification;
5. activer `AG1 V4 - Consensus Portfolio Manager` dans n8n.

Ne pas activer le workflow V4 live si la Gateway ne voit que `isPaper=true`:
le systeme serait alors actif sur le mauvais environnement IBKR.
