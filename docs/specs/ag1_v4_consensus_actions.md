# AG1 V4 Consensus Actions

Date: 2026-06-10

## Objectif

AG1 V4 consolide les trois Portfolio Managers actions historiques dans un
workflow unique. GPT, Grok et Gemini recoivent le meme brief d'entree; le
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
- `gemini30_pro`

Les trois extracteurs ajoutent `modelKey` et `modelName`, puis un merge 4
entrees collecte le contexte AG1.00 et les trois propositions.

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
