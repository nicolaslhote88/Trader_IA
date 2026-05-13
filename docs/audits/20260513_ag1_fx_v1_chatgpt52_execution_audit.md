# Audit executions AG1-FX-V1 Portfolio Manager - chatgpt52

Date audit: 2026-05-13
Source: VPS `srv961978`, n8n SQLite, DuckDB `/local-files/duckdb/*`

## Conclusion

Le workflow tourne, mais il ne fonctionnait pas correctement sur les derniers runs.
Depuis la version n8n mise a jour le 2026-05-11 16:04, les noeuds AG1-FX de lecture
AG2/AG4 echouaient silencieusement avec `name 'hasattr' is not defined` dans le
runner Python natif n8n. Le workflow continuait donc avec:

- `technical_signals = []`
- `macro_news = {top_news: [], pair_focus: {}, macro_regime: {}}`
- `fundamental_fx = {}`
- `pair_matrix = []`
- `market_watch = 2`, uniquement les deux positions ouvertes (`EURCHF`, `EURJPY`)

Cela explique directement pourquoi le LLM ne proposait presque plus rien: il ne
voyait plus l'univers FX exploitable, seulement les lots existants.

## Chiffres principaux

- Runs AG1-FX PM persistes: 21 entre 2026-05-06 et 2026-05-13.
- Decisions LLM: 98 au total, dont 94 `hold` et 4 `open_short`.
- Ordres crees: 6.
- Fills confirmes IBKR: 2, sur `EURCHF` et `EURJPY`.
- Ordres rejetes par risk/reconciliation: 3.
- Ordre en broker_error: 1, sur `GBPJPY`.
- Table `core.ai_signals`: vide avant correction, malgre `core.runs.decision_json`.
- Positions ouvertes au 2026-05-13 07:00 UTC: 2 (`EURCHF` short 0.018 lot, `EURJPY` short 0.01 lot).
- Equity derniere valorisation: 9988.34 EUR, PnL total environ -11.66 EUR.

## Chronologie utile

- 2026-05-06: deux positions ont ete ouvertes et confirmees par IBKR:
  - `EURCHF` short, 0.018 lot.
  - `EURJPY` short, 0.01 lot.
- 2026-05-10 et 2026-05-11: le LLM a propose de nouveaux shorts (`CADJPY`, `GBPCHF`), mais ils ont ete rejetes car le pre-run reconciliation bloquait les nouvelles ouvertures.
- 2026-05-11 16:04: workflow n8n mis a jour.
- Apres cette mise a jour, les runs montrent le symptome systematique: AG2/AG4/AG3 absents du brief compact, puis le LLM ne retourne que 2 holds sur les positions ouvertes.
- 2026-05-13: plusieurs reconciliations IBKR echouent aussi par `HTTP Error 502: Bad Gateway`, ce qui active le blocage deterministic des nouvelles ouvertures.

## Audit noeud par noeud

### 01 Init Run FX

Conforme sur la derniere execution inspectee (`2026-05-13 08:30 Paris`):

- `run_id = AG1FX_chatgpt52_20260513063000`
- `llm_model = gpt-5.5`
- chemins DuckDB corrects (`ag1_fx_v1_chatgpt52`, `ag2_fx_v1`, `ag3_fx_v1`, `ag4_fx_v1`)
- `dry_run = false`

Point d'attention: le nom de variante reste `chatgpt52`, mais le modele runtime est `gpt-5.5`.

### 02 Load Universe FX

Charge 34 paires activees, pas 27.

Point d'attention: l'univers inclut des exotiques (`EURCNH`, `USDMXN`, `USDNOK`,
`USDSEK`, etc.). AG2 ne produit actuellement que 27 signaux sur le dernier run,
donc l'univers charge par AG1 peut etre plus large que les donnees techniques
disponibles.

### 03 Load Portfolio State FX

Charge correctement l'etat portefeuille et les deux lots ouverts. Le noeud
declenche aussi un blocage quand IBKR reconciliation echoue.

Dernier run inspecte:

- `equity_eur = 9988.34`
- `gross_exposure_eur = 2800`
- `leverage_effective = 0.2803`
- `open_lots = 2`
- `ibkr_reconciliation.ok = false`
- raison: `IBKR_RECONCILIATION_FAILED:HTTP Error 502: Bad Gateway`

Ce blocage est prudent cote broker, mais il empeche toute nouvelle ouverture.

### 04 Load Technical Signals FX

Non conforme avant correction.

Sortie observee le 2026-05-13:

- `technical_signals = []`
- `technical_error = name 'hasattr' is not defined`

La base AG2 contenait pourtant bien le dernier run:

- `AG2FX_20260513060000`
- 27 paires fetch/signal
- 0 erreur

Cause: `json_safe()` utilisait `hasattr(value, "isoformat")`, indisponible dans
le runner Python natif n8n.

### 05 Load News Macro FX

Non conforme avant correction.

Sortie observee:

- `top_news = []`
- `pair_focus = {}`
- `macro_regime = {}`
- `macro_news_error = name 'hasattr' is not defined`

La base AG4 contenait pourtant un digest frais:

- `AG4FXD_20260513061000`
- `news_after_dedupe = 30`
- `sections_written = 3`
- regime macro: `Risk-Off`, confidence `0.9`

Meme cause que le noeud 04.

### 05b Load Fundamental FX

Non conforme avant correction sur les derniers runs: `fundamental_fx = {}`.

La base AG3 contenait pourtant 34 paires scorees et un statut `DEGRADED`, pas
vide. Le statut degrade vient de donnees macro manquantes/stales, mais il reste
utilisable comme signal prudent.

### 06 Assemble Brief FX

Non conforme par propagation.

Dernier brief inspecte:

- `pair_matrix_len = 0`
- `market_watch_len = 2`
- `top_news_len = 0`
- `fundamental.by_pair` absent
- `macro.market_regime = Unknown`
- `macro.confidence = 0`

Consequence: le LLM ne pouvait plus scanner l'univers FX; il gerait seulement
`EURCHF` et `EURJPY`.

### Agent #1

Comportement logique compte tenu du brief defectueux: retour de deux decisions
`hold`, avec rationale disant que AG3/AG4/AG2 sont indisponibles.

Ce n'est pas un manque d'agressivite du modele: le modele recevait un brief
appauvri.

### 10 Parse Decision FX

Conforme: parse correctement le JSON LLM.

Point d'attention: si le LLM ne retourne que les positions ouvertes, le parser ne
complete pas automatiquement des `hold` pour les paires manquantes. Cela rend le
nombre de decisions peu comparable d'un run a l'autre.

### 11 Validate Enforce Safety FX

Conforme cote prudence: active `kill_switch_active_effective` si reconciliation
IBKR bloque.

Effet observe: les propositions `CADJPY`/`GBPCHF` ont ete rejetees par
`KILL_SWITCH_ACTIVE`, pas par manque de conviction LLM.

### 11b a 16

Pas d'anomalie bloquante sur les derniers runs, car aucun ordre executable ne
passait la safety.

Historique: un fill `EURCHF` a necessite une reparation manuelle apres un souci
timestamp au noeud 14, mais le ledger actuel contient bien les deux lots ouverts.

### 17 Log Run FX

Avant correction, le noeud persistait `core.runs.decision_json`, mais
n'ecrivait pas `core.ai_signals`. Cela rendait la table decisionnelle par paire
vide et limitait l'audit.

Correction appliquee: insertion `INSERT OR REPLACE` dans `core.ai_signals` pour
chaque decision LLM.

## Corrections appliquees dans le repo

- Remplacement de `hasattr(value, "isoformat")` par un appel direct protege
  `try: value.isoformat() ... except AttributeError`.
- Application aux noeuds:
  - `04_load_technical_signals_fx.py`
  - `05_load_news_macro_fx.py`
  - `05b_load_fundamental_fx.py`
- Ajout de la persistance des decisions par paire dans `17_log_run_fx.py`.
- Regeneration des workflows `AG1_FX_workflow_*_v1.json`.

## Risques restants

- La prod n8n doit etre redeployee/importee avec le JSON corrige; sinon les runs
  planifies continueront a utiliser l'ancienne definition.
- Les erreurs de chargement de donnees sont encore non fatales: le workflow peut
  continuer avec un brief vide. Recommandation suivante: ajouter un hard gate si
  `technical_signals`, `macro_news` ou `fundamental_fx` sont vides hors mode
  dry-run.
- IBKR renvoie parfois `502/503`; tant que ce blocage arrive en pre-run,
  nouvelles ouvertures impossibles.
- L'univers AG1 charge 34 paires alors que la specification historique parlait
  de 27 paires et que le dernier AG2 n'en score que 27.
