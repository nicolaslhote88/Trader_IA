# Audit AG1-FX GPT - rejets ordres

Date: 2026-05-18

## Constat VPS

- Workflow actif: `AG1-FX-V1 Portfolio Manager - chatgpt52`.
- Workflows comparatifs `grok41_reasoning` et `gemini30_pro`: inactifs.
- Broker IBKR: `IBKR_DRY_RUN=false`, compte paper `DUQ816375`.
- Les 18 ordres problematiques dans `core.orders` se decomposent en:
  - 14 ordres `rejected / KILL_SWITCH_ACTIVE`;
  - 4 ordres `broker_error / IBKR_BROKER_ERROR`.

## Causes

1. Les 14 rejets `KILL_SWITCH_ACTIVE` ne sont pas des rejets broker. Le pre-run
   AG1-FX bloquait les nouvelles ouvertures quand la reconciliation IBKR
   echouait avec `HTTP Error 502: Bad Gateway`, typiquement pendant une session
   Client Portal expiree ou non authentifiee.

2. Les 4 erreurs broker incluent des refus IBKR du type:
   `FX trade would expose account to currency leverage`. Le compte paper actuel
   accepte des trades FX financables par la devise de base, mais refuse les
   ouvertures qui empruntent une devise non-EUR comme `GBPUSD`, `USDCAD` ou
   `GBPJPY`.

## Correctifs

- Le pre-run AG1-FX et le workflow `AG1-FX-PF-V1` tentent maintenant une
  reinitialisation brokerage ponctuelle via `/auth/initialize` quand c'est
  possible, puis journalisent `IBKR_MANUAL_LOGIN_REQUIRED` si un relogin
  navigateur/2FA est necessaire.
- Les logs de reconciliation conservent les lots DuckDB attendus meme si le
  broker est indisponible, ce qui evite les payloads vides peu actionnables.
- Correction de suivi: la reconciliation FX traite maintenant `/account/ledger`
  comme source autoritaire pour les contrats spot-FX CASH lorsque le ledger est
  lisible. Les deltas de pseudo-position CPAPI restent audites, mais ne bloquent
  plus les nouvelles ouvertures si le ledger cash est disponible. Les ecarts
  cash ne bloquent que si `IBKR_BLOCK_ON_CASH_DIVERGENCE=true`.
- `AG1_FX_CASH_ONLY_BASE_CCY_MODE=true` bloque avant broker les nouvelles
  ouvertures qui exposeraient le compte a du levier de devise non-EUR. Avec
  `AG1_FX_PORTFOLIO_BASE_CCY=EUR`, seules les nouvelles ouvertures `SELL_BASE`
  sur `EURxxx` et `BUY_BASE` sur `xxxEUR` passent. Les clotures/reductions de
  lots existants restent autorisees.
- Le brief compact donne cette contrainte au LLM pour reduire les propositions
  non executables.

## Verification attendue

- `core.reconciliation_log.reasons_json` doit afficher `IBKR_MANUAL_LOGIN_REQUIRED`
  quand la session CPAPI expire.
- Les prochaines propositions hors contrainte cash-only doivent etre rejetees
  par le validateur avec `IBKR_CASH_ONLY_EUR_LEG_REQUIRED`, sans appel broker.
- Apres relogin IBKR et reconciliation OK, les ouvertures compatibles EUR
  peuvent continuer a etre envoyees au compte paper.
