# Execution IBKR

Ce document decrit le branchement IBKR Client Portal API ajoute au projet.

## Architecture

```text
n8n Code node 07b / 11b
  -> http://ibkr-broker:8080
  -> https://ibkr-gateway:5000/v1/api
  -> IBKR
```

Le `ibkr-broker` encapsule l'API IBKR. Cote Forex, le mode production actuel est
un environnement **IBKR paper** avec `IBKR_DRY_RUN=false`; cote actions/ETF, le
chemin reste dry-run par defaut tant qu'une validation separee n'a pas ete faite.

References IBKR utilisees :

- Web API orders: https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-doc/
- Gateway / staging notes: https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-staging/

## Services Docker

- `services/ibkr-gateway/` telecharge et lance `clientportal.gw`.
- `services/ibkr-broker/` expose `/health`, `/orders/fx`, `/orders/equity`,
  `/fills`, `/positions` et `/auth/tickle`.
- Sur le VPS actuel, les deux services sont integres directement dans le stack
  `/docker/yfinance`. Le fichier de reference a copier comme compose principal
  est `infra/vps_hostinger_config/docker-compose.yfinance.yml`.
- `infra/vps_hostinger_config/docker-compose.yml` garde aussi la definition pour
  un deploiement complet du repo, mais ce n'est pas le mode utilise sur le VPS.
- Les nodes n8n lisent `IBKR_BROKER_URL`, `IBKR_DRY_RUN`,
  `IBKR_SEND_DRY_RUN_TO_BROKER`, `IBKR_REQUIRE_PAPER_ACCOUNT` et
  `IBKR_PAPER_ACCOUNT_PREFIXES`. Sans variable explicite, ils restent en
  dry-run et utilisent `http://ibkr-broker:8080`.

## Production Paper Forex (2026-05-06)

Etat deploye :

- `IBKR_DRY_RUN=false` pour le Forex paper.
- `IBKR_REQUIRE_PAPER_ACCOUNT=true`.
- `IBKR_PAPER_ACCOUNT_PREFIXES=DU`.
- Compte paper attendu : un compte dont l'identifiant commence par `DU`.
- Seul `AG1-FX-V1 Portfolio Manager - chatgpt52` est actif.
- `grok41_reasoning` et `gemini30_pro` sont desactives pour eviter que trois PMs
  ecrivent des ordres sur le meme compte IBKR.
- `AG1-FX-PF-V1 - Hourly Portfolio Valuation` est actif et reconcilie le ledger
  GPT toutes les heures.

Garanties ajoutees :

- Pre-run reconciliation dans `03_load_portfolio_state_fx.py` : compare les lots
  DuckDB ouverts avec `/positions` IBKR. Une divergence active le kill switch et
  bloque les ouvertures.
- Global lock AG1-FX : empeche deux runs PM simultanes contre le meme compte.
- Le validateur `11 Validate Enforce Safety FX` bloque deterministiquement tout
  nouvel ordre dont le compact pack indique `trade_permission=NO_NEW_POSITION`.
- Le meme validateur limite les ouvertures `REDUCED_SIZE_ONLY` a
  `AG1_FX_REDUCED_SIZE_MAX_PAIR_PCT` de l'equity, par defaut `0.10`, avant
  l'envoi broker.
- En `IBKR_DRY_RUN=false`, `12_simulate_fills_fx.py` ne cree plus de fill simule.
  Il ne persiste que les fills confirmes par IBKR.
- Les ordres envoyes mais sans fill immediat restent `submitted`; le workflow PF
  importe ensuite les fills confirmes depuis `/fills`.
- Les rejets IBKR explicites sont classes en `broker_error` et ne generent ni
  fill simule ni retry automatique.
- `core.reconciliation_log` conserve les controles IBKR/DuckDB.

## Deploiement VPS

Le VPS ne clone pas tout le repo dans `/opt/trader-ia`. Pour deploier depuis la
machine locale, copier seulement les services utiles puis reconstruire le stack
`yfinance`. Ne pas mettre IBKR dans un `docker-compose.override.yml` separe : le
compose principal doit rester la source de verite, sinon un redeploiement du
stack peut oublier `ibkr-gateway` et `ibkr-broker`.

```bash
# Sur la machine locale, depuis la racine du repo
scp -r services/ibkr-gateway services/ibkr-broker root@100.104.236.78:/opt/trader-ia/services/
scp infra/vps_hostinger_config/docker-compose.yfinance.yml root@100.104.236.78:/docker/yfinance/docker-compose.yml

# Sur le VPS
cd /docker/yfinance
grep -q '^IBKR_DRY_RUN=' .env || echo 'IBKR_DRY_RUN=true' >> .env
grep -q '^IBKR_SEND_DRY_RUN_TO_BROKER=' .env || echo 'IBKR_SEND_DRY_RUN_TO_BROKER=false' >> .env
grep -q '^IBKR_ACCOUNT_ID=' .env || echo 'IBKR_ACCOUNT_ID=' >> .env
grep -q '^IBKR_REQUIRE_PAPER_ACCOUNT=' .env || echo 'IBKR_REQUIRE_PAPER_ACCOUNT=true' >> .env
grep -q '^IBKR_PAPER_ACCOUNT_PREFIXES=' .env || echo 'IBKR_PAPER_ACCOUNT_PREFIXES=DU' >> .env
docker compose config --quiet
docker compose up -d --build yfinance-api yf-enrichment ibkr-gateway ibkr-broker
docker compose ps yfinance-api yf-enrichment ibkr-gateway ibkr-broker
curl -sS http://127.0.0.1:18080/health
```

Le gateway est publie uniquement sur `127.0.0.1:5000` du VPS. Pour le login IBKR
depuis la machine locale :

```bash
ssh -L 5000:127.0.0.1:5000 root@100.104.236.78
# puis ouvrir https://localhost:5000
```

Les task-runners n8n externes doivent exposer les variables IBKR aux Code nodes.
Sur le VPS actuel, cela passe par `/opt/trader-ia/n8n-task-runners.clean.json` :

- ajouter les variables IBKR et AG1-FX dans `allowed-env`;
- definir `N8N_BLOCK_RUNNER_ENV_ACCESS=false` dans `env-overrides`.

## Nodes n8n

Actions AG1-V3 :

- fichier code : `agents/trading-actions/AG1-V3-Portfolio manager/workflow/nodes/post_agent/07b_ibkr_send_orders.js`
- node importable : `agents/trading-actions/AG1-V3-Portfolio manager/workflow/nodes/post_agent/07b_ibkr_send_orders.node.json`
- deja cable dans le template et les variantes entre `7 - Validate & Enforce Safety`
  et `8 - Build DuckDB Bundle`.

Forex AG1-FX-V1 :

- fichier code : `agents/trading-forex/AG1-FX-V1-Portfolio manager/nodes/post_agent/11b_ibkr_send_orders_fx.py`
- node importable : `agents/trading-forex/AG1-FX-V1-Portfolio manager/nodes/post_agent/11b_ibkr_send_orders_fx.node.json`
- deja cable dans le template et les variantes entre `11 Validate Enforce Safety FX`
  et `12 Simulate Fills FX`.

## Sequence de Validation

Dry-run broker :

1. Garder `IBKR_DRY_RUN=true`.
2. Deployer `ibkr-gateway` et `ibkr-broker`.
3. Ouvrir le tunnel SSH vers le gateway et s'authentifier a IBKR.
4. Verifier `/health`.
5. Executer des runs n8n avec `IBKR_SEND_DRY_RUN_TO_BROKER=false`.
6. Optionnel : passer `IBKR_SEND_DRY_RUN_TO_BROKER=true` pour tester les appels
   broker sans ordre live.

Paper production Forex :

1. Verifier que le compte IBKR detecte commence par `DU`.
2. Activer `IBKR_DRY_RUN=false`, `IBKR_REQUIRE_PAPER_ACCOUNT=true` et
   `IBKR_PAPER_ACCOUNT_PREFIXES=DU`.
3. Garder un seul PM Forex actif par compte IBKR. En production actuelle : GPT
   actif, Grok/Gemini inactifs.
4. Lancer `AG1-FX-PF-V1` et verifier `core.reconciliation_log`.
5. Lancer un run AG1-FX GPT seulement si la reconciliation est `OK`.
6. Surveiller `core.orders.status`: `submitted` signifie ordre envoye en attente
   de fill IBKR; `filled` signifie fill confirme.

En paper/live, si un appel broker echoue, les nodes marquent l'ordre en erreur
et ne creent pas de fill simule pour cet ordre.
