# Execution IBKR

Ce document decrit le branchement IBKR Client Portal API ajoute au projet.

## Architecture

```
n8n Code node 07b / 11b
  -> http://ibkr-broker:8080
  -> https://ibkr-gateway:5000/v1/api
  -> IBKR
```

Le `ibkr-broker` encapsule l'API IBKR et garde `IBKR_DRY_RUN=true` par defaut.
Les nodes n8n restent sandbox-only en dry-run, sauf si
`IBKR_SEND_DRY_RUN_TO_BROKER=true` est defini pour tester le broker sans ordre
live.

References IBKR utilisees :

- Web API orders: https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-doc/
- Gateway / staging notes: https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-staging/

## Services Docker

- `services/ibkr-gateway/` telecharge et lance `clientportal.gw`.
- `services/ibkr-broker/` expose `/health`, `/orders/fx`, `/orders/equity`,
  `/fills`, `/positions` et `/auth/tickle`.
- Sur le VPS actuel, les deux services sont integres directement dans le stack
  `/docker/yfinance`. Le fichier de reference a copier tel quel comme compose
  principal est `infra/vps_hostinger_config/docker-compose.yfinance.yml`.
- `infra/vps_hostinger_config/docker-compose.yml` garde aussi la definition pour
  un deploiement complet du repo, mais ce n'est pas le mode utilise sur le VPS.
- Les nodes n8n lisent `IBKR_BROKER_URL`, `IBKR_DRY_RUN` et
  `IBKR_SEND_DRY_RUN_TO_BROKER`. Sans variable explicite, ils restent en
  dry-run et utilisent `http://ibkr-broker:8080`.

## Deploiement VPS actuel

Le VPS ne clone pas tout le repo dans `/opt/trader-ia`. Pour deploier depuis la
machine locale, copier seulement les services utiles puis reconstruire le stack
`yfinance`. Ne pas mettre IBKR dans un `docker-compose.override.yml` separe :
le compose principal doit rester la source de verite, sinon un redeploiement du
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
docker compose config --quiet
docker compose up -d --build yfinance-api yf-enrichment ibkr-gateway ibkr-broker
docker compose ps yfinance-api yf-enrichment ibkr-gateway ibkr-broker
curl -sS http://127.0.0.1:18080/health
```

Le gateway est publie uniquement sur `127.0.0.1:5000` du VPS. Pour le login
IBKR depuis la machine locale :

```bash
ssh -L 5000:127.0.0.1:5000 root@100.104.236.78
# puis ouvrir https://localhost:5000
```

## Nodes n8n prets a inserer

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

## Sequence de validation

1. Garder `IBKR_DRY_RUN=true`.
2. Deployer `ibkr-gateway` et `ibkr-broker`.
3. Ouvrir le tunnel SSH vers le gateway et s'authentifier a IBKR.
4. Verifier `/health`.
5. Executer au moins 5 runs n8n avec `IBKR_SEND_DRY_RUN_TO_BROKER=false`.
6. Optionnel : passer `IBKR_SEND_DRY_RUN_TO_BROKER=true` pour tester les appels
   broker sans ordre live.
7. Passer `IBKR_DRY_RUN=false` uniquement apres validation manuelle.

En live, si un appel broker echoue, les nodes marquent l'ordre en erreur et ne
creent pas de fill simule pour cet ordre.
