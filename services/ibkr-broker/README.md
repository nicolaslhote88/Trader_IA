# ibkr-broker — Passerelle IBKR Client Portal API

Microservice FastAPI qui sert d'intermédiaire entre les workflows n8n et l'API IBKR.

## Architecture

```
n8n (node 11b FX / node 07b Actions)
        │  HTTP POST
        ▼
ibkr-broker:8080   (FastAPI — réseau Docker interne)
        │  HTTPS
        ▼
ibkr-gateway:5000  (clientportal.gw IBKR — réseau Docker interne)
        │  IBKR protocol
        ▼
IBKR Servers
```

Les nodes n8n ne contactent pas le broker quand `IBKR_DRY_RUN=true`, sauf si
`IBKR_SEND_DRY_RUN_TO_BROKER=true`. Le broker garde aussi une securite dry-run :
`IBKR_DRY_RUN=true` retourne un statut `dry_run` sans envoyer d'ordre live.

## Endpoints

| Méthode | Endpoint          | Description |
|---------|-------------------|-------------|
| GET     | `/health`         | Statut session IBKR |
| POST    | `/orders/fx`      | Envoyer ordres FX (IDEALPRO) |
| POST    | `/orders/equity`  | Envoyer ordres actions/ETF |
| GET     | `/fills`          | Fills récents |
| GET     | `/positions`      | Positions actuelles |
| POST    | `/auth/tickle`    | Keepalive manuel |

## Variables d'environnement

| Variable           | Défaut                      | Description |
|--------------------|-----------------------------|-------------|
| `IBKR_GATEWAY_URL` | `https://ibkr-gateway:5000` | URL CPAPI gateway |
| `IBKR_DRY_RUN`     | `true`                      | **true** = log only, **false** = live |
| `IBKR_SSL_VERIFY`  | `false`                     | Vérifier le certificat SSL du gateway |
| `IBKR_ACCOUNT_ID`  | *(auto-détecté)*            | ID compte IBKR |
| `IBKR_SEND_DRY_RUN_TO_BROKER` | `false` | Variable lue par les nodes n8n, pas par le broker. Permet de tester le chemin HTTP en dry-run. |

## Démarrage et authentification

### 1. Démarrer les containers

```bash
cd infra/vps_hostinger_config
docker compose up -d ibkr-gateway ibkr-broker
```

### 2. Authentification IBKR (une seule fois, puis keepalive automatique)

```bash
# Sur ta machine locale :
ssh -L 5000:localhost:5000 user@vps_ip

# Dans ton navigateur :
# https://localhost:5000
# → Login avec credentials IBKR
```

Après login, le `ibkr-broker` envoie un tickle toutes les 55 secondes pour maintenir la session.

### 3. Vérifier la santé

```bash
curl http://localhost:8080/health
# → { "dry_run": true, "authenticated": true, ... }
```

### 4. Passer en mode live

Dans `.env` sur le VPS :
```bash
IBKR_DRY_RUN=false
IBKR_ACCOUNT_ID=UxxxXXXXX
```

Puis :
```bash
docker compose up -d ibkr-broker
```

**⚠️ Ne passer à `false` qu'après avoir validé au moins 5 runs en dry-run.**

## Notes API IBKR

Le broker envoie les ordres au format Web API actuel : `POST
/v1/api/iserver/account/{accountId}/orders` avec un objet `{ "orders": [...] }`.
Les confirmations IBKR renvoyant un `id` sont confirmees via
`/v1/api/iserver/reply/{id}`.

Reference : https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-doc/

## Format des requêtes

### FX
```json
POST /orders/fx
{
  "run_id": "run_2026-05-05T09:30:00",
  "orders": [
    {
      "pair": "EURUSD",
      "side": "buy_base",
      "size_lots": 0.1,
      "order_id": "550e8400-e29b-41d4-a716-446655440000",
      "order_type": "MKT"
    }
  ]
}
```

### Actions
```json
POST /orders/equity
{
  "run_id": "run_2026-05-05T09:15:00",
  "orders": [
    {
      "symbol": "MC.PA",
      "side": "BUY",
      "quantity": 2,
      "order_id": "550e8400-e29b-41d4-a716-446655440001",
      "order_type": "MKT"
    }
  ]
}
```
