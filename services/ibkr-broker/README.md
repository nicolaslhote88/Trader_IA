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
| GET     | `/marketdata/fx/snapshot?pairs=EURUSD,USDJPY` | Snapshot FX bid/ask/mid/spread via IBKR market data |
| POST    | `/orders/fx`      | Envoyer ordres FX (IDEALPRO) |
| POST    | `/orders/equity`  | Envoyer ordres actions/ETF |
| GET     | `/fills`          | Fills récents |
| GET     | `/positions`      | Positions actuelles |
| POST    | `/auth/tickle`    | Keepalive manuel |
| POST    | `/auth/initialize` | Reinitialise la session brokerage si la session Gateway/SSO est encore valide |
| POST    | `/auth/recover` | Lance tickle + reinit puis renvoie l'action operateur si IBKR demande un relogin |
| GET     | `/auth/operator-action` | Instructions relogin/2FA et statut credentials assistes |

## Variables d'environnement

| Variable           | Défaut                      | Description |
|--------------------|-----------------------------|-------------|
| `IBKR_GATEWAY_URL` | `https://ibkr-gateway:5000` | URL CPAPI gateway |
| `IBKR_DRY_RUN`     | `true`                      | **true** = log only, **false** = live |
| `IBKR_SSL_VERIFY`  | `false`                     | Vérifier le certificat SSL du gateway |
| `IBKR_ACCOUNT_ID`  | *(auto-détecté)*            | ID compte IBKR |
| `IBKR_KEEPALIVE_INTERVAL_SECONDS` | `55` | Frequence du superviseur de session |
| `IBKR_AUTO_REAUTH_ENABLED` | `true` | Tente `/iserver/auth/ssodh/init` si la session brokerage n'est plus authentifiee mais reste connectee |
| `IBKR_AUTO_REAUTH_COMPETE` | `false` | Si `true`, peut deconnecter une session IBKR concurrente du meme username |
| `IBKR_ALERT_WEBHOOK_URL` | *(vide)* | Webhook optionnel appele quand `manual_login_required=true` |
| `IBKR_ALERT_COOLDOWN_SECONDS` | `900` | Cooldown minimal entre deux alertes relogin |
| `IBKR_LOGIN_URL` | `https://localhost:5000` | URL affichee dans l'action operateur |
| `IBKR_LOGIN_TUNNEL_COMMAND` | tunnel VPS | Commande affichee dans l'action operateur |
| `IBKR_ASSISTED_LOGIN_ENABLED` | `false` | Active le statut "credentials assistes presents" dans `/health` |
| `IBKR_USERNAME` / `IBKR_PASSWORD` | *(vide)* | Credentials optionnels pour un flux assiste externe. Ne pas exposer dans Git. |
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

Après login, le `ibkr-broker` envoie un tickle toutes les 55 secondes pour
maintenir la session. Si IBKR retourne `connected=true` mais
`authenticated=false`, le broker tente automatiquement
`/iserver/auth/ssodh/init`. Si le Gateway/SSO a totalement expire, `/health`
indique `session_monitor.manual_login_required=true` : il faut alors rouvrir
`https://localhost:5000` via le tunnel et valider le login/2FA.

Le broker expose aussi :

```bash
curl -sS -X POST http://localhost:8080/auth/recover
curl -sS http://localhost:8080/auth/operator-action
```

`/auth/recover` tente uniquement une recuperation non destructive. Si IBKR
demande un relogin, la reponse contient `operator_action` avec la commande de
tunnel, l'URL de login, et l'etat du mode assiste. Si `IBKR_ALERT_WEBHOOK_URL`
est defini, la meme information est envoyee au webhook avec un cooldown.

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

IBKR impose le Secure Login System / 2FA sur le Client Portal Gateway. La
reinitialisation automatique couvre uniquement le cas ou la session Gateway/SSO
est encore valide mais ou la session brokerage `/iserver` est tombee.

Des credentials peuvent etre injectes via l'environnement pour un flux assiste
externe (`IBKR_USERNAME`/`IBKR_PASSWORD`, ou `IBEAM_ACCOUNT`/`IBEAM_PASSWORD` si
un wrapper type IBeam est branche). Cela reduit la saisie manuelle, mais ne doit
pas etre considere comme un contournement garanti du 2FA. Le broker ne journalise
jamais ces secrets et ne les renvoie pas dans `/health`; il indique seulement si
les variables sont configurees.

Le broker envoie les ordres au format Web API actuel : `POST
/v1/api/iserver/account/{accountId}/orders` avec un objet `{ "orders": [...] }`.
Les confirmations IBKR renvoyant un `id` sont confirmees via
`/v1/api/iserver/reply/{id}`.

Reference : https://www.interactivebrokers.com/campus/ibkr-api-page/webapi-doc/

## Données de marché FX

AG2-FX peut appeler :

```bash
curl "http://localhost:8080/marketdata/fx/snapshot?pairs=EURUSD,USDJPY"
```

Le endpoint retourne `bid`, `ask`, `mid`, `spread`, `spread_pct` et la disponibilité
IBKR (`6509`) quand l'abonnement market data du compte le permet. Les ordres restent
protégés par `IBKR_DRY_RUN`; ce endpoint ne passe aucun ordre.

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
