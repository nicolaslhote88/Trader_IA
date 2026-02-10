# yfinance-api v2.0.0 - Migration Guide

## Problème résolu

La v1 avait un bug critique dans le FAST PATH :

```python
# v1 - BUG: retourne TOUJOURS le cache, même s'il a 5+ jours
if cached_ok and not force_refresh:
    return _respond_from_df(cached_df, False, None, "cache")
```

Le cache était servi indéfiniment sans vérification de fraîcheur, avec `stale: false`.

## Changements clés v1 → v2

### 1. Détection de fraîcheur du cache (Cache TTL)

Chaque intervalle a un TTL configurable. Si la dernière barre est plus vieille que le TTL, le cache est considéré périmé et une requête Yahoo est faite.

| Intervalle | TTL par défaut |
|------------|---------------|
| 1m         | 15 min        |
| 5m         | 30 min        |
| 15m        | 1h            |
| 1h         | 2h            |
| 1d         | 4h + weekend  |
| 1wk        | 48h           |

### 2. Cooldown par symbole (et non global)

En v1, un seul symbole en erreur bloquait TOUS les symboles pendant 30min-6h.
En v2, chaque paire symbole/intervalle a son propre état de cooldown.

### 3. Classification des erreurs

| Type erreur     | Cooldown           |
|----------------|--------------------|
| Rate limit 429 | 30 min (escalade)  |
| Réseau         | 5 min (escalade)   |
| Données vides  | Aucun (si cache)   |
| Autre          | 5 min (escalade)   |

### 4. Réponses vides ≠ erreurs

En dehors des heures de marché, Yahoo renvoie souvent des données vides.
La v2 ne déclenche PAS de cooldown dans ce cas si le cache existe.

### 5. Nouveaux endpoints

- `GET /cache/status?symbol=AAPL&interval=1d` - État du cache sans fetch
- `GET /cooldown/reset` - Reset tous les cooldowns
- `GET /cooldown/reset?symbol=AAPL` - Reset cooldown pour un symbole
- `GET /cooldown/reset?symbol=AAPL&interval=1d` - Reset cooldown spécifique

## Variables d'environnement

| Variable | v1 défaut | v2 défaut | Description |
|----------|-----------|-----------|-------------|
| `YF_COOLDOWN_BASE_SEC` | 900 | 300 | Cooldown de base |
| `YF_COOLDOWN_MAX_SEC` | 14400 | 3600 | Cooldown maximum |
| `YF_COOLDOWN_RATELIMIT_SEC` | N/A | 1800 | Cooldown spécifique 429 |
| `YF_MIN_SECONDS_BETWEEN_CALLS` | 2 | 5 | Rate limit global |
| `YF_CACHE_TTL_JSON` | N/A | {} | Override TTL par intervalle (JSON) |

## docker-compose.yml - section à modifier

```yaml
  yfinance-api:
    build:
      context: ./yfinance-api
      dockerfile: Dockerfile
    container_name: yfinance-api
    environment:
      - TZ=Europe/Paris
      - YF_DATA_DIR=/data
      - YF_INIT_LOOKBACK_DAYS=400
      - YF_OVERLAP_BARS=3
      - YF_COOLDOWN_BASE_SEC=300
      - YF_COOLDOWN_MAX_SEC=3600
      - YF_COOLDOWN_RATELIMIT_SEC=1800
      - YF_MIN_SECONDS_BETWEEN_CALLS=5
    dns:
      - 1.1.1.1
      - 8.8.8.8
    restart: unless-stopped
    volumes:
      - yfinance_data:/data
    networks:
      - web
```

## Déploiement

```bash
# Sur le VPS, copier le dossier yfinance-api/ puis :
cd /root  # ou le répertoire de votre docker-compose
docker compose build yfinance-api
docker compose up -d yfinance-api

# Vérifier
curl http://yfinance-api:8080/health
curl http://yfinance-api:8080/cache/status?symbol=AAPL&interval=1d

# Si un symbole est en cooldown injustifié :
curl http://yfinance-api:8080/cooldown/reset?symbol=AAPL
```

## Compatibilité

L'API `/history` conserve exactement les mêmes paramètres et le même format de réponse.
Le champ `source` peut maintenant valoir `"cache_stale"` ou `"cache_checked"` en plus des valeurs existantes.
