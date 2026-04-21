# Déploiement VPS

Déploiement de référence sur un VPS Hostinger Linux. Deux stacks Docker Compose cohabitent :

- **Stack principale** : `infra/vps_hostinger_config/docker-compose.yml` — `traefik`, `n8n`, `task-runners` ×3, `yfinance-api`, `yf-enrichment`, `trading-dashboard`, `toolbox`.
- **Stack Qdrant** : `infra/vps_hostinger_config/docker-compose.qdrant.yml` — `qdrant` isolé, rejoint le réseau `web` via alias.

> ⚠️ **Arborescence GitHub ≠ arborescence VPS.** Ce dossier `infra/vps_hostinger_config/` est la *source* du compose côté repo. Sur le VPS, le compose est copié sous `/opt/trader-ia/` et les builds pointent vers `../../services/yfinance-api` et `../../services/yf-enrichment-service` — autrement dit, sur le VPS il faut soit cloner ce repo dans `/opt/trader-ia/` et lancer `docker compose` depuis `/opt/trader-ia/infra/vps_hostinger_config/`, soit copier à la main le compose **et** les dossiers `services/yfinance-api/` + `services/yf-enrichment-service/` à côté (voir §3).

## 1. Prérequis VPS

```bash
# Docker + Compose v2 préinstallés
docker --version
docker compose version

# Réseaux et volumes externes (à créer une seule fois)
docker network create web
docker network create traefik_proxy
docker volume create traefik_data
docker volume create n8n_data
docker volume create qdrant_data
docker volume create yfinance_data
docker volume create runner_pydeps
```

## 2. Arborescence VPS attendue

```
/opt/
├── trader-ia/
│   ├── docker-compose.yml              # symlink ou copie depuis vps_hostinger_config/
│   ├── docker-compose.qdrant.yml
│   ├── .env                            # jamais committé
│   ├── n8n-task-runners.clean.json     # config runner (clean)
│   ├── duckdb_home/                    # home DuckDB partagé runners
│   └── AG1-V2-EXPORT/                  # pack read-only monté sur n8n
├── traefik/
│   └── secrets/
├── traefik_logs/                       # logs accès traefik
├── yf-enrichment-v1/                   # daily_enrichment.py
├── trading-dashboard/
│   ├── app/
│   ├── secrets/
│   └── requirements.txt
└── local-files/                        # monté sur n8n ET runners (lecture-écriture)
    ├── duckdb/                         # bases DuckDB produites par les workflows
    │   ├── ag1_v3.duckdb
    │   ├── ag1_v3_chatgpt52.duckdb
    │   ├── ag1_v3_grok41_reasoning.duckdb
    │   ├── ag1_v3_gemini30_pro.duckdb
    │   ├── ag2_v3.duckdb
    │   ├── ag3_v2.duckdb
    │   ├── ag4_v3.duckdb
    │   ├── ag4_spe_v2.duckdb
    │   └── yf_enrichment_v1.duckdb
    └── AG1-V3-EXPORT/                  # pack DuckDB writer + schema
        ├── nodes/post_agent/duckdb_writer.py
        └── sql/portfolio_ledger_schema_v2.sql
```

## 3. Première installation

Deux options selon la façon dont le repo vit sur le VPS.

### 3.a Clone direct (recommandé depuis la restructure)

```bash
# Cloner le repo directement sous /opt/trader-ia
sudo git clone https://github.com/nicolaslhote88/Trader_IA.git /opt/trader-ia
cd /opt/trader-ia/infra/vps_hostinger_config

# Configurer l'env
cp .env.example .env
vim .env   # compléter toutes les valeurs

# Préparer le fichier runners (hors repo par historique)
cp n8n-task-runners.json /opt/trader-ia/n8n-task-runners.clean.json
# (adapter le chemin si le nom exact diffère)

# Build + up stack principale (les contextes ../../services/... résolvent dans le repo)
docker compose build
docker compose up -d

# Up Qdrant
docker compose -f docker-compose.qdrant.yml up -d
```

### 3.b Copie manuelle (legacy)

```bash
cd /opt/trader-ia

# Copier la stack, les services Dockerfile et configurer
cp /chemin/vers/repo/infra/vps_hostinger_config/docker-compose.yml .
cp /chemin/vers/repo/infra/vps_hostinger_config/docker-compose.qdrant.yml .
cp /chemin/vers/repo/infra/vps_hostinger_config/.env.example .env
cp -r /chemin/vers/repo/services/yfinance-api .     # requis par le build yfinance-api
cp -r /chemin/vers/repo/services/yf-enrichment-service .  # requis par le build yf-enrichment
vim .env

# Dans ce cas, il faut corriger les contextes du compose pour qu'ils pointent
# en relatif à /opt/trader-ia/ (par ex. context: ./yfinance-api).
```

### 3.c Vérification

```bash
docker compose ps
curl -I https://${SUBDOMAIN}.${DOMAIN_NAME}/
curl http://localhost:8080/health          # yfinance-api (via le réseau web)
```

## 4. Mises à jour

```bash
# Rebuild + redeploy d'un service
docker compose build n8n
docker compose up -d n8n

# Logs
docker compose logs -f n8n
docker compose logs -f task-runners
```

## 5. Nettoyage

```bash
# n8n purge auto ses exécutions à 72h (EXECUTIONS_DATA_MAX_AGE=72)
# et conserve max 5000 runs (EXECUTIONS_DATA_PRUNE_MAX_COUNT=5000).
# Au boot: VACUUM SQLite activé (DB_SQLITE_VACUUM_ON_STARTUP=true).

# Logs Traefik (accesslog) : /opt/traefik_logs/access.log — logrotate recommandé.
```

## 6. Points d'attention

- **Ne pas renommer** `AG1-V3-EXPORT`, `AG1-V2-EXPORT` : les chemins sont en dur dans n8n + dans `09_upsert_run_bundle_duckdb.code.py` (variable `STATIC_WRITER_PATHS`).
- **DNS** : Traefik utilise `1.1.1.1` + `8.8.8.8` pour éviter les rate-limits ACME (définis dans `docker-compose.yml`).
- **Network `web` + `traefik_proxy`** : les deux doivent exister. `traefik_proxy` sert aux services routés par Traefik sans ports publics directs.
