# Variables d'environnement

Cette page décrit les variables attendues côté VPS. Le fichier template est `infra/vps_hostinger_config/.env.example`.

## Traefik / TLS

| Variable | Rôle |
|---|---|
| `SSL_EMAIL` | Email utilisé par Let's Encrypt pour le challenge ACME. |
| `GENERIC_TIMEZONE` | Timezone appliquée aux conteneurs (défaut `Europe/Paris`). |

## n8n

| Variable | Rôle |
|---|---|
| `SUBDOMAIN` | Sous-domaine n8n (ex. `n8n`). |
| `DOMAIN_NAME` | Domaine racine (ex. `trader-ia.com`). |
| `N8N_RUNNERS_AUTH_TOKEN` | Token partagé entre `n8n` et `task-runners`. Doit être identique. Valeur forte obligatoire. |

## Qdrant

Qdrant tourne dans sa propre stack (`docker-compose.qdrant.yml`).

| Variable | Rôle |
|---|---|
| `QDRANT_API_KEY` | Clé API Qdrant. Utilisée par `n8n` pour les nœuds `Qdrant Delete` / `Qdrant Upsert`. |

## Services externes

| Variable | Rôle |
|---|---|
| `TRANSCRIPT_API_BASE` | URL de base du service de transcripts consommé par les analystes AG4. |

## Google Sheets (héritage / dashboard)

| Variable | Rôle |
|---|---|
| `GOOGLE_SHEET_ID` | ID du Google Sheet utilisé par le `trading-dashboard` Streamlit. |

Le fichier de compte de service Google doit être monté à `/secrets/service_account.json` côté `trading-dashboard` (déjà câblé dans le docker-compose).

## Dashboard Streamlit

| Variable | Rôle |
|---|---|
| `DASHBOARD_DOMAIN` | Nom d'hôte public du dashboard (ex. `dashboard.trader-ia.com`). |
| `DASHBOARD_BASIC_AUTH` | Entrée Basic Auth au format `user:hash_apache`. Les `$` doivent être échappés en `$$` pour Docker Compose. |

Génération du hash :

```bash
htpasswd -nb admin 'motdepasse' | sed -e s/\\$/\\$\\$/g
```

## Variables internes au service `trading-dashboard`

Définies dans le docker-compose (pas dans le `.env`). Le volume `/local-files/duckdb:/files/duckdb:ro` les rend toutes lisibles :

- `AG1_CHATGPT52_DUCKDB_PATH=/files/duckdb/ag1_v3_chatgpt52.duckdb`
- `AG1_GROK41_REASONING_DUCKDB_PATH=/files/duckdb/ag1_v3_grok41_reasoning.duckdb`
- `AG1_GEMINI30_PRO_DUCKDB_PATH=/files/duckdb/ag1_v3_gemini30_pro.duckdb`
- `AG2_DUCKDB_PATH=/files/duckdb/ag2_v3.duckdb`
- `AG3_DUCKDB_PATH=/files/duckdb/ag3_v2.duckdb`
- `AG4_DUCKDB_PATH=/files/duckdb/ag4_v3.duckdb`
- `AG4_SPE_DUCKDB_PATH=/files/duckdb/ag4_spe_v2.duckdb`
- `AG4_FOREX_DUCKDB_PATH=/files/duckdb/ag4_forex_v1.duckdb` *(alimente la page « Forex P&L (LLM x Paire) » — couverture news taguées FX)*
- `YF_ENRICH_DUCKDB_PATH=/files/duckdb/yf_enrichment_v1.duckdb`

## Variables internes au système Forex AG1-FX-V1

Définies dans `infra/vps_hostinger_config/docker-compose.yml` pour `n8n`, `task-runners` et `trading-dashboard` :

- `AG1_FX_V1_CHATGPT52_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_chatgpt52.duckdb`
- `AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb`
- `AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH=/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb`
- `AG2_FX_V1_DUCKDB_PATH=/files/duckdb/ag2_fx_v1.duckdb`
- `AG4_FX_V1_DUCKDB_PATH=/files/duckdb/ag4_fx_v1.duckdb`
- `AG1_FX_V1_WRITER_PATH=/files/AG1-FX-V1-EXPORT/nodes/post_agent/duckdb_writer.py`
- `AG1_FX_V1_LEDGER_SCHEMA_PATH=/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql`

## Variables internes au service n8n

Ces variables sont déjà définies dans le docker-compose — elles ne sont **pas** dans le .env :

- `AG1_DUCKDB_PATH=/files/duckdb/ag1_v3.duckdb`
- `AG1_DUCKDB_WRITER_PATH=/files/AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py`
- `AG1_LEDGER_SCHEMA_PATH=/files/AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql`
- `EXECUTIONS_DATA_MAX_AGE=72`
- `EXECUTIONS_DATA_SAVE_ON_SUCCESS=none`
- `EXECUTIONS_DATA_SAVE_MANUAL_EXECUTIONS=false`
- `EXECUTIONS_DATA_PRUNE_MAX_COUNT=5000`
- `N8N_PROXY_HOPS=1`
- `DB_SQLITE_VACUUM_ON_STARTUP=true`

## Variables internes aux task-runners

- `N8N_RUNNERS_TASK_BROKER_URI=http://n8n:5679`
- `N8N_RUNNERS_MAX_CONCURRENCY=4`
- `N8N_RUNNERS_LAUNCHER_LOG_LEVEL=debug`

Les mêmes `AG1_DUCKDB_*` sont répliquées côté runners pour accès aux `.duckdb`.
