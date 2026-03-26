# YF Enrichment V1 (daily)

Objectif: sortir les appels YFinance du dashboard et pre-calculer une base DuckDB quotidienne pour la vue consolidee.

Le job:
- lit les symboles depuis `ag2_v2.duckdb` table `universe` (ou `--symbols`)
- appelle `yfinance-api` endpoints `/quote`, `/options`, `/calendar`
- stocke les resultats dans `yf_enrichment_v1.duckdb`
- expose une vue `v_latest_symbol_enrichment` consommee par le dashboard

## Fichiers
- `yf-enrichment-v1/daily_enrichment.py`
- `yf-enrichment-v1/YF-ENRICH-V1-daily-workflow.json` (workflow n8n via HTTP)
- `yf-enrichment-service/` (micro-service FastAPI qui execute le job)

## Tables creees
- `run_log`
- `yf_symbol_enrichment_history`
- `v_latest_symbol_enrichment`

## Execution manuelle

```bash
python yf-enrichment-v1/daily_enrichment.py \
  --yf-enrich-db-path /files/duckdb/yf_enrichment_v1.duckdb \
  --ag2-db-path /files/duckdb/ag2_v2.duckdb \
  --yf-api-url http://yfinance-api:8080
```

## Variables utiles
- `YF_ENRICH_DB_PATH` (defaut: `/files/duckdb/yf_enrichment_v1.duckdb`)
- `AG2_DUCKDB_PATH` (defaut: `/files/duckdb/ag2_v2.duckdb`)
- `YFINANCE_API_URL` (defaut: `http://yfinance-api:8080`)
- `YF_OPTIONS_RECHECK_DAYS` (defaut: `7`)
- `YF_ENRICH_QUOTE_CHUNK` (defaut: `80`)
- `YF_ENRICH_OPTIONS_TARGET_DAYS` (defaut: `30`)
- `YF_ENRICH_TIMEOUT_SEC` (defaut: `14`)
- `YF_ENRICH_MAX_SYMBOLS` (defaut: `0` = pas de limite)

## Optimisation FR (options vides)

Si `NO_EXPIRATIONS_AVAILABLE` est detecte, le job memorise l'etat et saute les appels options pendant `YF_OPTIONS_RECHECK_DAYS` jours.
Ca evite de repayer des appels inutiles quotidiennement sur les titres non couverts.

## Integration n8n (daily) - recommandee

Architecture robuste:
- `n8n` declenche un appel HTTP `POST http://yf-enrichment:8081/run`
- le micro-service `yf-enrichment` execute `daily_enrichment.py` dans un container Python slim
- le dashboard lit ensuite uniquement `v_latest_symbol_enrichment`

Workflow n8n importable:
- `yf-enrichment-v1/YF-ENRICH-V1-daily-workflow.json`

## Service Docker `yf-enrichment`

Construire et demarrer:

```bash
docker compose build yf-enrichment
docker compose up -d yf-enrichment
```

Verifier la sante du service:

```bash
curl http://localhost:8081/health
```

Declencher un run manuellement:

```bash
curl -X POST http://localhost:8081/run \
  -H "Content-Type: application/json" \
  -d '{}'
```
