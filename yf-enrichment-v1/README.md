# YF Enrichment V1 (daily)

Objectif: sortir les appels YFinance du dashboard et pre-calculer une base DuckDB quotidienne pour la vue consolidee.

Le job:
- lit les symboles depuis `ag2_v2.duckdb` table `universe` (ou `--symbols`)
- appelle `yfinance-api` endpoints `/quote`, `/options`, `/calendar`
- stocke les resultats dans `yf_enrichment_v1.duckdb`
- expose une vue `v_latest_symbol_enrichment` consommee par le dashboard

## Fichiers
- `yf-enrichment-v1/daily_enrichment.py`

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

## Integration n8n (daily)

Option simple:
- Noeud `Execute Command` quotidien (cron 1x/jour)
- Commande:

```bash
python /workspace/yf-enrichment-v1/daily_enrichment.py
```

Le dashboard lit ensuite uniquement `v_latest_symbol_enrichment`.

