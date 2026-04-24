#!/usr/bin/env bash
set -euo pipefail

N8N_CONTAINER="${N8N_CONTAINER:-}"
HOST_DUCKDB_DIR="${HOST_DUCKDB_DIR:-/local-files/duckdb}"
HOST_FILES_DIR="${HOST_FILES_DIR:-/local-files}"
CONTAINER_DUCKDB_DIR="${CONTAINER_DUCKDB_DIR:-/files/duckdb}"

if [[ -z "$N8N_CONTAINER" ]]; then
  if docker ps --format '{{.Names}}' | grep -qx 'root-n8n-1'; then
    N8N_CONTAINER="root-n8n-1"
  else
    N8N_CONTAINER="$(docker ps --format '{{.Names}}' | grep -E '(^|-)n8n(-|$)' | head -n 1 || true)"
  fi
fi

if [[ -z "$N8N_CONTAINER" ]]; then
  echo "ERROR: conteneur n8n introuvable. Relance avec N8N_CONTAINER=nom_du_conteneur bash $0" >&2
  exit 1
fi

if [[ ! -f "$HOST_DUCKDB_DIR/ag4_v3.duckdb" ]]; then
  echo "ERROR: base host introuvable: $HOST_DUCKDB_DIR/ag4_v3.duckdb" >&2
  exit 1
fi

echo "== AG4 geo/forex migration =="
echo "n8n container : $N8N_CONTAINER"
echo "host duckdb   : $HOST_DUCKDB_DIR"

BACKUP="$HOST_DUCKDB_DIR/ag4_v3.duckdb.bak_$(date +%Y%m%d_%H%M%S)"
cp -a "$HOST_DUCKDB_DIR/ag4_v3.duckdb" "$BACKUP"
echo "backup OK     : $BACKUP"

mkdir -p "$HOST_FILES_DIR/Trader_IA/infra/config/sources"
cat > "$HOST_FILES_DIR/Trader_IA/infra/config/sources/fx_sources.yaml" <<'YAML'
sources:
  - id: forexlive_main
    type: rss
    url: https://www.forexlive.com/feed/news
    tier: A
    enabled: true

  - id: dailyfx_analysis
    type: rss
    url: https://www.dailyfx.com/feeds/market-news
    tier: A
    enabled: false

  - id: fxstreet_news
    type: rss
    url: https://www.fxstreet.com/rss/news
    tier: A
    enabled: false

  - id: investing_econ_calendar
    type: api
    url: https://api.investing.com/api/financialdata/economic-calendar
    tier: A
    enabled: false
    params:
      countries: [US, EU, JP, GB, CH, AU, CA, NZ]
      importance: [2, 3]

  - id: bis_press
    type: rss
    url: https://www.bis.org/rss/press.xml
    tier: S
    enabled: false

  - id: fed_statements
    type: rss
    url: https://www.federalreserve.gov/feeds/press_monetary.xml
    tier: S
    enabled: false

  - id: ecb_press
    type: rss
    url: https://www.ecb.europa.eu/rss/press.html
    tier: S
    enabled: false

  - id: boj_statements
    type: rss
    url: https://www.boj.or.jp/en/rss/whatsnew.xml
    tier: S
    enabled: false
YAML
echo "config FX OK  : $HOST_FILES_DIR/Trader_IA/infra/config/sources/fx_sources.yaml"

docker exec -i \
  -e CONTAINER_DUCKDB_DIR="$CONTAINER_DUCKDB_DIR" \
  "$N8N_CONTAINER" python - <<'PY'
import os
import duckdb

db_dir = os.environ.get("CONTAINER_DUCKDB_DIR", "/files/duckdb")
ag4_path = f"{db_dir}/ag4_v3.duckdb"
fx_path = f"{db_dir}/ag4_forex_v1.duckdb"

ag4_sql = [
    "ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_region VARCHAR",
    "ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_asset_class VARCHAR",
    "ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_magnitude VARCHAR",
    "ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS impact_fx_pairs VARCHAR",
    "ALTER TABLE main.news_history ADD COLUMN IF NOT EXISTS tagger_version VARCHAR",
    "CREATE INDEX IF NOT EXISTS idx_news_impact_asset_class ON main.news_history(impact_asset_class)",
    "CREATE INDEX IF NOT EXISTS idx_news_impact_region ON main.news_history(impact_region)",
    "CREATE INDEX IF NOT EXISTS idx_news_tagger_version ON main.news_history(tagger_version)",
]

fx_sql = [
    "CREATE SCHEMA IF NOT EXISTS main",
    """
    CREATE TABLE IF NOT EXISTS main.fx_news_history (
        dedupe_key VARCHAR PRIMARY KEY,
        event_key VARCHAR,
        run_id VARCHAR,
        origin VARCHAR,
        canonical_url VARCHAR,
        published_at TIMESTAMP,
        title VARCHAR,
        source VARCHAR,
        source_tier VARCHAR,
        snippet VARCHAR,
        impact_region VARCHAR,
        impact_magnitude VARCHAR,
        impact_fx_pairs VARCHAR,
        currencies_bullish VARCHAR,
        currencies_bearish VARCHAR,
        regime VARCHAR,
        theme VARCHAR,
        urgency DOUBLE,
        confidence DOUBLE,
        impact_score INTEGER,
        fx_narrative VARCHAR,
        fx_directional_hint VARCHAR,
        tagger_version VARCHAR,
        first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP,
        analyzed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fxnh_published ON main.fx_news_history(published_at)",
    "CREATE INDEX IF NOT EXISTS idx_fxnh_magnitude ON main.fx_news_history(impact_magnitude)",
    "CREATE INDEX IF NOT EXISTS idx_fxnh_pairs ON main.fx_news_history(impact_fx_pairs)",
    "CREATE INDEX IF NOT EXISTS idx_fxnh_origin ON main.fx_news_history(origin)",
    """
    CREATE TABLE IF NOT EXISTS main.fx_macro (
        run_id VARCHAR,
        as_of TIMESTAMP,
        market_regime VARCHAR,
        drivers VARCHAR,
        confidence DOUBLE,
        usd_bias DOUBLE,
        eur_bias DOUBLE,
        jpy_bias DOUBLE,
        gbp_bias DOUBLE,
        chf_bias DOUBLE,
        aud_bias DOUBLE,
        cad_bias DOUBLE,
        nzd_bias DOUBLE,
        bias_json VARCHAR,
        source_window_days INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (run_id, as_of)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS main.fx_pairs (
        id VARCHAR PRIMARY KEY,
        run_id VARCHAR,
        pair VARCHAR,
        symbol_internal VARCHAR,
        directional_bias VARCHAR,
        rationale VARCHAR,
        confidence DOUBLE,
        urgent_event_window BOOLEAN,
        as_of TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fxp_pair ON main.fx_pairs(pair)",
    "CREATE INDEX IF NOT EXISTS idx_fxp_asof ON main.fx_pairs(as_of)",
    """
    CREATE TABLE IF NOT EXISTS main.run_log (
        run_id VARCHAR PRIMARY KEY,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        news_ingested INTEGER,
        news_from_global INTEGER,
        news_from_fx_channels INTEGER,
        pairs_written INTEGER,
        errors INTEGER,
        notes VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS main.news_errors (
        run_id VARCHAR,
        occurred_at TIMESTAMP,
        source VARCHAR,
        feed_url VARCHAR,
        error_type VARCHAR,
        error_detail VARCHAR
    )
    """,
]

with duckdb.connect(ag4_path) as con:
    for stmt in ag4_sql:
        con.execute(stmt)
    cols = con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='news_history'
          AND column_name IN (
            'impact_region',
            'impact_asset_class',
            'impact_magnitude',
            'impact_fx_pairs',
            'tagger_version'
          )
        ORDER BY column_name
    """).fetchall()
    print("ag4_v3 columns:", cols)

with duckdb.connect(fx_path) as con:
    for stmt in fx_sql:
        con.execute(stmt)
    tables = con.execute("SHOW TABLES").fetchall()
    print("ag4_forex_v1 tables:", tables)

print("migration OK")
PY

ls -lh "$HOST_DUCKDB_DIR/ag4_v3.duckdb" "$HOST_DUCKDB_DIR/ag4_forex_v1.duckdb"
echo "DONE"
