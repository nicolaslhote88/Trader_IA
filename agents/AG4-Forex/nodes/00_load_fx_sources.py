import os
import re
import duckdb
from datetime import datetime, timezone

CONFIG_PATH = os.getenv("AG4_FX_SOURCES_PATH", "/files/Trader_IA/infra/config/sources/fx_sources.yaml")
DB_PATH = os.getenv("AG4_FOREX_DB_PATH", "/files/duckdb/ag4_forex_v1.duckdb")


def parse_bool(v):
    return str(v or "").strip().lower() == "true"


def load_sources(path):
    text = open(path, "r", encoding="utf-8").read()
    sources = []
    current = None
    for line in text.splitlines():
        raw = line.rstrip()
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        m = re.match(r"\s*-\s+id:\s*(.+)\s*$", raw)
        if m:
            if current:
                sources.append(current)
            current = {"id": m.group(1).strip()}
            continue
        if current is None:
            continue
        m = re.match(r"\s+([a-zA-Z_]+):\s*(.+?)\s*$", raw)
        if not m:
            continue
        key, value = m.group(1), m.group(2).strip()
        current[key] = value.strip('"').strip("'")
    if current:
        sources.append(current)
    return sources


def init_schema(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
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
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS main.news_errors (
            run_id VARCHAR,
            occurred_at TIMESTAMP,
            source VARCHAR,
            feed_url VARCHAR,
            error_type VARCHAR,
            error_detail VARCHAR
        )
        """
    )


run_id = f"AG4FX_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
sources = [s for s in load_sources(CONFIG_PATH) if parse_bool(s.get("enabled")) and s.get("type") == "rss"]

with duckdb.connect(DB_PATH) as con:
    init_schema(con)
    con.execute(
        """
        INSERT OR REPLACE INTO main.run_log (
            run_id, started_at, news_ingested, news_from_global, news_from_fx_channels, pairs_written, errors, notes
        ) VALUES (?, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, ?)
        """,
        [run_id, f"sources={len(sources)}"],
    )

return [
    {
        "json": {
            "run_id": run_id,
            "sourceId": s.get("id", ""),
            "sourceTier": s.get("tier", "A"),
            "url": s.get("url", ""),
            "db_path": DB_PATH,
        }
    }
    for s in sources
]

