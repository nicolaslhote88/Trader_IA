"""Writer unique du snapshot atomique ``global_context_v1.duckdb``."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_PATH = os.environ.get("GLOBAL_CONTEXT_DUCKDB_PATH", "/files/duckdb/global_context_v1.duckdb")

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS cfg;

CREATE TABLE IF NOT EXISTS core.snapshots (
  snapshot_id VARCHAR PRIMARY KEY,
  schema_version VARCHAR NOT NULL,
  as_of TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  status VARCHAR NOT NULL,
  component_snapshot_ids_json JSON NOT NULL,
  component_as_of_json JSON NOT NULL,
  component_ages_json JSON NOT NULL,
  coverage_ratio DOUBLE NOT NULL,
  confidence DOUBLE NOT NULL,
  freshness_status VARCHAR NOT NULL,
  payload_hash VARCHAR NOT NULL,
  method_version VARCHAR NOT NULL,
  ag1_pack_json JSON NOT NULL,
  payload_json JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS core.component_status (
  snapshot_id VARCHAR NOT NULL,
  component VARCHAR NOT NULL,
  component_snapshot_id VARCHAR,
  component_as_of TIMESTAMP,
  age_hours DOUBLE,
  status VARCHAR NOT NULL,
  coverage_ratio DOUBLE,
  confidence DOUBLE,
  freshness_status VARCHAR NOT NULL,
  schema_version VARCHAR,
  method_version VARCHAR,
  row_count INTEGER NOT NULL,
  warnings_json JSON,
  PRIMARY KEY (snapshot_id, component)
);

CREATE TABLE IF NOT EXISTS core.global_regime (
  snapshot_id VARCHAR PRIMARY KEY,
  macro_regime_json JSON,
  rates_liquidity_regime_json JSON,
  positioning_regime_json JSON,
  fx_relative_valuation_json JSON,
  geopolitical_risk_regime_json JSON,
  source_warnings_json JSON
);

CREATE TABLE IF NOT EXISTS core.country_context (
  snapshot_id VARCHAR NOT NULL, country VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, country)
);
CREATE TABLE IF NOT EXISTS core.currency_context (
  snapshot_id VARCHAR NOT NULL, currency VARCHAR NOT NULL,
  macro_json JSON, valuation_json JSON, positioning_json JSON, rates_json JSON, event_risk_json JSON,
  confidence DOUBLE, freshness_status VARCHAR, payload_json JSON,
  PRIMARY KEY (snapshot_id, currency)
);
CREATE TABLE IF NOT EXISTS core.sector_context (
  snapshot_id VARCHAR NOT NULL, sector VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, sector)
);
CREATE TABLE IF NOT EXISTS core.asset_context (
  snapshot_id VARCHAR NOT NULL, asset_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, exposure_known BOOLEAN NOT NULL,
  contributors_json JSON, limitations_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, asset_id)
);
CREATE TABLE IF NOT EXISTS core.critical_events (
  snapshot_id VARCHAR NOT NULL, event_id VARCHAR NOT NULL, event_type VARCHAR,
  title VARCHAR, summary VARCHAR, event_time TIMESTAMP, effective_score DOUBLE,
  confidence DOUBLE, countries_json JSON, sectors_json JSON, commodities_json JSON,
  currencies_json JSON, lineage_json JSON,
  PRIMARY KEY (snapshot_id, event_id)
);
CREATE TABLE IF NOT EXISTS core.source_lineage (
  snapshot_id VARCHAR NOT NULL, source_id VARCHAR NOT NULL, component VARCHAR NOT NULL,
  source_snapshot_id VARCHAR, source_as_of TIMESTAMP, schema_version VARCHAR,
  method_version VARCHAR, payload_hash VARCHAR, detail_json JSON,
  PRIMARY KEY (snapshot_id, source_id)
);
CREATE TABLE IF NOT EXISTS core.run_log (
  run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP,
  status VARCHAR NOT NULL, snapshot_id VARCHAR, components_available INTEGER,
  components_missing INTEGER, rows_written INTEGER, error_code VARCHAR,
  error_detail VARCHAR, payload_json JSON
);

CREATE TABLE IF NOT EXISTS cfg.weights (
  component VARCHAR PRIMARY KEY, weight DOUBLE NOT NULL, config_version VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS cfg.freshness (
  component VARCHAR PRIMARY KEY, max_age_hours DOUBLE NOT NULL, config_version VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS cfg.method_versions (
  name VARCHAR PRIMARY KEY, version VARCHAR NOT NULL, config_version VARCHAR NOT NULL
);

CREATE OR REPLACE VIEW main.v_latest_global_context AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY created_at DESC, snapshot_id DESC) rn FROM core.snapshots
) WHERE rn = 1;
CREATE OR REPLACE VIEW main.v_latest_country_context AS
SELECT c.* FROM core.country_context c JOIN main.v_latest_global_context s ON s.snapshot_id=c.snapshot_id;
CREATE OR REPLACE VIEW main.v_latest_currency_context AS
SELECT c.* FROM core.currency_context c JOIN main.v_latest_global_context s ON s.snapshot_id=c.snapshot_id;
CREATE OR REPLACE VIEW main.v_latest_sector_context AS
SELECT c.* FROM core.sector_context c JOIN main.v_latest_global_context s ON s.snapshot_id=c.snapshot_id;
CREATE OR REPLACE VIEW main.v_latest_asset_context AS
SELECT c.* FROM core.asset_context c JOIN main.v_latest_global_context s ON s.snapshot_id=c.snapshot_id;
CREATE OR REPLACE VIEW main.v_latest_critical_events AS
SELECT c.* FROM core.critical_events c JOIN main.v_latest_global_context s ON s.snapshot_id=c.snapshot_id
ORDER BY c.effective_score DESC NULLS LAST, c.event_time DESC NULLS LAST;
CREATE OR REPLACE VIEW main.v_component_health AS
SELECT c.* FROM core.component_status c JOIN main.v_latest_global_context s ON s.snapshot_id=c.snapshot_id
ORDER BY c.component;
CREATE OR REPLACE VIEW main.v_ag1_global_context_pack AS
SELECT snapshot_id, schema_version, as_of, created_at, status, freshness_status,
       coverage_ratio, confidence, payload_hash, method_version, ag1_pack_json
FROM main.v_latest_global_context;
"""


def jdump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


class GlobalContextDB:
    def __init__(self, path: str = DEFAULT_PATH):
        self.path = path
        Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as con:
            con.execute(SCHEMA_SQL)
            self._seed(con)

    def connect(self):
        return duckdb.connect(self.path)

    @staticmethod
    def _config() -> dict:
        path = Path(__file__).resolve().parent / "config" / "context.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def _seed(self, con) -> None:
        config = self._config()
        con.executemany("INSERT OR REPLACE INTO cfg.weights VALUES (?, ?, ?)", [(key, value, config["config_version"]) for key, value in config["component_weights"].items()])
        con.executemany("INSERT OR REPLACE INTO cfg.freshness VALUES (?, ?, ?)", [(key, value, config["config_version"]) for key, value in config["max_age_hours"].items()])
        con.executemany("INSERT OR REPLACE INTO cfg.method_versions VALUES (?, ?, ?)", [
            ("global_context", config["method_version"], config["config_version"]),
            ("schema", config["schema_version"], config["config_version"]),
        ])

    def publish(self, bundle: dict) -> None:
        """Transaction unique : un lecteur ne voit jamais un snapshot partiel."""

        snapshot = bundle["snapshot"]
        with self.connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    "INSERT INTO core.snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        snapshot["snapshot_id"], snapshot["schema_version"], snapshot["as_of"], snapshot["created_at"],
                        snapshot["status"], jdump(snapshot["component_snapshot_ids"]), jdump(snapshot["component_as_of"]),
                        jdump(snapshot["component_ages"]), snapshot["coverage_ratio"], snapshot["confidence"],
                        snapshot["freshness_status"], snapshot["payload_hash"], snapshot["method_version"],
                        jdump(snapshot["ag1_pack"]), jdump(snapshot),
                    ],
                )
                con.executemany(
                    "INSERT INTO core.component_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [(
                        snapshot["snapshot_id"], row["component"], row.get("component_snapshot_id"), row.get("component_as_of"),
                        row.get("age_hours"), row["status"], row.get("coverage_ratio"), row.get("confidence"),
                        row["freshness_status"], row.get("schema_version"), row.get("method_version"), row["row_count"],
                        jdump(row.get("warnings")),
                    ) for row in bundle["component_status"]],
                )
                regime = bundle["global_regime"]
                con.execute(
                    "INSERT INTO core.global_regime VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [snapshot["snapshot_id"], jdump(regime["macro_regime"]), jdump(regime["rates_liquidity_regime"]),
                     jdump(regime["positioning_regime"]), jdump(regime["fx_relative_valuation"]),
                     jdump(regime["geopolitical_risk_regime"]), jdump(regime["source_warnings"])],
                )
                self._insert_context(con, snapshot["snapshot_id"], bundle)
                run = bundle["run_log"]
                con.execute(
                    "INSERT INTO core.run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [run["run_id"], run["started_at"], run["finished_at"], run["status"], snapshot["snapshot_id"],
                     run["components_available"], run["components_missing"], run["rows_written"], None, None, jdump(run.get("payload"))],
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    @staticmethod
    def _insert_context(con, snapshot_id: str, bundle: dict) -> None:
        country_rows = bundle.get("country_context") or []
        if country_rows:
            con.executemany("INSERT INTO core.country_context VALUES (?, ?, ?, ?, ?, ?, ?)", [
                (snapshot_id, row["country"], row.get("risk_score"), row.get("confidence"), row.get("freshness_status"), jdump(row.get("contributors")), jdump(row.get("payload"))) for row in country_rows
            ])
        currency_rows = bundle.get("currency_context") or []
        if currency_rows:
            con.executemany("INSERT INTO core.currency_context VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                (snapshot_id, row["currency"], jdump(row.get("macro")), jdump(row.get("valuation")), jdump(row.get("positioning")), jdump(row.get("rates")), jdump(row.get("event_risk")), row.get("confidence"), row.get("freshness_status"), jdump(row)) for row in currency_rows
            ])
        sector_rows = bundle.get("sector_context") or []
        if sector_rows:
            con.executemany("INSERT INTO core.sector_context VALUES (?, ?, ?, ?, ?, ?, ?)", [
                (snapshot_id, row["sector"], row.get("risk_score"), row.get("confidence"), row.get("freshness_status"), jdump(row.get("contributors")), jdump(row.get("payload"))) for row in sector_rows
            ])
        asset_rows = bundle.get("asset_context") or []
        if asset_rows:
            con.executemany("INSERT INTO core.asset_context VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                (snapshot_id, row["asset_id"], row.get("risk_score"), row.get("confidence"), row.get("freshness_status"), row.get("exposure_known", False), jdump(row.get("contributors")), jdump(row.get("limitations")), jdump(row.get("payload"))) for row in asset_rows
            ])
        events = bundle.get("critical_events") or []
        if events:
            con.executemany("INSERT INTO core.critical_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                (snapshot_id, row["event_id"], row.get("event_type"), row.get("title"), row.get("summary"), row.get("event_time"), row.get("effective_score"), row.get("confidence"), jdump(row.get("countries")), jdump(row.get("sectors")), jdump(row.get("commodities")), jdump(row.get("currencies")), jdump(row.get("lineage"))) for row in events
            ])
        lineage = bundle.get("source_lineage") or []
        if lineage:
            con.executemany("INSERT INTO core.source_lineage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                (snapshot_id, row["source_id"], row["component"], row.get("source_snapshot_id"), row.get("source_as_of"), row.get("schema_version"), row.get("method_version"), row.get("payload_hash"), jdump(row.get("detail"))) for row in lineage
            ])

    def persist_error(self, run: dict) -> None:
        with self.connect() as con:
            con.execute("INSERT OR REPLACE INTO core.run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                run["run_id"], run["started_at"], run.get("finished_at"), "ERROR", None,
                run.get("components_available", 0), run.get("components_missing", 5), 0,
                run.get("error_code"), run.get("error_detail"), jdump(run.get("payload")),
            ])

    def latest(self) -> dict | None:
        with self.connect() as con:
            row = con.execute("SELECT payload_json FROM main.v_latest_global_context").fetchone()
        return json.loads(row[0]) if row else None

    def latest_pack(self) -> dict | None:
        with self.connect() as con:
            row = con.execute("SELECT ag1_pack_json FROM main.v_ag1_global_context_pack").fetchone()
        return json.loads(row[0]) if row else None

    def latest_exposure_context(self) -> dict:
        def rows(con, query: str) -> list[dict]:
            cur = con.execute(query)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        with self.connect() as con:
            return {
                "countries": rows(con, "SELECT * FROM main.v_latest_country_context"),
                "sectors": rows(con, "SELECT * FROM main.v_latest_sector_context"),
                "assets": rows(con, "SELECT * FROM main.v_latest_asset_context"),
            }

    def history(self, limit: int = 100) -> list[dict]:
        with self.connect() as con:
            cur = con.execute("SELECT * FROM core.run_log ORDER BY started_at DESC LIMIT ?", [limit])
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
