"""Writer unique de ``worldmonitor_v1.duckdb``."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_PATH = os.environ.get("WORLD_MONITOR_DUCKDB_PATH", "/files/duckdb/worldmonitor_v1.duckdb")

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS cfg;

CREATE TABLE IF NOT EXISTS raw.api_responses (
  request_id VARCHAR PRIMARY KEY,
  run_id VARCHAR NOT NULL,
  capability VARCHAR,
  tool_name VARCHAR NOT NULL,
  tool_contract_hash VARCHAR,
  requested_at TIMESTAMP NOT NULL,
  received_at TIMESTAMP,
  status VARCHAR NOT NULL,
  cache_status VARCHAR,
  payload_hash VARCHAR,
  raw_payload_json JSON,
  error_code VARCHAR,
  error_detail VARCHAR
);

CREATE TABLE IF NOT EXISTS cfg.tool_registry (
  capability VARCHAR PRIMARY KEY,
  domain VARCHAR NOT NULL,
  tool_name VARCHAR,
  tool_contract_hash VARCHAR,
  tool_contract_json JSON,
  discovery_status VARCHAR NOT NULL,
  compatible BOOLEAN NOT NULL,
  discovered_at TIMESTAMP NOT NULL,
  config_version VARCHAR NOT NULL,
  detail VARCHAR
);

CREATE TABLE IF NOT EXISTS cfg.entity_mappings (
  mapping_type VARCHAR NOT NULL,
  source_entity VARCHAR NOT NULL,
  target_entities_json JSON NOT NULL,
  config_version VARCHAR NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (mapping_type, source_entity)
);

CREATE TABLE IF NOT EXISTS cfg.event_decay (
  event_type VARCHAR PRIMARY KEY,
  half_life_hours DOUBLE NOT NULL,
  config_version VARCHAR NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.events (
  snapshot_id VARCHAR NOT NULL,
  event_id VARCHAR NOT NULL,
  event_fingerprint VARCHAR NOT NULL,
  event_type VARCHAR NOT NULL,
  title VARCHAR NOT NULL,
  summary VARCHAR,
  countries_json JSON,
  regions_json JSON,
  coordinates_json JSON,
  sectors_json JSON,
  commodities_json JSON,
  currencies_json JSON,
  chokepoints_json JSON,
  severity_raw VARCHAR,
  severity_normalized DOUBLE,
  confidence DOUBLE,
  source_count INTEGER,
  source_diversity DOUBLE,
  event_time TIMESTAMP,
  first_seen TIMESTAMP,
  last_seen TIMESTAMP,
  ingestion_time TIMESTAMP NOT NULL,
  half_life_hours DOUBLE,
  freshness_decay DOUBLE,
  relevance_factor DOUBLE,
  effective_score DOUBLE,
  is_correlated_signal BOOLEAN,
  is_llm_generated BOOLEAN,
  ag4_duplicate BOOLEAN,
  derived_from_json JSON,
  lineage_json JSON,
  PRIMARY KEY (snapshot_id, event_id)
);

CREATE TABLE IF NOT EXISTS core.snapshots (
  snapshot_id VARCHAR PRIMARY KEY,
  schema_version VARCHAR NOT NULL,
  as_of TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  global_risk_regime VARCHAR NOT NULL,
  global_risk_score DOUBLE,
  confidence DOUBLE NOT NULL,
  coverage_ratio DOUBLE NOT NULL,
  freshness_status VARCHAR NOT NULL,
  critical_event_count INTEGER NOT NULL,
  missing_sources_json JSON,
  stale_sources_json JSON,
  payload_hash VARCHAR NOT NULL,
  method_version VARCHAR NOT NULL,
  payload_json JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS core.country_risk (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);
CREATE TABLE IF NOT EXISTS core.chokepoint_status (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);
CREATE TABLE IF NOT EXISTS core.energy_risk (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);
CREATE TABLE IF NOT EXISTS core.supply_chain_risk (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);
CREATE TABLE IF NOT EXISTS core.cyber_risk (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);
CREATE TABLE IF NOT EXISTS core.sanctions (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);
CREATE TABLE IF NOT EXISTS core.signal_convergence (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);
CREATE TABLE IF NOT EXISTS core.temporal_anomalies (
  snapshot_id VARCHAR NOT NULL, entity_id VARCHAR NOT NULL, risk_score DOUBLE,
  confidence DOUBLE, freshness_status VARCHAR, contributors_json JSON, payload_json JSON,
  PRIMARY KEY (snapshot_id, entity_id)
);

CREATE TABLE IF NOT EXISTS core.asset_impacts (
  snapshot_id VARCHAR NOT NULL,
  asset_id VARCHAR NOT NULL,
  impact_score DOUBLE,
  confidence DOUBLE,
  exposure_known BOOLEAN NOT NULL,
  contributors_json JSON,
  limitations_json JSON,
  payload_json JSON,
  PRIMARY KEY (snapshot_id, asset_id)
);
CREATE TABLE IF NOT EXISTS core.sector_impacts (
  snapshot_id VARCHAR NOT NULL,
  sector VARCHAR NOT NULL,
  impact_score DOUBLE,
  confidence DOUBLE,
  contributors_json JSON,
  payload_json JSON,
  PRIMARY KEY (snapshot_id, sector)
);

CREATE TABLE IF NOT EXISTS core.source_health (
  run_id VARCHAR NOT NULL,
  capability VARCHAR NOT NULL,
  tool_name VARCHAR,
  status VARCHAR NOT NULL,
  latency_ms DOUBLE,
  rows_received INTEGER,
  cache_status VARCHAR,
  error_code VARCHAR,
  error_detail VARCHAR,
  checked_at TIMESTAMP NOT NULL,
  PRIMARY KEY (run_id, capability)
);

CREATE TABLE IF NOT EXISTS core.run_log (
  run_id VARCHAR PRIMARY KEY,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  status VARCHAR NOT NULL,
  tools_discovered INTEGER DEFAULT 0,
  tools_called INTEGER DEFAULT 0,
  responses_ok INTEGER DEFAULT 0,
  responses_error INTEGER DEFAULT 0,
  events_normalized INTEGER DEFAULT 0,
  events_deduplicated INTEGER DEFAULT 0,
  snapshot_id VARCHAR,
  error_code VARCHAR,
  error_detail VARCHAR,
  payload_json JSON
);

CREATE OR REPLACE VIEW main.v_latest_ag9_global_risk AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY created_at DESC, snapshot_id DESC) rn
  FROM core.snapshots
) WHERE rn = 1;

CREATE OR REPLACE VIEW main.v_latest_events AS
SELECT e.* FROM core.events e
JOIN main.v_latest_ag9_global_risk s ON s.snapshot_id = e.snapshot_id
ORDER BY e.effective_score DESC NULLS LAST, e.event_time DESC NULLS LAST;

CREATE OR REPLACE VIEW main.v_latest_country_risk AS
SELECT c.* FROM core.country_risk c
JOIN main.v_latest_ag9_global_risk s ON s.snapshot_id = c.snapshot_id;

CREATE OR REPLACE VIEW main.v_latest_sector_impacts AS
SELECT c.* FROM core.sector_impacts c
JOIN main.v_latest_ag9_global_risk s ON s.snapshot_id = c.snapshot_id;

CREATE OR REPLACE VIEW main.v_source_health AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY capability ORDER BY checked_at DESC, run_id DESC) rn
  FROM core.source_health
) WHERE rn = 1;
"""


def jdump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class WorldMonitorDB:
    def __init__(self, path: str = DEFAULT_PATH):
        self.path = path
        parent = Path(path).resolve().parent
        parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as con:
            con.execute(SCHEMA_SQL)
            self._seed_config(con)

    def connect(self):
        return duckdb.connect(self.path)

    @staticmethod
    def _config(name: str) -> dict:
        path = Path(__file__).resolve().parent / "config" / name
        return json.loads(path.read_text(encoding="utf-8"))

    def _seed_config(self, con) -> None:
        decay = self._config("event_decay.json")
        rows = [(name, value, decay["config_version"]) for name, value in decay["event_half_life_hours"].items()]
        rows.append(("default", decay["default_half_life_hours"], decay["config_version"]))
        con.executemany("INSERT OR REPLACE INTO cfg.event_decay VALUES (?, ?, ?, now())", rows)
        mappings = self._config("entity_mappings.json")
        mapping_rows = []
        for mapping_type, entries in mappings.items():
            if mapping_type == "config_version":
                continue
            for source, targets in entries.items():
                mapping_rows.append((mapping_type, source, jdump(targets), mappings["config_version"]))
        if mapping_rows:
            con.executemany("INSERT OR REPLACE INTO cfg.entity_mappings VALUES (?, ?, ?, ?, now())", mapping_rows)

    def persist_registry(self, rows: list[dict]) -> None:
        if not rows:
            return
        with self.connect() as con:
            con.executemany(
                """INSERT OR REPLACE INTO cfg.tool_registry
                   (capability, domain, tool_name, tool_contract_hash, tool_contract_json,
                    discovery_status, compatible, discovered_at, config_version, detail)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(
                    row["capability"], row["domain"], row.get("tool_name"), row.get("tool_contract_hash"),
                    jdump(row.get("tool_contract")), row["discovery_status"], row["compatible"],
                    row["discovered_at"], row["config_version"], row.get("detail"),
                ) for row in rows],
            )

    def persist_refresh(self, bundle: dict) -> None:
        """Publie réponses, normalisations et snapshot dans une transaction."""

        with self.connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                raw_rows = bundle.get("raw_responses") or []
                if raw_rows:
                    con.executemany(
                        """INSERT OR REPLACE INTO raw.api_responses VALUES
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [(
                            row["request_id"], row["run_id"], row.get("capability"), row["tool_name"],
                            row.get("tool_contract_hash"), row["requested_at"], row.get("received_at"),
                            row["status"], row.get("cache_status"), row.get("payload_hash"),
                            jdump(row.get("raw_payload")), row.get("error_code"), row.get("error_detail"),
                        ) for row in raw_rows],
                    )
                events = bundle.get("events") or []
                if events:
                    con.executemany(
                        """INSERT OR REPLACE INTO core.events VALUES
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [(
                            row["snapshot_id"], row["event_id"], row["event_fingerprint"], row["event_type"],
                            row["title"], row.get("summary"), jdump(row.get("countries")), jdump(row.get("regions")),
                            jdump(row.get("coordinates")), jdump(row.get("sectors")), jdump(row.get("commodities")),
                            jdump(row.get("currencies")), jdump(row.get("chokepoints")), str(row.get("severity_raw")),
                            row.get("severity_normalized"), row.get("confidence"), row.get("source_count"),
                            row.get("source_diversity"), row.get("event_time"), row.get("first_seen"), row.get("last_seen"),
                            row["ingestion_time"], row.get("half_life_hours"), row.get("freshness_decay"),
                            row.get("relevance_factor"), row.get("effective_score"), row.get("is_correlated_signal", False),
                            row.get("is_llm_generated", False), row.get("ag4_duplicate", False),
                            jdump(row.get("derived_from")), jdump(row.get("lineage")),
                        ) for row in events],
                    )
                self._persist_overlay_tables(con, bundle)
                health = bundle.get("source_health") or []
                if health:
                    con.executemany(
                        "INSERT OR REPLACE INTO core.source_health VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [(
                            row["run_id"], row["capability"], row.get("tool_name"), row["status"], row.get("latency_ms"),
                            row.get("rows_received"), row.get("cache_status"), row.get("error_code"), row.get("error_detail"),
                            row["checked_at"],
                        ) for row in health],
                    )
                snapshot = bundle["snapshot"]
                con.execute(
                    "INSERT OR REPLACE INTO core.snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        snapshot["snapshot_id"], snapshot["schema_version"], snapshot["as_of"], snapshot["created_at"],
                        snapshot["global_risk_regime"], snapshot.get("global_risk_score"), snapshot["confidence"],
                        snapshot["coverage_ratio"], snapshot["freshness_status"], len(snapshot.get("critical_events") or []),
                        jdump(snapshot.get("missing_sources")), jdump(snapshot.get("stale_sources")), snapshot["payload_hash"],
                        snapshot["method_version"], jdump(snapshot),
                    ],
                )
                run = bundle["run_log"]
                con.execute(
                    "INSERT OR REPLACE INTO core.run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        run["run_id"], run["started_at"], run.get("finished_at"), run["status"], run.get("tools_discovered", 0),
                        run.get("tools_called", 0), run.get("responses_ok", 0), run.get("responses_error", 0),
                        run.get("events_normalized", 0), run.get("events_deduplicated", 0), run.get("snapshot_id"),
                        run.get("error_code"), run.get("error_detail"), jdump(run.get("payload")),
                    ],
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    @staticmethod
    def _persist_overlay_tables(con, bundle: dict) -> None:
        table_map = {
            "country_risk": "core.country_risk", "chokepoint_status": "core.chokepoint_status",
            "energy_risk": "core.energy_risk", "supply_chain_risk": "core.supply_chain_risk",
            "cyber_risk": "core.cyber_risk", "sanctions": "core.sanctions",
            "signal_convergence": "core.signal_convergence", "temporal_anomalies": "core.temporal_anomalies",
        }
        for key, table in table_map.items():
            rows = bundle.get(key) or []
            if rows:
                con.executemany(
                    f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(
                        row["snapshot_id"], row["entity_id"], row.get("risk_score"), row.get("confidence"),
                        row.get("freshness_status"), jdump(row.get("contributors")), jdump(row.get("payload")),
                    ) for row in rows],
                )
        sector_rows = bundle.get("sector_impacts") or []
        if sector_rows:
            con.executemany(
                "INSERT OR REPLACE INTO core.sector_impacts VALUES (?, ?, ?, ?, ?, ?)",
                [(
                    row["snapshot_id"], row["sector"], row.get("impact_score"), row.get("confidence"),
                    jdump(row.get("contributors")), jdump(row.get("payload")),
                ) for row in sector_rows],
            )
        asset_rows = bundle.get("asset_impacts") or []
        if asset_rows:
            con.executemany(
                "INSERT OR REPLACE INTO core.asset_impacts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [(
                    row["snapshot_id"], row["asset_id"], row.get("impact_score"), row.get("confidence"),
                    row.get("exposure_known", False), jdump(row.get("contributors")),
                    jdump(row.get("limitations")), jdump(row.get("payload")),
                ) for row in asset_rows],
            )

    def latest_snapshot(self) -> dict | None:
        with self.connect() as con:
            cur = con.execute("SELECT payload_json FROM main.v_latest_ag9_global_risk")
            row = cur.fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def persist_error_run(self, run: dict, raw_responses: list[dict], source_health: list[dict]) -> None:
        with self.connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                if raw_responses:
                    con.executemany(
                        """INSERT OR REPLACE INTO raw.api_responses VALUES
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        [(
                            row["request_id"], row["run_id"], row.get("capability"), row["tool_name"],
                            row.get("tool_contract_hash"), row["requested_at"], row.get("received_at"), row["status"],
                            row.get("cache_status"), row.get("payload_hash"), jdump(row.get("raw_payload")),
                            row.get("error_code"), row.get("error_detail"),
                        ) for row in raw_responses],
                    )
                if source_health:
                    con.executemany(
                        "INSERT OR REPLACE INTO core.source_health VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [(
                            row["run_id"], row["capability"], row.get("tool_name"), row["status"], row.get("latency_ms"),
                            row.get("rows_received"), row.get("cache_status"), row.get("error_code"),
                            row.get("error_detail"), row["checked_at"],
                        ) for row in source_health],
                    )
                con.execute(
                    "INSERT OR REPLACE INTO core.run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        run["run_id"], run["started_at"], run.get("finished_at"), run["status"], run.get("tools_discovered", 0),
                        run.get("tools_called", 0), run.get("responses_ok", 0), run.get("responses_error", 0),
                        run.get("events_normalized", 0), run.get("events_deduplicated", 0), None,
                        run.get("error_code"), run.get("error_detail"), jdump(run.get("payload")),
                    ],
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    def source_health(self) -> list[dict]:
        with self.connect() as con:
            cur = con.execute("SELECT * FROM main.v_source_health ORDER BY capability")
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def run_history(self, limit: int = 100) -> list[dict]:
        with self.connect() as con:
            cur = con.execute("SELECT * FROM core.run_log ORDER BY started_at DESC LIMIT ?", [limit])
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
