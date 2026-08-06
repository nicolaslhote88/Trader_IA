"""
DuckDB persistence pour les données macro, COT et taux.
Toutes les tables du framework 3 piliers.
"""

import logging
import math
import os
import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger("macro_db")

DEFAULT_DB_PATH = os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb")

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS macro;
CREATE SCHEMA IF NOT EXISTS cot;
CREATE SCHEMA IF NOT EXISTS rates;
CREATE SCHEMA IF NOT EXISTS pillars;
CREATE SCHEMA IF NOT EXISTS components;
CREATE SCHEMA IF NOT EXISTS cfg;

-- Taux directeurs banques centrales
CREATE TABLE IF NOT EXISTS macro.policy_rates (
    as_of       VARCHAR NOT NULL,
    currency    VARCHAR NOT NULL,
    rate_pct    DOUBLE,
    cb_name     VARCHAR,
    source      VARCHAR DEFAULT 'FRED',
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of, currency)
);

-- Indicateurs macroéconomiques (PIB, CPI, CA)
CREATE TABLE IF NOT EXISTS macro.country_indicators (
    as_of       VARCHAR NOT NULL,
    currency    VARCHAR NOT NULL,
    indicator   VARCHAR NOT NULL,  -- 'gdp_growth', 'cpi_yoy', 'current_account'
    value       DOUBLE,
    unit        VARCHAR,
    source      VARCHAR DEFAULT 'FRED',
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of, currency, indicator)
);

-- Courbe des taux souverains
CREATE TABLE IF NOT EXISTS rates.yield_curve (
    as_of           VARCHAR NOT NULL,
    currency        VARCHAR NOT NULL,
    yield_2y_pct    DOUBLE,
    yield_10y_pct   DOUBLE,
    slope_10y2y     DOUBLE,
    slope_change_30d DOUBLE,
    steepening      BOOLEAN,
    rates_signal    VARCHAR DEFAULT 'neutral',
    source          VARCHAR DEFAULT 'FRED',
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of, currency)
);

-- COT CFTC — positions spéculatives
CREATE TABLE IF NOT EXISTS cot.speculative_positions (
    report_date         VARCHAR NOT NULL,
    currency            VARCHAR NOT NULL,
    net_spec            INTEGER,
    lev_money_long      INTEGER,
    lev_money_short     INTEGER,
    asset_mgr_long      INTEGER,
    asset_mgr_short     INTEGER,
    open_interest       INTEGER,
    net_z_score         DOUBLE,
    crowded_flag        BOOLEAN DEFAULT FALSE,
    crowded_direction   VARCHAR DEFAULT 'neutral',
    positioning_score   DOUBLE,
    source              VARCHAR DEFAULT 'CFTC_COT',
    confidence          VARCHAR DEFAULT 'high',
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, currency)
);

-- Scores piliers calculés (synthèse quotidienne)
CREATE TABLE IF NOT EXISTS pillars.currency_scores (
    as_of                   VARCHAR NOT NULL,
    currency                VARCHAR NOT NULL,
    -- Pilier 1 : Macro/Flows
    macro_growth_score      DOUBLE,
    macro_inflation_score   DOUBLE,
    macro_policy_score      DOUBLE,
    macro_ca_score          DOUBLE,
    macro_score             DOUBLE,  -- score composite pilier 1 [-1,+1]
    -- Pilier 2 : Valorisation
    carry_score             DOUBLE,
    ppp_deviation           DOUBLE,
    valuation_score         DOUBLE,  -- score composite pilier 2 [-1,+1]
    -- Pilier 3 : Positionnement
    cot_z_score             DOUBLE,
    positioning_score       DOUBLE,  -- score composite pilier 3 [-1,+1]
    crowded_flag            BOOLEAN DEFAULT FALSE,
    -- Composite
    composite_score         DOUBLE,  -- moyenne pondérée des 3 piliers
    all_pillars_aligned     BOOLEAN DEFAULT FALSE,
    data_completeness       VARCHAR DEFAULT 'complete',
    score_status            VARCHAR DEFAULT 'scored',
    confidence_floor        VARCHAR DEFAULT 'high',
    missing_inputs          VARCHAR,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (as_of, currency)
);

-- Log des runs de refresh
CREATE TABLE IF NOT EXISTS pillars.run_log (
    run_id      VARCHAR PRIMARY KEY,
    started_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status      VARCHAR DEFAULT 'running',
    error_msg   VARCHAR,
    records_written INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cfg.neutral_rates (
    currency VARCHAR PRIMARY KEY,
    rate_pct DOUBLE,
    uncertainty_pct DOUBLE,
    source VARCHAR NOT NULL,
    method VARCHAR NOT NULL,
    as_of VARCHAR,
    confidence DOUBLE NOT NULL,
    config_version VARCHAR NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS components.ag5_macro (
    component_snapshot_id VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    entity_type VARCHAR NOT NULL,
    observation_time TIMESTAMP,
    publication_time TIMESTAMP,
    ingestion_time TIMESTAMP NOT NULL,
    calculation_time TIMESTAMP NOT NULL,
    macro_score DOUBLE,
    subscores_json JSON,
    coverage_ratio DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    freshness_status VARCHAR NOT NULL,
    missing_inputs_json JSON,
    stale_inputs_json JSON,
    proxy_inputs_json JSON,
    weights_json JSON,
    contributions_json JSON,
    lineage_json JSON,
    source VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    method_version VARCHAR NOT NULL,
    PRIMARY KEY (component_snapshot_id, entity_id)
);

CREATE TABLE IF NOT EXISTS components.ag6_fx_valuation (
    component_snapshot_id VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    observation_time TIMESTAMP,
    publication_time TIMESTAMP,
    ingestion_time TIMESTAMP NOT NULL,
    calculation_time TIMESTAMP NOT NULL,
    carry_score DOUBLE,
    real_carry_score DOUBLE,
    ppp_gap DOUBLE,
    reer_gap DOUBLE,
    terms_of_trade_score DOUBLE,
    valuation_score DOUBLE,
    spot_reference DOUBLE,
    coverage_ratio DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    freshness_status VARCHAR NOT NULL,
    missing_inputs_json JSON,
    stale_inputs_json JSON,
    proxy_inputs_json JSON,
    input_status_json JSON,
    weights_json JSON,
    contributions_json JSON,
    lineage_json JSON,
    source VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    method_version VARCHAR NOT NULL,
    PRIMARY KEY (component_snapshot_id, currency)
);

CREATE TABLE IF NOT EXISTS components.ag7_positioning (
    component_snapshot_id VARCHAR NOT NULL,
    entity_id VARCHAR NOT NULL,
    report_date DATE,
    observation_time TIMESTAMP,
    publication_time TIMESTAMP,
    ingestion_time TIMESTAMP NOT NULL,
    calculation_time TIMESTAMP NOT NULL,
    net_position DOUBLE,
    z_score DOUBLE,
    positioning_score DOUBLE,
    crowded_flag BOOLEAN,
    crowded_direction VARCHAR,
    crowded_threshold DOUBLE NOT NULL,
    is_proxy BOOLEAN NOT NULL,
    contributors_json JSON,
    weights_json JSON,
    confidence DOUBLE NOT NULL,
    freshness_status VARCHAR NOT NULL,
    missing_inputs_json JSON,
    lineage_json JSON,
    source VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    method_version VARCHAR NOT NULL,
    PRIMARY KEY (component_snapshot_id, entity_id)
);

CREATE TABLE IF NOT EXISTS components.ag8_rates_liquidity (
    component_snapshot_id VARCHAR NOT NULL,
    currency VARCHAR NOT NULL,
    observation_time TIMESTAMP,
    publication_time TIMESTAMP,
    ingestion_time TIMESTAMP NOT NULL,
    calculation_time TIMESTAMP NOT NULL,
    policy_regime VARCHAR NOT NULL,
    curve_regime VARCHAR NOT NULL,
    yield_2y DOUBLE,
    yield_10y DOUBLE,
    slope_10y2y DOUBLE,
    slope_change DOUBLE,
    real_rate DOUBLE,
    duration_pressure DOUBLE,
    liquidity_score DOUBLE,
    overlays_json JSON,
    coverage_ratio DOUBLE NOT NULL,
    confidence DOUBLE NOT NULL,
    freshness_status VARCHAR NOT NULL,
    missing_inputs_json JSON,
    stale_inputs_json JSON,
    proxy_inputs_json JSON,
    lineage_json JSON,
    source VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    method_version VARCHAR NOT NULL,
    PRIMARY KEY (component_snapshot_id, currency)
);

CREATE TABLE IF NOT EXISTS components.run_log (
    run_id VARCHAR PRIMARY KEY,
    component VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR NOT NULL,
    rows_read INTEGER DEFAULT 0,
    rows_written INTEGER DEFAULT 0,
    coverage_ratio DOUBLE,
    error_code VARCHAR,
    error_detail VARCHAR,
    payload_json JSON
);

CREATE OR REPLACE VIEW main.v_latest_ag5_macro AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY calculation_time DESC, component_snapshot_id DESC) rn
  FROM components.ag5_macro
) WHERE rn = 1;

CREATE OR REPLACE VIEW main.v_latest_ag6_fx_valuation AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY calculation_time DESC, component_snapshot_id DESC) rn
  FROM components.ag6_fx_valuation
) WHERE rn = 1;

CREATE OR REPLACE VIEW main.v_latest_ag7_positioning AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY report_date DESC NULLS LAST, calculation_time DESC) rn
  FROM components.ag7_positioning
) WHERE rn = 1;

CREATE OR REPLACE VIEW main.v_latest_ag8_rates_liquidity AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY calculation_time DESC, component_snapshot_id DESC) rn
  FROM components.ag8_rates_liquidity
) WHERE rn = 1;
"""


class MacroDB:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_schema(self):
        with self._connect() as con:
            con.execute(SCHEMA_SQL)
            self._migrate_schema(con)
            self._seed_neutral_rates(con)

    @staticmethod
    def _seed_neutral_rates(con):
        config_path = Path(__file__).resolve().parent / "config" / "neutral_rates.json"
        if not config_path.is_file():
            logger.warning("Neutral-rate config missing: %s", config_path)
            return
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        config_version = str(payload.get("config_version") or "UNKNOWN")
        rows = []
        for currency, row in (payload.get("currencies") or {}).items():
            rows.append((
                str(currency).upper(),
                row.get("rate_pct"),
                row.get("uncertainty_pct"),
                row.get("source"),
                row.get("method"),
                row.get("as_of"),
                row.get("confidence"),
                config_version,
            ))
        if rows:
            con.executemany(
                """INSERT INTO cfg.neutral_rates
                   (currency, rate_pct, uncertainty_pct, source, method, as_of, confidence, config_version)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (currency) DO UPDATE SET
                     rate_pct=excluded.rate_pct,
                     uncertainty_pct=excluded.uncertainty_pct,
                     source=excluded.source,
                     method=excluded.method,
                     as_of=excluded.as_of,
                     confidence=excluded.confidence,
                     config_version=excluded.config_version,
                     updated_at=now()""",
                rows,
            )

    @staticmethod
    def _migrate_schema(con):
        """Idempotent migrations for already-created DuckDB files."""
        migrations = [
            "ALTER TABLE cot.speculative_positions ADD COLUMN source VARCHAR DEFAULT 'CFTC_COT'",
            "ALTER TABLE cot.speculative_positions ADD COLUMN confidence VARCHAR DEFAULT 'high'",
            "UPDATE cot.speculative_positions SET source = 'CFTC_COT' WHERE source IS NULL",
            "UPDATE cot.speculative_positions SET confidence = 'high' WHERE confidence IS NULL",
            "ALTER TABLE pillars.currency_scores ADD COLUMN data_completeness VARCHAR DEFAULT 'complete'",
            "ALTER TABLE pillars.currency_scores ADD COLUMN score_status VARCHAR DEFAULT 'scored'",
            "ALTER TABLE pillars.currency_scores ADD COLUMN confidence_floor VARCHAR DEFAULT 'high'",
            "ALTER TABLE pillars.currency_scores ADD COLUMN missing_inputs VARCHAR",
            "ALTER TABLE components.ag5_macro ADD COLUMN entity_type VARCHAR DEFAULT 'country_or_currency'",
            "ALTER TABLE components.ag6_fx_valuation ADD COLUMN input_status_json JSON",
            "ALTER TABLE components.ag6_fx_valuation ADD COLUMN stale_inputs_json JSON",
        ]
        for sql in migrations:
            try:
                con.execute(sql)
            except Exception as exc:
                logger.warning("MacroDB migration skipped (%s): %s", sql, exc)

    @staticmethod
    def _json_safe(value):
        if value is None:
            return None
        if isinstance(value, float) and not math.isfinite(value):
            return None
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        try:
            if hasattr(value, "item"):
                return MacroDB._json_safe(value.item())
        except Exception:
            pass
        return value

    @classmethod
    def _records(cls, cur) -> list[dict]:
        cols = [d[0] for d in cur.description]
        return [
            {col: cls._json_safe(value) for col, value in zip(cols, row)}
            for row in cur.fetchall()
        ]

    # ── Policy Rates ──────────────────────────────────────────────────────────

    def upsert_policy_rates(self, rates: dict[str, dict]):
        rows = [
            (d.get("as_of", date.today().isoformat()), ccy, d.get("rate_pct"), "Central Bank", d.get("source", "FRED"))
            for ccy, d in rates.items()
            if d.get("rate_pct") is not None
        ]
        if not rows:
            return
        with self._connect() as con:
            con.executemany(
                """INSERT OR REPLACE INTO macro.policy_rates
                   (as_of, currency, rate_pct, cb_name, source)
                   VALUES (?, ?, ?, ?, ?)""",
                rows,
            )

    def get_latest_policy_rates(self) -> list[dict]:
        with self._connect() as con:
            cur = con.execute(
                """SELECT DISTINCT ON (currency) *
                   FROM macro.policy_rates
                   ORDER BY currency, as_of DESC"""
            )
            return self._records(cur)

    # ── Country Indicators ────────────────────────────────────────────────────

    def upsert_country_indicator(self, currency: str, indicator: str, value: float, as_of: str, unit: str = "", source: str = "FRED"):
        with self._connect() as con:
            con.execute(
                """INSERT OR REPLACE INTO macro.country_indicators
                   (as_of, currency, indicator, value, unit, source)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [as_of, currency, indicator, value, unit, source],
            )

    def upsert_gdp_data(self, gdp_data: dict[str, dict]):
        for currency, d in gdp_data.items():
            if d.get("latest_qoq") is not None:
                self.upsert_country_indicator(currency, "gdp_growth_qoq", d["latest_qoq"], d.get("as_of", date.today().isoformat()), "pct_qoq_saar", d.get("source", "FRED"))
                self.upsert_country_indicator(currency, "gdp_momentum", d.get("momentum", 0.0), d.get("as_of", date.today().isoformat()), "delta_qoq", d.get("source", "FRED"))

    def upsert_cpi_data(self, cpi_data: dict[str, dict]):
        for currency, d in cpi_data.items():
            if d.get("yoy_pct") is not None:
                self.upsert_country_indicator(currency, "cpi_yoy", d["yoy_pct"], d.get("as_of", date.today().isoformat()), "pct_yoy", d.get("source", "FRED"))

    def upsert_current_account_data(self, ca_data: dict[str, dict]):
        for currency, d in ca_data.items():
            if d.get("balance_bn_usd") is not None:
                self.upsert_country_indicator(currency, "current_account_bn_usd", d["balance_bn_usd"], d.get("as_of", date.today().isoformat()), "bn_usd", d.get("source", "FRED"))

    def upsert_unemployment_data(self, unemployment_data: dict[str, dict]):
        for currency, d in unemployment_data.items():
            if d.get("unemployment_pct") is not None:
                self.upsert_country_indicator(
                    currency,
                    "unemployment_pct",
                    d["unemployment_pct"],
                    d.get("as_of", date.today().isoformat()),
                    "pct",
                    d.get("source", "FRED"),
                )

    def get_indicators(self, currency: Optional[str] = None, indicator: Optional[str] = None) -> list[dict]:
        with self._connect() as con:
            q = "SELECT * FROM macro.country_indicators WHERE 1=1"
            params = []
            if currency:
                q += " AND currency = ?"
                params.append(currency)
            if indicator:
                q += " AND indicator = ?"
                params.append(indicator)
            q += " ORDER BY as_of DESC"
            cur = con.execute(q, params)
            return self._records(cur)

    # ── COT Data ──────────────────────────────────────────────────────────────

    def upsert_cot_positions(self, records: list[dict]):
        if not records:
            return
        rows = [
            (
                r["report_date"], r["currency"], r.get("net_spec"), r.get("lev_money_long"), r.get("lev_money_short"),
                r.get("asset_mgr_long"), r.get("asset_mgr_short"), r.get("open_interest"),
                r.get("net_z_score"), r.get("crowded_flag", False), r.get("crowded_direction", "neutral"),
                r.get("positioning_score"), r.get("source", "CFTC_COT"), r.get("confidence", "high"),
            )
            for r in records
        ]
        with self._connect() as con:
            con.executemany(
                """INSERT OR REPLACE INTO cot.speculative_positions
                   (report_date, currency, net_spec, lev_money_long, lev_money_short,
                    asset_mgr_long, asset_mgr_short, open_interest, net_z_score,
                    crowded_flag, crowded_direction, positioning_score, source, confidence)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def get_latest_cot(self, currency: Optional[str] = None) -> list[dict]:
        with self._connect() as con:
            q = """SELECT DISTINCT ON (currency) *
                   FROM cot.speculative_positions"""
            params = []
            if currency:
                q += " WHERE currency = ?"
                params.append(currency)
            q += " ORDER BY currency, report_date DESC"
            cur = con.execute(q, params)
            return self._records(cur)

    def get_cot_history(self, currency: str, limit: int = 104) -> list[dict]:
        with self._connect() as con:
            cur = con.execute(
                "SELECT * FROM cot.speculative_positions WHERE currency = ? ORDER BY report_date DESC LIMIT ?",
                [currency, limit],
            )
            return self._records(cur)

    # ── Yield Curve ───────────────────────────────────────────────────────────

    def upsert_yield_curve(self, curves: dict[str, dict]):
        today = date.today().isoformat()
        rows = [
            (
                d.get("as_of", today), ccy,
                d.get("yield_2y"), d.get("yield_10y"),
                d.get("slope_10y2y"), d.get("slope_change_30d"),
                d.get("steepening"), d.get("rates_signal", "neutral"), d.get("source", "FRED"),
            )
            for ccy, d in curves.items()
            if d.get("yield_10y") is not None
        ]
        if not rows:
            return
        with self._connect() as con:
            con.executemany(
                """INSERT OR REPLACE INTO rates.yield_curve
                   (as_of, currency, yield_2y_pct, yield_10y_pct, slope_10y2y,
                    slope_change_30d, steepening, rates_signal, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def get_latest_yield_curve(self) -> list[dict]:
        with self._connect() as con:
            cur = con.execute(
                """SELECT DISTINCT ON (currency) *
                   FROM rates.yield_curve
                   ORDER BY currency, as_of DESC"""
            )
            return self._records(cur)

    def get_yield_curve_history(self, currency: str, limit: int = 90) -> list[dict]:
        with self._connect() as con:
            cur = con.execute(
                "SELECT * FROM rates.yield_curve WHERE currency = ? ORDER BY as_of DESC LIMIT ?",
                [currency, limit],
            )
            return self._records(cur)

    # ── Pillar Scores ─────────────────────────────────────────────────────────

    def upsert_pillar_scores(self, scores: list[dict]):
        if not scores:
            return
        today = date.today().isoformat()
        rows = [
            (
                s.get("as_of", today), s["currency"],
                s.get("macro_growth_score"), s.get("macro_inflation_score"),
                s.get("macro_policy_score"), s.get("macro_ca_score"), s.get("macro_score"),
                s.get("carry_score"), s.get("ppp_deviation"), s.get("valuation_score"),
                s.get("cot_z_score"), s.get("positioning_score"), s.get("crowded_flag", False),
                s.get("composite_score"), s.get("all_pillars_aligned", False),
                s.get("data_completeness", "complete"), s.get("score_status", "scored"),
                s.get("confidence_floor", "high"), s.get("missing_inputs"),
            )
            for s in scores
        ]
        with self._connect() as con:
            con.executemany(
                """INSERT OR REPLACE INTO pillars.currency_scores
                   (as_of, currency, macro_growth_score, macro_inflation_score,
                    macro_policy_score, macro_ca_score, macro_score,
                    carry_score, ppp_deviation, valuation_score,
                    cot_z_score, positioning_score, crowded_flag,
                    composite_score, all_pillars_aligned, data_completeness,
                    score_status, confidence_floor, missing_inputs)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def get_latest_pillar_scores(self) -> list[dict]:
        with self._connect() as con:
            cur = con.execute(
                """SELECT DISTINCT ON (currency) *
                   FROM pillars.currency_scores
                   ORDER BY currency, as_of DESC"""
            )
            return self._records(cur)

    def get_pillar_history(self, currency: str, limit: int = 30) -> list[dict]:
        with self._connect() as con:
            cur = con.execute(
                "SELECT * FROM pillars.currency_scores WHERE currency = ? ORDER BY as_of DESC LIMIT ?",
                [currency, limit],
            )
            return self._records(cur)

    # -- Canonical AG5-AG8 components ---------------------------------------

    @staticmethod
    def _json_text(value) -> str:
        return json.dumps(value if value is not None else [], ensure_ascii=False, sort_keys=True)

    def get_neutral_rates(self) -> dict[str, dict]:
        with self._connect() as con:
            rows = self._records(con.execute("SELECT * FROM cfg.neutral_rates ORDER BY currency"))
        return {str(row["currency"]): row for row in rows}

    def upsert_ag5_macro(self, rows: list[dict]):
        if not rows:
            raise ValueError("AG5_ZERO_VALID_ROWS")
        values = [(
            row["component_snapshot_id"], row["entity_id"], row.get("entity_type", "country_or_currency"), row.get("observation_time"),
            row.get("publication_time"), row["ingestion_time"], row["calculation_time"],
            row.get("macro_score"), self._json_text(row.get("subscores")),
            row["coverage_ratio"], row["confidence"], row["freshness_status"],
            self._json_text(row.get("missing_inputs")), self._json_text(row.get("stale_inputs")),
            self._json_text(row.get("proxy_inputs")), self._json_text(row.get("weights")),
            self._json_text(row.get("contributions")), self._json_text(row.get("lineage")),
            row.get("source", "MACRO_DATA_API"), row["schema_version"], row["method_version"],
        ) for row in rows]
        with self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.executemany(
                    """INSERT OR REPLACE INTO components.ag5_macro VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    values,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    def upsert_ag6_fx_valuation(self, rows: list[dict]):
        if not rows:
            raise ValueError("AG6_ZERO_VALID_ROWS")
        values = [(
            row["component_snapshot_id"], row["currency"], row.get("observation_time"),
            row.get("publication_time"), row["ingestion_time"], row["calculation_time"],
            row.get("carry_score"), row.get("real_carry_score"), row.get("ppp_gap"),
            row.get("reer_gap"), row.get("terms_of_trade_score"), row.get("valuation_score"),
            row.get("spot_reference"), row["coverage_ratio"], row["confidence"],
            row["freshness_status"], self._json_text(row.get("missing_inputs")),
            self._json_text(row.get("stale_inputs")), self._json_text(row.get("proxy_inputs")),
            self._json_text(row.get("input_status")), self._json_text(row.get("weights")),
            self._json_text(row.get("contributions")), self._json_text(row.get("lineage")),
            row.get("source", "MACRO_DATA_API"), row["schema_version"], row["method_version"],
        ) for row in rows]
        with self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.executemany(
                    """INSERT OR REPLACE INTO components.ag6_fx_valuation VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    values,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    def upsert_ag7_positioning(self, rows: list[dict]):
        if not rows:
            raise ValueError("AG7_ZERO_VALID_ROWS")
        values = [(
            row["component_snapshot_id"], row["entity_id"], row.get("report_date"),
            row.get("observation_time"), row.get("publication_time"), row["ingestion_time"],
            row["calculation_time"], row.get("net_position"), row.get("z_score"),
            row.get("positioning_score"), row.get("crowded_flag", False),
            row.get("crowded_direction", "unknown"), row.get("crowded_threshold", 1.5),
            row.get("is_proxy", False), self._json_text(row.get("contributors")),
            self._json_text(row.get("weights")), row["confidence"], row["freshness_status"],
            self._json_text(row.get("missing_inputs")), self._json_text(row.get("lineage")),
            row.get("source", "CFTC_COT"), row["schema_version"], row["method_version"],
        ) for row in rows]
        with self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.executemany(
                    """INSERT OR REPLACE INTO components.ag7_positioning VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    values,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    def upsert_ag8_rates_liquidity(self, rows: list[dict]):
        if not rows:
            raise ValueError("AG8_ZERO_VALID_ROWS")
        values = [(
            row["component_snapshot_id"], row["currency"], row.get("observation_time"),
            row.get("publication_time"), row["ingestion_time"], row["calculation_time"],
            row["policy_regime"], row["curve_regime"], row.get("yield_2y"),
            row.get("yield_10y"), row.get("slope_10y2y"), row.get("slope_change"),
            row.get("real_rate"), row.get("duration_pressure"), row.get("liquidity_score"),
            self._json_text(row.get("overlays")), row["coverage_ratio"], row["confidence"],
            row["freshness_status"], self._json_text(row.get("missing_inputs")),
            self._json_text(row.get("stale_inputs")), self._json_text(row.get("proxy_inputs")),
            self._json_text(row.get("lineage")), row.get("source", "MACRO_DATA_API"),
            row["schema_version"], row["method_version"],
        ) for row in rows]
        with self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.executemany(
                    """INSERT OR REPLACE INTO components.ag8_rates_liquidity VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    values,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    def get_latest_component(self, component: str) -> list[dict]:
        views = {
            "ag5": "main.v_latest_ag5_macro",
            "ag6": "main.v_latest_ag6_fx_valuation",
            "ag7": "main.v_latest_ag7_positioning",
            "ag8": "main.v_latest_ag8_rates_liquidity",
        }
        view = views.get(component.lower())
        if not view:
            raise ValueError(f"UNKNOWN_COMPONENT:{component}")
        with self._connect() as con:
            return self._records(con.execute(f"SELECT * FROM {view} ORDER BY 2"))

    def log_component_run(self, run: dict):
        with self._connect() as con:
            con.execute(
                """INSERT OR REPLACE INTO components.run_log
                   (run_id, component, started_at, finished_at, status, rows_read,
                    rows_written, coverage_ratio, error_code, error_detail, payload_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    run["run_id"], run["component"], run["started_at"], run.get("finished_at"),
                    run["status"], run.get("rows_read", 0), run.get("rows_written", 0),
                    run.get("coverage_ratio"), run.get("error_code"), run.get("error_detail"),
                    self._json_text(run.get("payload", {})),
                ],
            )

    def get_component_health(self, limit: int = 40) -> list[dict]:
        with self._connect() as con:
            return self._records(con.execute(
                "SELECT * FROM components.run_log ORDER BY started_at DESC LIMIT ?", [limit]
            ))
