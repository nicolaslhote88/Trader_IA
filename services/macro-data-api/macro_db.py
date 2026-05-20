"""
DuckDB persistence pour les données macro, COT et taux.
Toutes les tables du framework 3 piliers.
"""

import logging
import math
import os
from datetime import date, datetime
from typing import Optional

import duckdb

logger = logging.getLogger("macro_db")

DEFAULT_DB_PATH = os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb")

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS macro;
CREATE SCHEMA IF NOT EXISTS cot;
CREATE SCHEMA IF NOT EXISTS rates;
CREATE SCHEMA IF NOT EXISTS pillars;

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
        today = date.today().isoformat()
        rows = [
            (today, ccy, d.get("rate_pct"), "Central Bank", "FRED")
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

    def upsert_country_indicator(self, currency: str, indicator: str, value: float, as_of: str, unit: str = ""):
        with self._connect() as con:
            con.execute(
                """INSERT OR REPLACE INTO macro.country_indicators
                   (as_of, currency, indicator, value, unit)
                   VALUES (?, ?, ?, ?, ?)""",
                [as_of, currency, indicator, value, unit],
            )

    def upsert_gdp_data(self, gdp_data: dict[str, dict]):
        for currency, d in gdp_data.items():
            if d.get("latest_qoq") is not None:
                self.upsert_country_indicator(currency, "gdp_growth_qoq", d["latest_qoq"], d.get("as_of", date.today().isoformat()), "pct_qoq_saar")
                self.upsert_country_indicator(currency, "gdp_momentum", d.get("momentum", 0.0), d.get("as_of", date.today().isoformat()), "delta_qoq")

    def upsert_cpi_data(self, cpi_data: dict[str, dict]):
        for currency, d in cpi_data.items():
            if d.get("yoy_pct") is not None:
                self.upsert_country_indicator(currency, "cpi_yoy", d["yoy_pct"], d.get("as_of", date.today().isoformat()), "pct_yoy")

    def upsert_current_account_data(self, ca_data: dict[str, dict]):
        for currency, d in ca_data.items():
            if d.get("balance_bn_usd") is not None:
                self.upsert_country_indicator(currency, "current_account_bn_usd", d["balance_bn_usd"], d.get("as_of", date.today().isoformat()), "bn_usd")

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
                r.get("positioning_score"),
            )
            for r in records
        ]
        with self._connect() as con:
            con.executemany(
                """INSERT OR REPLACE INTO cot.speculative_positions
                   (report_date, currency, net_spec, lev_money_long, lev_money_short,
                    asset_mgr_long, asset_mgr_short, open_interest, net_z_score,
                    crowded_flag, crowded_direction, positioning_score)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                d.get("steepening"), d.get("rates_signal", "neutral"), "FRED",
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
                    composite_score, all_pillars_aligned)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
