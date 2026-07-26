#!/usr/bin/env python3
"""Collecteur de coûts broker IBKR -> broker_costs.duckdb (base dédiée, single-writer).

Poll deux endpoints du service ibkr-broker et persiste de façon idempotente :
  - GET /fills          : trades récents (7 j glissants IBKR) avec `commission` RÉELLE par
                          exécution. On garde tout (STK + conversions FX/CASH) ; le dashboard
                          filtre `sec_type='CASH'` pour les frais de change réels.
  - GET /account/ledger : soldes de cash par devise (EUR/USD/...) -> instrumente la liquidité
                          USD (montant réel) pour la cascade « Impact change sur liquidité ».

Base écrite en devise native ; conversion EUR faite côté lecteur (dashboard) au taux courant.
Idempotent : upsert par execution_id (trades) et par (ts_day, currency) (cash).

Usage : broker_costs_collector.py  (variables d'env ci-dessous)
  BROKER_BASE_URL   défaut http://127.0.0.1:18080
  BROKER_COSTS_DB   défaut /local-files/duckdb/broker_costs.duckdb
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

import duckdb

BROKER_BASE_URL = os.getenv("BROKER_BASE_URL", "http://127.0.0.1:18080").rstrip("/")
DB_PATH = os.getenv("BROKER_COSTS_DB", "/local-files/duckdb/broker_costs.duckdb")
HTTP_TIMEOUT = float(os.getenv("BROKER_HTTP_TIMEOUT", "25"))


def _get(path: str):
    url = f"{BROKER_BASE_URL}{path}"
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as r:
        return json.loads(r.read().decode())


def _f(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", "."))
    except Exception:
        return default


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_trades (
          execution_id   VARCHAR PRIMARY KEY,
          account        VARCHAR,
          symbol         VARCHAR,
          sec_type       VARCHAR,
          side           VARCHAR,
          size           DOUBLE,
          price          DOUBLE,
          commission     DOUBLE,      -- devise native de l'exécution
          net_amount     DOUBLE,
          conid          VARCHAR,
          trade_time_ms  BIGINT,
          trade_ts       TIMESTAMP,
          ingested_at    TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cash_snapshots (
          ts_day       DATE,
          currency     VARCHAR,
          cashbalance  DOUBLE,
          settledcash  DOUBLE,
          netliq       DOUBLE,
          exchangerate DOUBLE,       -- devise -> BASE (EUR)
          ts           TIMESTAMP,
          PRIMARY KEY (ts_day, currency)
        )
        """
    )


def collect_trades(con: duckdb.DuckDBPyConnection) -> int:
    try:
        trades = _get("/fills")
    except Exception as exc:  # noqa: BLE001
        print(f"[trades] ERREUR fetch /fills: {exc}", file=sys.stderr)
        return 0
    if not isinstance(trades, list):
        print(f"[trades] réponse inattendue: {type(trades).__name__}", file=sys.stderr)
        return 0
    now = datetime.now(timezone.utc)
    n = 0
    for t in trades:
        eid = str(t.get("execution_id") or "").strip()
        if not eid:
            continue
        ms = t.get("trade_time_r")
        ms = int(ms) if ms not in (None, "") else None
        ts = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc) if ms else None
        con.execute(
            """
            INSERT INTO broker_trades
              (execution_id, account, symbol, sec_type, side, size, price, commission,
               net_amount, conid, trade_time_ms, trade_ts, ingested_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (execution_id) DO UPDATE SET
              commission = excluded.commission,
              net_amount = excluded.net_amount,
              ingested_at = excluded.ingested_at
            """,
            [
                eid,
                str(t.get("account") or t.get("accountCode") or ""),
                str(t.get("symbol") or ""),
                str(t.get("sec_type") or t.get("secType") or "").upper(),
                str(t.get("side") or ""),
                _f(t.get("size"), 0.0),
                _f(t.get("price"), 0.0),
                _f(t.get("commission"), 0.0),
                _f(t.get("net_amount"), 0.0),
                str(t.get("conid") or ""),
                ms,
                ts,
                now,
            ],
        )
        n += 1
    return n


def collect_ledger(con: duckdb.DuckDBPyConnection) -> int:
    try:
        led = _get("/account/ledger")
    except Exception as exc:  # noqa: BLE001
        print(f"[ledger] ERREUR fetch /account/ledger: {exc}", file=sys.stderr)
        return 0
    if not isinstance(led, dict):
        print(f"[ledger] réponse inattendue: {type(led).__name__}", file=sys.stderr)
        return 0
    now = datetime.now(timezone.utc)
    day = now.date()
    n = 0
    for ccy, d in led.items():
        if not isinstance(d, dict):
            continue
        cur = str(d.get("currency") or ccy).upper()
        if cur in ("", "BASE"):
            # 'BASE' = agrégat, on garde seulement les vraies devises
            if cur == "BASE":
                cur = "BASE"
            else:
                continue
        con.execute(
            """
            INSERT INTO cash_snapshots (ts_day, currency, cashbalance, settledcash, netliq, exchangerate, ts)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT (ts_day, currency) DO UPDATE SET
              cashbalance = excluded.cashbalance,
              settledcash = excluded.settledcash,
              netliq = excluded.netliq,
              exchangerate = excluded.exchangerate,
              ts = excluded.ts
            """,
            [
                day,
                cur,
                _f(d.get("cashbalance"), 0.0),
                _f(d.get("settledcash"), 0.0),
                _f(d.get("netliquidationvalue"), 0.0),
                _f(d.get("exchangerate"), 1.0),
                now,
            ],
        )
        n += 1
    return n


def main() -> int:
    con = duckdb.connect(DB_PATH)
    try:
        ensure_schema(con)
        nt = collect_trades(con)
        nc = collect_ledger(con)
        fx = con.execute("SELECT COUNT(*) FROM broker_trades WHERE sec_type='CASH'").fetchone()[0]
        stk = con.execute("SELECT COUNT(*) FROM broker_trades WHERE sec_type='STK'").fetchone()[0]
        con.execute("CHECKPOINT")
        print(
            f"OK collect: trades={nt} (STK={stk}, FX/CASH={fx}) cash_rows={nc} -> {DB_PATH}"
        )
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
