#!/usr/bin/env python3
"""Self-healing FX : normalise en EUR tout nouveau fill sans devise tracee.

Contexte (migration 22/07, docs/operations/AUDIT_recon_adjustment_20260722.md) :
convention `core.fills.price` = EUR ; `currency`/`price_native`/`fx_rate_eur`
tracent l'origine. Les writers n8n (confirmations broker) ecrivent encore des
prix natifs USD sans devise -> ce script les normalise et rebuild les lots.
Idempotent, ne touche que les fills `currency IS NULL`. Cron : 5,35 * * * *.

Deps : duckdb uniquement (stdlib sinon). DB_PATH surchargeable par env.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import statistics
import sys
import time

import duckdb

DB_PATH = os.environ.get("AG1_V4_DB", "/local-files/duckdb/ag1_v4_consensus.duckdb")
FALLBACK_USD_EUR = 0.8764
RATIO_MIN, RATIO_MAX = 0.85, 1.15


def log(msg: str) -> None:
    print(f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


def connect_rw(path: str, retries: int = 6, delay: float = 1.5) -> duckdb.DuckDBPyConnection:
    last = None
    for i in range(retries):
        try:
            return duckdb.connect(path)
        except Exception as exc:  # lock/busy
            last = exc
            time.sleep(delay * (i + 1))
    raise RuntimeError(f"connect failed: {last}")


def last_usd_rate(con: duckdb.DuckDBPyConnection) -> float:
    try:
        row = con.execute(
            "SELECT TRY_CAST(json_extract(meta_json, '$.rates.USD') AS DOUBLE) "
            "FROM core.portfolio_snapshot WHERE meta_json IS NOT NULL ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if row and row[0] and 0.5 < float(row[0]) < 1.5:
            return float(row[0])
    except Exception:
        pass
    return FALLBACK_USD_EUR


def ref_price(con: duckdb.DuckDBPyConnection, symbol: str, day: dt.date) -> float | None:
    try:
        rows = con.execute(
            "SELECT CAST(close AS DOUBLE) FROM core.market_prices "
            "WHERE UPPER(symbol) = ? AND ts::DATE BETWEEN ? AND ? AND close IS NOT NULL",
            [symbol, day - dt.timedelta(days=3), day + dt.timedelta(days=1)],
        ).fetchall()
        vals = [r[0] for r in rows if r[0]]
        return statistics.median(vals) if vals else None
    except Exception:
        return None


def normalize(con: duckdb.DuckDBPyConnection) -> int:
    for ddl in (
        "ALTER TABLE core.fills ADD COLUMN IF NOT EXISTS currency VARCHAR",
        "ALTER TABLE core.fills ADD COLUMN IF NOT EXISTS price_native DOUBLE",
        "ALTER TABLE core.fills ADD COLUMN IF NOT EXISTS fx_rate_eur DOUBLE",
    ):
        con.execute(ddl)
    pending = con.execute(
        """
        SELECT f.fill_id, UPPER(o.symbol), f.ts_fill, CAST(f.price AS DOUBLE), f.raw_fill_json
        FROM core.fills f JOIN core.orders o USING(order_id)
        WHERE f.currency IS NULL
        ORDER BY f.ts_fill
        """
    ).fetchall()
    if not pending:
        return 0
    rate = last_usd_rate(con)
    updates = []
    for fid, sym, ts, price, raw in pending:
        day = (ts if isinstance(ts, dt.datetime) else dt.datetime.now(dt.timezone.utc)).date()
        age_h = (dt.datetime.now(dt.timezone.utc) - (ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc))).total_seconds() / 3600
        try:
            rj = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            rj = {}
        src = str(rj.get("source") or "")
        if "." in sym:  # titre EUR local
            updates.append(("EUR", price, 1.0, price, fid))
            continue
        if age_h > 48:
            log(f"WARN fill {fid} {sym} age {age_h:.0f}h : taux courant approximatif")
        if "reconcile" in src:  # deja EUR
            final, native = price, price / rate
        else:  # natif USD
            native, final = price, round(price * rate, 6)
        rp = ref_price(con, sym, day)
        if rp:
            ratio = final / rp
            if not (RATIO_MIN <= ratio <= RATIO_MAX):
                log(f"SKIP fill {fid} {sym} ratio {ratio:.3f} hors bornes (price={price}, ref={rp:.2f})")
                continue
        updates.append(("USD", native, rate, final, fid))
    if updates:
        con.executemany(
            "UPDATE core.fills SET currency=?, price_native=?, fx_rate_eur=?, price=? WHERE fill_id=?",
            updates,
        )
    return len(updates)


def rebuild_lots(con: duckdb.DuckDBPyConnection) -> int:
    """Replique le rebuild FIFO du reconcile prod (format meta identique)."""
    fills = con.execute(
        """
        SELECT f.fill_id, CAST(f.ts_fill AS VARCHAR), CAST(f.qty AS DOUBLE), CAST(f.price AS DOUBLE),
               CAST(COALESCE(f.fees_eur, 0) AS DOUBLE), UPPER(COALESCE(o.symbol, '')), UPPER(COALESCE(o.side, ''))
        FROM core.fills f JOIN core.orders o ON o.order_id = f.order_id
        ORDER BY f.ts_fill, f.fill_id
        """
    ).fetchall()
    by_sym: dict[str, list[dict]] = {}
    lots: list[dict] = []
    for fid, ts, qty, price, fees, sym, side in fills:
        if not sym or qty is None or qty <= 0 or price is None or price <= 0:
            continue
        L = by_sym.setdefault(sym, [])
        if side == "BUY":
            lot = {"lot_id": f"LOT|{fid}", "symbol": sym, "open_fill_id": fid, "open_ts": ts,
                   "open_qty": qty, "open_price": price, "open_fees_eur": fees, "remaining": qty,
                   "status": "OPEN", "events": [], "partial": 0.0,
                   "closed": None, "close_ts": None, "close_fid": None}
            L.append(lot)
            lots.append(lot)
            continue
        if side != "SELL":
            continue
        rem = qty
        while rem > 1e-12:
            lot = next((l for l in L if l["status"] == "OPEN" and l["remaining"] > 1e-12), None)
            if lot is None:
                break
            take = min(lot["remaining"], rem)
            fee_alloc = fees * (take / qty) if qty > 0 else 0.0
            inc = (price - lot["open_price"]) * take - fee_alloc
            lot["events"].append({"close_fill_id": fid, "close_ts": ts, "qty": take,
                                  "close_price": price, "realized_pnl_eur": inc})
            lot["partial"] += inc
            lot["remaining"] -= take
            if lot["remaining"] <= 1e-12:
                lot.update(remaining=0.0, status="CLOSED", closed=round(lot["partial"], 2),
                           close_ts=ts, close_fid=fid)
            rem -= take
    con.execute("DELETE FROM core.position_lots")
    con.executemany(
        """
        INSERT INTO core.position_lots (
          lot_id, symbol, open_fill_id, open_ts, open_qty, open_price, open_fees_eur,
          remaining_qty, status, close_ts, close_fill_id, realized_pnl_eur, close_method, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'FIFO', ?)
        """,
        [[l["lot_id"], l["symbol"], l["open_fill_id"], l["open_ts"], l["open_qty"], l["open_price"],
          l["open_fees_eur"], l["remaining"], l["status"], l["close_ts"], l["close_fid"], l["closed"],
          json.dumps({"close_events": l["events"], "realized_pnl_partial": round(l["partial"], 2)},
                     ensure_ascii=False)] for l in lots],
    )
    return len(lots)


def main() -> int:
    con = connect_rw(DB_PATH)
    try:
        n = normalize(con)
        if n:
            r = rebuild_lots(con)
            log(f"normalized={n} fills, lots rebuilt={r}")
        else:
            log("nothing to do")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
