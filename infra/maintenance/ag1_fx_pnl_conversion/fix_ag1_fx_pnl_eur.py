#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb


DEFAULT_DBS = (
    "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb",
    "/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb",
    "/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb",
)


def to_float(value, default=0.0) -> float:
    try:
        if value is None:
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def parse_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class FxRates:
    def __init__(self, base_url: str, lookback_days: int):
        self.base_url = base_url.rstrip("/")
        self.lookback_days = int(lookback_days)
        self.cache: dict[str, list[tuple[datetime, float]]] = {}
        self.warnings: list[str] = []

    def _fetch_history(self, pair: str) -> list[tuple[datetime, float]]:
        pair = str(pair or "").upper().strip()
        if pair in self.cache:
            return self.cache[pair]

        params = {
            "symbol": f"{pair}=X",
            "interval": "1d",
            "lookback_days": self.lookback_days,
            "max_bars": max(10, self.lookback_days + 10),
            "min_bars": 1,
            "allow_stale": "true",
        }
        url = f"{self.base_url}/history?{urlencode(params)}"
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            bars = payload.get("bars") if isinstance(payload, dict) else []
            out: list[tuple[datetime, float]] = []
            for bar in bars or []:
                dt = parse_dt(bar.get("t"))
                close = to_float(bar.get("c"), 0.0)
                if dt and close > 0:
                    out.append((dt, close))
            out.sort(key=lambda item: item[0])
            self.cache[pair] = out
            if not out:
                self.warnings.append(f"No conversion history for {pair}")
            return out
        except Exception as exc:
            self.cache[pair] = []
            self.warnings.append(f"{pair}: {exc}")
            return []

    def _rate_on_or_before(self, pair: str, at_dt: datetime) -> float | None:
        hist = self._fetch_history(pair)
        if not hist:
            return None
        at_dt = at_dt if at_dt.tzinfo else at_dt.replace(tzinfo=timezone.utc)
        before = [rate for dt, rate in hist if dt.date() <= at_dt.date()]
        if before:
            return float(before[-1])
        return float(hist[0][1])

    def quote_to_eur(self, quote: str, at_dt: datetime) -> float:
        quote = str(quote or "").upper().strip()
        if quote == "EUR":
            return 1.0

        direct = self._rate_on_or_before(f"{quote}EUR", at_dt)
        if direct and direct > 0:
            return direct

        inverse = self._rate_on_or_before(f"EUR{quote}", at_dt)
        if inverse and inverse > 0:
            return 1.0 / inverse

        eurusd = self._rate_on_or_before("EURUSD", at_dt)
        usd_eur = 1.0 / eurusd if eurusd and eurusd > 0 else 0.0
        if quote == "USD":
            if usd_eur > 0:
                return usd_eur
            self.warnings.append(f"Missing EURUSD for {at_dt.date()}, using USD->EUR=1")
            return 1.0

        quote_usd = self._rate_on_or_before(f"{quote}USD", at_dt)
        if quote_usd and quote_usd > 0 and usd_eur > 0:
            return quote_usd * usd_eur

        usd_quote = self._rate_on_or_before(f"USD{quote}", at_dt)
        if usd_quote and usd_quote > 0 and usd_eur > 0:
            return (1.0 / usd_quote) * usd_eur

        self.warnings.append(f"Missing quote->EUR conversion for {quote} on {at_dt.date()}, fallback=1")
        return 1.0


def iter_db_paths(args) -> list[str]:
    if args.db:
        return [str(Path(p).expanduser()) for p in args.db]
    raw = os.getenv("AG1_FX_DB_PATHS", "").strip()
    if raw:
        return [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    return list(DEFAULT_DBS)


def connect_duckdb(db_path: Path, read_only: bool):
    last_exc = None
    for attempt in range(7):
        try:
            return duckdb.connect(str(db_path), read_only=read_only)
        except Exception as exc:
            last_exc = exc
            msg = str(exc).lower()
            if ("lock" in msg or "busy" in msg) and attempt < 6:
                time.sleep(0.25 * (2**attempt))
                continue
            raise
    raise last_exc


def table_exists(con, schema_name: str, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def fix_db(db_path: str, rates: FxRates, dry_run: bool) -> dict[str, object]:
    db = Path(db_path)
    if not db.exists():
        return {"db": db_path, "status": "missing", "rows": 0, "updated": 0, "delta_eur": 0.0}

    con = connect_duckdb(db, read_only=dry_run)
    try:
        if not table_exists(con, "core", "position_lots"):
            return {"db": db_path, "status": "missing_table", "rows": 0, "updated": 0, "delta_eur": 0.0}

        rows = con.execute(
            """
            SELECT lot_id, pair, side, size_lots, open_price, close_price, close_at,
                   pnl_quote, pnl_eur
            FROM core.position_lots
            WHERE status = 'closed'
              AND close_price IS NOT NULL
              AND close_at IS NOT NULL
            ORDER BY close_at, lot_id
            """
        ).fetchall()

        updated = 0
        delta_eur = 0.0
        examples = []
        for lot_id, pair, side, size_lots, open_price, close_price, close_at, old_pnl_quote, old_pnl_eur in rows:
            pair = str(pair or "").upper().strip()
            if len(pair) != 6:
                continue
            at_dt = parse_dt(close_at)
            if at_dt is None:
                continue
            direction = 1.0 if str(side or "").lower() == "long" else -1.0
            pnl_quote = to_float(size_lots) * 100000.0 * (to_float(close_price) - to_float(open_price)) * direction
            q2e = rates.quote_to_eur(pair[3:6], at_dt)
            pnl_eur = pnl_quote * q2e
            old_eur = to_float(old_pnl_eur, 0.0)
            old_quote = to_float(old_pnl_quote, 0.0)
            changed = abs(old_eur - pnl_eur) > 0.005 or abs(old_quote - pnl_quote) > 0.005
            if not changed:
                continue
            updated += 1
            delta_eur += pnl_eur - old_eur
            if len(examples) < 8:
                examples.append(
                    {
                        "lot_id": lot_id,
                        "pair": pair,
                        "old_pnl_eur": round(old_eur, 4),
                        "new_pnl_eur": round(pnl_eur, 4),
                        "pnl_quote": round(pnl_quote, 4),
                        "q2e": round(q2e, 8),
                    }
                )
            if not dry_run:
                con.execute(
                    """
                    UPDATE core.position_lots
                    SET pnl_quote = ?, pnl_eur = ?
                    WHERE lot_id = ?
                    """,
                    [pnl_quote, pnl_eur, lot_id],
                )
        if not dry_run:
            con.execute("CHECKPOINT")
        return {
            "db": db_path,
            "status": "dry_run" if dry_run else "updated",
            "rows": len(rows),
            "updated": updated,
            "delta_eur": round(delta_eur, 4),
            "examples": examples,
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fix AG1-FX closed lot pnl_eur conversion from quote currency to EUR.")
    parser.add_argument("--db", action="append", help="DuckDB path. Can be repeated. Defaults to the 3 AG1-FX portfolio DBs.")
    parser.add_argument("--yfinance-api", default=os.getenv("YFINANCE_API_URL", "http://yfinance-api:8080"))
    parser.add_argument("--lookback-days", type=int, default=5000)
    parser.add_argument("--apply", action="store_true", help="Write corrections. Without this flag the script only reports changes.")
    args = parser.parse_args()

    rates = FxRates(args.yfinance_api, args.lookback_days)
    dry_run = not args.apply
    results = [fix_db(path, rates, dry_run=dry_run) for path in iter_db_paths(args)]
    print(json.dumps({"dry_run": dry_run, "results": results, "warnings": sorted(set(rates.warnings))}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
