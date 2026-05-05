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


def iter_db_paths(args) -> list[str]:
    if args.db:
        return [str(Path(p).expanduser()) for p in args.db]
    raw = os.getenv("AG1_FX_DB_PATHS", "").strip()
    if raw:
        return [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    return list(DEFAULT_DBS)


class FxHistory:
    def __init__(self, base_url: str, interval: str, lookback_days: int):
        self.base_url = base_url.rstrip("/")
        self.interval = interval
        self.lookback_days = int(lookback_days)
        self.cache: dict[str, list[tuple[datetime, float]]] = {}
        self.warnings: list[str] = []

    def _fetch_history(self, pair: str) -> list[tuple[datetime, float]]:
        pair = str(pair or "").upper().strip()
        if pair in self.cache:
            return self.cache[pair]

        params = {
            "symbol": f"{pair}=X",
            "interval": self.interval,
            "lookback_days": self.lookback_days,
            "max_bars": max(100, self.lookback_days * 30),
            "min_bars": 1,
            "allow_stale": "true",
        }
        url = f"{self.base_url}/history?{urlencode(params)}"
        try:
            req = Request(url, headers={"Accept": "application/json"})
            with urlopen(req, timeout=25) as resp:
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
                self.warnings.append(f"No history for {pair}")
            return out
        except Exception as exc:
            self.cache[pair] = []
            self.warnings.append(f"{pair}: {exc}")
            return []

    def rate_on_or_before(self, pair: str, at_dt: datetime) -> float | None:
        hist = self._fetch_history(pair)
        if not hist:
            return None
        at_dt = at_dt if at_dt.tzinfo else at_dt.replace(tzinfo=timezone.utc)
        before = [rate for dt, rate in hist if dt <= at_dt]
        if before:
            return float(before[-1])
        return float(hist[0][1])

    def quote_to_eur(self, quote: str, at_dt: datetime) -> float | None:
        quote = str(quote or "").upper().strip()
        if quote == "EUR":
            return 1.0

        direct = self.rate_on_or_before(f"{quote}EUR", at_dt)
        if direct and direct > 0:
            return direct

        inverse = self.rate_on_or_before(f"EUR{quote}", at_dt)
        if inverse and inverse > 0:
            return 1.0 / inverse

        eurusd = self.rate_on_or_before("EURUSD", at_dt)
        usd_eur = 1.0 / eurusd if eurusd and eurusd > 0 else 0.0
        if quote == "USD":
            if usd_eur > 0:
                return usd_eur
            self.warnings.append(f"Missing EURUSD for {at_dt.isoformat()}")
            return None

        quote_usd = self.rate_on_or_before(f"{quote}USD", at_dt)
        if quote_usd and quote_usd > 0 and usd_eur > 0:
            return quote_usd * usd_eur

        usd_quote = self.rate_on_or_before(f"USD{quote}", at_dt)
        if usd_quote and usd_quote > 0 and usd_eur > 0:
            return (1.0 / usd_quote) * usd_eur

        self.warnings.append(f"Missing quote->EUR conversion for {quote} on {at_dt.isoformat()}")
        return None


def read_config(con) -> tuple[float, float]:
    if table_exists(con, "cfg", "portfolio_config"):
        row = con.execute(
            """
            SELECT initial_capital_eur, leverage_max
            FROM cfg.portfolio_config
            WHERE config_key = 'default'
            LIMIT 1
            """
        ).fetchone()
        if row:
            return to_float(row[0], 10000.0), max(0.01, to_float(row[1], 1.0))
    return 10000.0, 1.0


def fetch_records(con) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    def dict_rows(query: str) -> list[dict]:
        cur = con.execute(query)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    snapshots = dict_rows(
        """
        SELECT snapshot_id, run_id, as_of, cash_eur, equity_eur, margin_used_eur,
               margin_free_eur, leverage_effective, open_lots_count, pnl_day_eur,
               pnl_total_eur, drawdown_day_pct, drawdown_total_pct, notes
        FROM core.portfolio_snapshot
        ORDER BY as_of, snapshot_id
        """
    )

    lots = dict_rows(
        """
        SELECT lot_id, pair, side, size_lots, open_price, open_at,
               close_price, close_at, pnl_eur
        FROM core.position_lots
        ORDER BY open_at, lot_id
        """
    )

    fills = []
    if table_exists(con, "core", "fills"):
        fills = dict_rows(
            """
            SELECT fill_id, fees_eur, filled_at
            FROM core.fills
            ORDER BY filled_at, fill_id
            """
        )

    cash_rows = []
    if table_exists(con, "core", "cash_ledger"):
        cash_rows = dict_rows(
            """
            SELECT amount_eur, as_of
            FROM core.cash_ledger
            ORDER BY as_of, ledger_id
            """
        )

    return snapshots, lots, fills, cash_rows


def row_dt(row: dict, key: str) -> datetime | None:
    return parse_dt(row.get(key))


def cumulative_cash(cash_rows: list[dict], at_dt: datetime, initial: float) -> float:
    total = 0.0
    for row in cash_rows:
        dt = row_dt(row, "as_of")
        if dt and dt <= at_dt:
            total += to_float(row.get("amount_eur"), 0.0)
    return total if abs(total) > 0.005 else initial


def recompute_snapshot(
    snapshot: dict,
    lots: list[dict],
    fills: list[dict],
    cash_rows: list[dict],
    history: FxHistory,
    initial: float,
    leverage_max: float,
    day_start_equity: float,
    peak_equity: float,
) -> tuple[dict | None, list[dict]]:
    as_dt = row_dt(snapshot, "as_of")
    if as_dt is None:
        return None, [{"snapshot_id": snapshot.get("snapshot_id"), "reason": "missing_as_of"}]

    skipped = []
    cash_eur = cumulative_cash(cash_rows, as_dt, initial)
    realized = 0.0
    floating = 0.0
    notional = 0.0
    open_count = 0

    for lot in lots:
        pair = str(lot.get("pair") or "").upper().strip()
        if len(pair) != 6:
            continue
        open_dt = row_dt(lot, "open_at")
        close_dt = row_dt(lot, "close_at")
        if close_dt and close_dt <= as_dt:
            realized += to_float(lot.get("pnl_eur"), 0.0)
            continue
        if not open_dt or open_dt > as_dt:
            continue

        px = history.rate_on_or_before(pair, as_dt)
        q2e = history.quote_to_eur(pair[3:6], as_dt)
        if px is None or q2e is None:
            skipped.append(
                {
                    "snapshot_id": snapshot.get("snapshot_id"),
                    "pair": pair,
                    "as_of": as_dt.isoformat(),
                    "missing": "price" if px is None else "quote_to_eur",
                }
            )
            continue
        direction = 1.0 if str(lot.get("side") or "").lower() == "long" else -1.0
        size = to_float(lot.get("size_lots"), 0.0)
        open_price = to_float(lot.get("open_price"), px)
        floating += size * 100000.0 * (px - open_price) * direction * q2e
        notional += abs(size * 100000.0 * px * q2e)
        open_count += 1

    fees = 0.0
    for fill in fills:
        dt = row_dt(fill, "filled_at")
        if dt and dt <= as_dt:
            fees += to_float(fill.get("fees_eur"), 0.0)

    equity = initial + realized + floating - fees
    pnl_total = equity - initial
    margin_used = notional / leverage_max
    margin_free = max(0.0, equity - margin_used)
    leverage = notional / equity if equity > 0 else 0.0
    drawdown_day = equity / day_start_equity - 1.0 if day_start_equity > 0 else 0.0
    drawdown_total = equity / peak_equity - 1.0 if peak_equity > 0 else 0.0

    return (
        {
            "snapshot_id": snapshot.get("snapshot_id"),
            "cash_eur": cash_eur,
            "equity_eur": equity,
            "margin_used_eur": margin_used,
            "margin_free_eur": margin_free,
            "leverage_effective": leverage,
            "open_lots_count": open_count,
            "pnl_day_eur": equity - day_start_equity,
            "pnl_total_eur": pnl_total,
            "drawdown_day_pct": min(0.0, drawdown_day),
            "drawdown_total_pct": min(0.0, drawdown_total),
            "notes": "AG1-FX history rewritten from corrected lots/fills",
            "_old_equity_eur": to_float(snapshot.get("equity_eur"), 0.0),
            "_as_of": as_dt,
        },
        skipped,
    )


def rewrite_db(db_path: str, history: FxHistory, dry_run: bool) -> dict[str, object]:
    db = Path(db_path)
    if not db.exists():
        return {"db": db_path, "status": "missing", "snapshots": 0, "updated": 0}

    con = connect_duckdb(db, read_only=dry_run)
    try:
        required = [
            table_exists(con, "core", "portfolio_snapshot"),
            table_exists(con, "core", "position_lots"),
        ]
        if not all(required):
            return {"db": db_path, "status": "missing_table", "snapshots": 0, "updated": 0}

        initial, leverage_max = read_config(con)
        snapshots, lots, fills, cash_rows = fetch_records(con)
        day_start_by_date: dict[object, float] = {}
        peak = initial
        updated = 0
        skipped_missing_rates = 0
        delta_equity = 0.0
        examples = []
        skipped_examples = []

        for snapshot in snapshots:
            as_dt = row_dt(snapshot, "as_of")
            if as_dt is None:
                continue
            day_key = as_dt.date()
            day_start = day_start_by_date.get(day_key, peak if peak > 0 else initial)
            result, skipped = recompute_snapshot(
                snapshot,
                lots,
                fills,
                cash_rows,
                history,
                initial,
                leverage_max,
                day_start,
                peak,
            )
            if skipped:
                skipped_missing_rates += len(skipped)
                skipped_examples.extend(skipped[: max(0, 8 - len(skipped_examples))])
                continue
            if result is None:
                continue
            day_start_by_date.setdefault(day_key, result["equity_eur"])
            peak = max(peak, result["equity_eur"])

            old_equity = result.pop("_old_equity_eur")
            result.pop("_as_of")
            changed = abs(old_equity - result["equity_eur"]) > 0.005
            if not changed:
                continue
            updated += 1
            delta_equity += result["equity_eur"] - old_equity
            if len(examples) < 8:
                examples.append(
                    {
                        "snapshot_id": result["snapshot_id"],
                        "old_equity_eur": round(old_equity, 4),
                        "new_equity_eur": round(result["equity_eur"], 4),
                        "delta_eur": round(result["equity_eur"] - old_equity, 4),
                        "open_lots_count": result["open_lots_count"],
                    }
                )
            if not dry_run:
                con.execute(
                    """
                    UPDATE core.portfolio_snapshot
                    SET cash_eur = ?, equity_eur = ?, margin_used_eur = ?,
                        margin_free_eur = ?, leverage_effective = ?,
                        open_lots_count = ?, pnl_day_eur = ?, pnl_total_eur = ?,
                        drawdown_day_pct = ?, drawdown_total_pct = ?, notes = ?
                    WHERE snapshot_id = ?
                    """,
                    [
                        result["cash_eur"],
                        result["equity_eur"],
                        result["margin_used_eur"],
                        result["margin_free_eur"],
                        result["leverage_effective"],
                        result["open_lots_count"],
                        result["pnl_day_eur"],
                        result["pnl_total_eur"],
                        result["drawdown_day_pct"],
                        result["drawdown_total_pct"],
                        result["notes"],
                        result["snapshot_id"],
                    ],
                )

        if not dry_run:
            con.execute("CHECKPOINT")

        status = "dry_run" if dry_run else "updated"
        if skipped_missing_rates:
            status = "blocked_missing_rates" if dry_run else "partial_missing_rates"
        return {
            "db": db_path,
            "status": status,
            "snapshots": len(snapshots),
            "updated": updated,
            "skipped_missing_rates": skipped_missing_rates,
            "delta_equity_sum_eur": round(delta_equity, 4),
            "examples": examples,
            "skipped_examples": skipped_examples,
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Rewrite AG1-FX portfolio_snapshot history from corrected lots/fills.")
    parser.add_argument("--db", action="append", help="DuckDB path. Can be repeated. Defaults to the 3 AG1-FX portfolio DBs.")
    parser.add_argument("--yfinance-api", default=os.getenv("YFINANCE_API_URL", "http://yfinance-api:8080"))
    parser.add_argument("--interval", default="1h", help="History interval used to mark open lots at snapshot time.")
    parser.add_argument("--lookback-days", type=int, default=730)
    parser.add_argument("--apply", action="store_true", help="Write corrections. Without this flag the script only reports changes.")
    args = parser.parse_args()

    history = FxHistory(args.yfinance_api, args.interval, args.lookback_days)
    dry_run = not args.apply
    results = [rewrite_db(path, history, dry_run=dry_run) for path in iter_db_paths(args)]
    print(json.dumps({"dry_run": dry_run, "results": results, "warnings": sorted(set(history.warnings))}, indent=2, ensure_ascii=False))
    if any(int(r.get("skipped_missing_rates") or 0) > 0 for r in results):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
