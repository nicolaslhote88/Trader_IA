#!/usr/bin/env python3
"""
Reconcile AG1 V4 DuckDB ledger with live IBKR read-only state.

This maintenance script does not place or confirm orders. It only reads the
ibkr-broker endpoints and repairs AG1 V4 ledger tables when invoked with
--apply. Default mode is dry-run.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
import urllib.request
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_DB_PATH = "/files/duckdb/ag1_v4_consensus.duckdb"
DEFAULT_BROKER_URL = "http://ibkr-broker:8080"
EXPECTED_ACCOUNT = "U25651155"


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return now_utc().isoformat()


def money(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def fetch_json(base_url: str, path: str, timeout: int = 20) -> Any:
    url = base_url.rstrip("/") + path
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).strip().replace(",", ".")
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def norm_symbol(symbol: Any) -> str:
    return str(symbol or "").strip().upper()


def ibkr_internal_symbol(row: dict[str, Any]) -> str:
    symbol = norm_symbol(
        row.get("symbol")
        or row.get("ticker")
        or row.get("contractDesc")
        or row.get("contract_description_1")
        or row.get("fullName")
    )
    if not symbol:
        return ""
    sec_type = norm_symbol(row.get("sec_type") or row.get("assetClass") or row.get("secType"))
    listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
    currency = norm_symbol(row.get("currency"))
    if sec_type == "STK" and currency == "EUR" and (
        listing in {"", "SBF", "EUDARK", "ENEXT", "PARIS"} or not listing
    ):
        return symbol if symbol.endswith(".PA") else f"{symbol}.PA"
    return symbol


def parse_ibkr_trade_time(row: dict[str, Any]) -> str:
    raw = str(row.get("trade_time") or "").strip()
    if raw:
        try:
            parsed = dt.datetime.strptime(raw, "%Y%m%d-%H:%M:%S").replace(tzinfo=dt.timezone.utc)
            return parsed.isoformat()
        except Exception:
            pass
    ms = parse_float(row.get("trade_time_r"), 0.0)
    if ms > 0:
        try:
            return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).isoformat()
        except Exception:
            pass
    return iso_now()


def side_from_ibkr(row: dict[str, Any]) -> str:
    side = norm_symbol(row.get("side"))
    if side in {"B", "BUY", "BOT"}:
        return "BUY"
    if side in {"S", "SELL", "SLD"}:
        return "SELL"
    desc = norm_symbol(row.get("order_description"))
    if desc.startswith("BOT "):
        return "BUY"
    if desc.startswith("SLD ") or desc.startswith("SOLD "):
        return "SELL"
    return side


def is_stock_fill(row: dict[str, Any]) -> bool:
    return norm_symbol(row.get("sec_type") or row.get("assetClass") or row.get("secType")) == "STK" and side_from_ibkr(row) in {"BUY", "SELL"}


def exchange_rates_from_ledger(ledger: dict[str, Any]) -> dict[str, float]:
    out = {"EUR": 1.0, "BASE": 1.0}
    for ccy, row in (ledger or {}).items():
        if isinstance(row, dict):
            rate = parse_float(row.get("exchangerate"), 0.0)
            if rate > 0:
                out[norm_symbol(ccy)] = rate
    return out


def position_market_value_eur(row: dict[str, Any], rates: dict[str, float]) -> float:
    ccy = norm_symbol(row.get("currency"))
    mv = parse_float(row.get("mktValue") or row.get("marketValue") or row.get("market_value"), 0.0)
    return mv * rates.get(ccy, 1.0)


def position_price_eur(row: dict[str, Any], rates: dict[str, float]) -> float:
    qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
    if qty == 0:
        return 0.0
    return position_market_value_eur(row, rates) / qty


def fill_price_eur(row: dict[str, Any], rates: dict[str, float]) -> float:
    ccy = norm_symbol(row.get("currency"))
    # /fills often omits currency for STK rows; use the listing/symbol defaults.
    if not ccy:
        listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
        ccy = "EUR" if listing in {"SBF", "EUDARK"} else "USD"
    return parse_float(row.get("price"), 0.0) * rates.get(ccy, 1.0)


def commission_eur(row: dict[str, Any], rates: dict[str, float]) -> float:
    amount = abs(parse_float(row.get("commission"), 0.0))
    listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
    ccy = norm_symbol(row.get("currency")) or ("EUR" if listing in {"SBF", "EUDARK"} else "USD")
    return amount * rates.get(ccy, 1.0)


def fetch_existing_orders(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cols = [
        "order_id",
        "run_id",
        "CAST(ts_created AS VARCHAR) AS ts_created",
        "symbol",
        "side",
        "qty",
        "limit_price",
        "status",
        "broker_order_id",
        "rationale_json",
    ]
    rows = con.execute(f"SELECT {', '.join(cols)} FROM core.orders").fetchall()
    names = [d[0] for d in con.description]
    return [dict(zip(names, row)) for row in rows]


def fetch_existing_execution_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    ids: set[str] = set()
    for (fill_id,) in con.execute("SELECT fill_id FROM core.fills").fetchall():
        text = str(fill_id or "")
        parts = text.split("_")
        if parts:
            ids.add(parts[-1])
        ids.add(text)
    try:
        for (execution_id,) in con.execute("SELECT broker_execution_id FROM core.fill_costs WHERE broker_execution_id IS NOT NULL").fetchall():
            if execution_id:
                ids.add(str(execution_id))
    except Exception:
        pass
    return ids


def parse_ts(value: Any) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def match_order(fill: dict[str, Any], orders: list[dict[str, Any]]) -> dict[str, Any] | None:
    broker_order_id = str(fill.get("order_id") or "").strip()
    symbol = ibkr_internal_symbol(fill)
    side = side_from_ibkr(fill)
    qty = parse_float(fill.get("size"), 0.0)
    execution_id = str(fill.get("execution_id") or "").strip()
    order_ref = str(fill.get("order_ref") or "").strip()

    for order in orders:
        if broker_order_id and str(order.get("broker_order_id") or "").strip() == broker_order_id:
            return order

    if order_ref:
        for order in orders:
            raw = str(order.get("rationale_json") or "")
            if order_ref in raw:
                return order

    fill_ts = parse_ts(parse_ibkr_trade_time(fill))
    candidates: list[tuple[float, dict[str, Any]]] = []
    for order in orders:
        if norm_symbol(order.get("symbol")) != symbol:
            continue
        if norm_symbol(order.get("side")) != side:
            continue
        if abs(parse_float(order.get("qty"), 0.0) - qty) > 1e-6:
            continue
        order_ts = parse_ts(order.get("ts_created"))
        if not fill_ts or not order_ts:
            continue
        delta_seconds = (fill_ts - order_ts).total_seconds()
        if -60 <= delta_seconds <= 6 * 3600:
            status = norm_symbol(order.get("status"))
            priority = 0 if status in {"PLANNED", "SUBMITTED"} else 1000
            candidates.append((priority + abs(delta_seconds), order))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    if execution_id:
        return None
    return None


def build_missing_fills(
    con: duckdb.DuckDBPyConnection,
    ibkr_fills: list[dict[str, Any]],
    rates: dict[str, float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    orders = fetch_existing_orders(con)
    existing_exec_ids = fetch_existing_execution_ids(con)
    missing: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []

    for row in ibkr_fills:
        if not isinstance(row, dict) or not is_stock_fill(row):
            continue
        execution_id = str(row.get("execution_id") or "").strip()
        if not execution_id or execution_id in existing_exec_ids:
            continue
        order = match_order(row, orders)
        if not order:
            unmatched.append(row)
            continue
        symbol = norm_symbol(order.get("symbol")) or ibkr_internal_symbol(row)
        run_id = str(order.get("run_id") or "").strip()
        order_id = str(order.get("order_id") or "").strip()
        missing.append(
            {
                "fill_id": f"FIL_{run_id}_{execution_id}",
                "order_id": order_id,
                "run_id": run_id,
                "symbol": symbol,
                "side": side_from_ibkr(row),
                "qty": parse_float(row.get("size"), 0.0),
                "price": fill_price_eur(row, rates),
                "fees_eur": commission_eur(row, rates),
                "ts_fill": parse_ibkr_trade_time(row),
                "broker_execution_id": execution_id,
                "broker_order_id": str(row.get("order_id") or ""),
                "raw": row,
            }
        )
    return missing, unmatched


def upsert_instruments_for_positions(con: duckdb.DuckDBPyConnection, positions: list[dict[str, Any]]) -> None:
    rows = []
    for row in positions:
        symbol = ibkr_internal_symbol(row)
        if not symbol:
            continue
        rows.append(
            [
                symbol,
                str(row.get("company_name") or row.get("contractDesc") or row.get("contract_description_1") or symbol),
                "EQUITY",
                str(row.get("listing_exchange") or row.get("exchange") or ""),
                norm_symbol(row.get("currency")) or None,
                None,
                None,
                None,
                True,
                iso_now(),
            ]
        )
    if not rows:
        return
    con.executemany(
        """
        INSERT INTO core.instruments (
          symbol, name, asset_class, exchange, currency, isin, sector, industry, is_active, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (symbol) DO UPDATE SET
          name = COALESCE(core.instruments.name, excluded.name),
          asset_class = COALESCE(core.instruments.asset_class, excluded.asset_class),
          exchange = COALESCE(core.instruments.exchange, excluded.exchange),
          currency = COALESCE(core.instruments.currency, excluded.currency),
          is_active = TRUE,
          updated_at = excluded.updated_at
        """,
        rows,
    )


def insert_missing_fills(con: duckdb.DuckDBPyConnection, missing: list[dict[str, Any]]) -> None:
    if not missing:
        return
    con.executemany(
        """
        INSERT INTO core.fills (
          fill_id, order_id, run_id, ts_fill, qty, price, fees_eur, slippage_bps, liquidity, raw_fill_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
        ON CONFLICT (fill_id) DO NOTHING
        """,
        [
            [
                row["fill_id"],
                row["order_id"],
                row["run_id"],
                row["ts_fill"],
                row["qty"],
                row["price"],
                row["fees_eur"],
                "IBKR_IMPORTED",
                json.dumps({"source": "ibkr_live_reconcile", "ibkrFill": row["raw"]}, ensure_ascii=False),
            ]
            for row in missing
        ],
    )
    con.executemany(
        """
        INSERT INTO core.fill_costs (
          fill_id, order_id, symbol, pair, broker, broker_execution_id,
          commission_amount, commission_ccy, commission_eur, commission_source, raw_json, recorded_at
        ) VALUES (?, ?, ?, ?, 'IBKR', ?, ?, 'EUR', ?, 'ibkr_live_reconcile', ?, ?)
        ON CONFLICT (fill_id) DO UPDATE SET
          order_id = excluded.order_id,
          symbol = excluded.symbol,
          pair = excluded.pair,
          broker = excluded.broker,
          broker_execution_id = excluded.broker_execution_id,
          commission_amount = excluded.commission_amount,
          commission_eur = excluded.commission_eur,
          commission_source = excluded.commission_source,
          raw_json = excluded.raw_json,
          recorded_at = excluded.recorded_at
        """,
        [
            [
                row["fill_id"],
                row["order_id"],
                row["symbol"],
                row["symbol"],
                row["broker_execution_id"],
                row["fees_eur"],
                row["fees_eur"],
                json.dumps(row["raw"], ensure_ascii=False),
                iso_now(),
            ]
            for row in missing
        ],
    )
    con.executemany(
        """
        UPDATE core.orders
        SET status = 'FILLED',
            broker_order_id = COALESCE(NULLIF(?, ''), broker_order_id),
            reason = NULL
        WHERE order_id = ?
        """,
        [[row["broker_order_id"], row["order_id"]] for row in missing],
    )


def rebuild_position_lots(con: duckdb.DuckDBPyConnection) -> int:
    fills = con.execute(
        """
        SELECT
          f.fill_id,
          CAST(f.ts_fill AS VARCHAR) AS ts_fill,
          CAST(f.qty AS DOUBLE) AS qty,
          CAST(f.price AS DOUBLE) AS price,
          CAST(COALESCE(f.fees_eur, 0) AS DOUBLE) AS fees_eur,
          UPPER(COALESCE(o.symbol, '')) AS symbol,
          UPPER(COALESCE(o.side, '')) AS side
        FROM core.fills f
        JOIN core.orders o ON o.order_id = f.order_id
        ORDER BY f.ts_fill, f.fill_id
        """
    ).fetchall()

    con.execute("DELETE FROM core.position_lots")
    rows = []
    for fill_id, ts_fill, qty, price, fees_eur, symbol, side in fills:
        if side != "BUY":
            continue
        rows.append(
            [
                f"LOT|{fill_id}",
                symbol,
                fill_id,
                ts_fill,
                qty,
                price,
                fees_eur,
                qty,
                "OPEN",
                None,
                None,
                None,
                "FIFO",
                json.dumps({"close_events": [], "realized_pnl_partial": 0.0}, ensure_ascii=False),
            ]
        )
    if rows:
        con.executemany(
            """
            INSERT INTO core.position_lots (
              lot_id, symbol, open_fill_id, open_ts, open_qty, open_price, open_fees_eur,
              remaining_qty, status, close_ts, close_fill_id, realized_pnl_eur, close_method, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def insert_reconciliation_run_and_snapshot(
    con: duckdb.DuckDBPyConnection,
    positions: list[dict[str, Any]],
    ledger: dict[str, Any],
    rates: dict[str, float],
    missing: list[dict[str, Any]],
) -> str:
    ts = iso_now()
    run_id = "RUN_RECON_IBKR_" + now_utc().strftime("%Y%m%d_%H%M%S")
    base = ledger.get("BASE") if isinstance(ledger, dict) else {}
    if not isinstance(base, dict):
        base = {}
    cash_eur = parse_float(base.get("cashbalance"), 0.0)
    equity_eur = parse_float(base.get("stockmarketvalue"), 0.0)
    total_value_eur = parse_float(base.get("netliquidationvalue"), cash_eur + equity_eur)
    initial = parse_float(
        con.execute("SELECT initial_capital_eur FROM cfg.portfolio_config ORDER BY updated_at DESC NULLS LAST LIMIT 1").fetchone()[0],
        10000.0,
    )
    cum_fees = parse_float(con.execute("SELECT COALESCE(SUM(CAST(fees_eur AS DOUBLE)), 0) FROM core.fills").fetchone()[0], 0.0)

    con.execute(
        """
        INSERT INTO core.runs (
          run_id, ts_start, ts_end, tz, strategy_version, config_version, prompt_version,
          model, n8n_execution_id, decision_summary, data_ok_for_trading, price_coverage_pct,
          news_count, ai_cost_eur, expected_fees_eur, warnings_json, agent_output_json, risk_gate_json
        ) VALUES (?, ?, ?, 'Europe/Paris', 'ag1_v4_consensus_reconcile', 'ag1_v4_consensus_v1',
          'manual_reconcile', 'ag1_v4_consensus', 'manual_ibkr_reconcile', 'RECONCILIATION',
          TRUE, NULL, 0, 0, 0, ?, ?, ?)
        ON CONFLICT (run_id) DO NOTHING
        """,
        [
            run_id,
            ts,
            ts,
            json.dumps([], ensure_ascii=False),
            json.dumps({"source": "ibkr_live_reconcile", "missing_fills": [m["fill_id"] for m in missing]}, ensure_ascii=False),
            json.dumps({"source": "ibkr_live_reconcile", "account": EXPECTED_ACCOUNT}, ensure_ascii=False),
        ],
    )

    con.execute("DELETE FROM core.positions_snapshot WHERE run_id = ?", [run_id])
    position_rows = []
    for row in positions:
        symbol = ibkr_internal_symbol(row)
        qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
        if not symbol or qty <= 0:
            continue
        ccy = norm_symbol(row.get("currency"))
        rate = rates.get(ccy, 1.0)
        avg_cost = parse_float(row.get("avgCost") or row.get("avgPrice") or row.get("avg_price"), 0.0) * rate
        market_value = position_market_value_eur(row, rates)
        last_price = market_value / qty if qty else 0.0
        unrealized = (last_price - avg_cost) * qty
        weight = market_value / total_value_eur if total_value_eur > 0 else 0.0
        position_rows.append([run_id, ts, symbol, qty, avg_cost, last_price, money(market_value), money(unrealized), weight])
    if position_rows:
        con.executemany(
            """
            INSERT INTO core.positions_snapshot (
              run_id, ts, symbol, qty, avg_cost, last_price, market_value_eur, unrealized_pnl_eur, weight_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            position_rows,
        )

    con.execute(
        """
        INSERT INTO core.portfolio_snapshot (
          run_id, ts, cash_eur, equity_eur, total_value_eur, cum_fees_eur, cum_ai_cost_eur,
          trades_this_run, total_pnl_eur, roi, drawdown_pct, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 0, ?)
        """,
        [
            run_id,
            ts,
            money(cash_eur),
            money(equity_eur),
            money(total_value_eur),
            money(cum_fees),
            len(missing),
            money(total_value_eur - initial),
            (total_value_eur - initial) / initial if initial else 0.0,
            json.dumps(
                {
                    "source": "ibkr_live_reconcile",
                    "account": EXPECTED_ACCOUNT,
                    "base_ledger": base,
                    "rates": rates,
                },
                ensure_ascii=False,
            ),
        ],
    )

    cash_pct = cash_eur / total_value_eur if total_value_eur > 0 else 0.0
    top1 = max((r[6] for r in position_rows), default=0.0) / total_value_eur if total_value_eur > 0 else 0.0
    con.execute(
        """
        INSERT INTO core.risk_metrics (
          run_id, ts, cash_pct, top1_pos_pct, top1_sector_pct, var95_est_eur,
          positions_count, risk_status, limits_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            ts,
            cash_pct,
            top1,
            top1,
            money(equity_eur * 0.015 * 1.65),
            len(position_rows),
            "BALANCED",
            json.dumps({"source": "ibkr_live_reconcile"}, ensure_ascii=False),
        ],
    )

    price_rows = []
    for row in positions:
        symbol = ibkr_internal_symbol(row)
        qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
        px = position_price_eur(row, rates)
        if symbol and qty > 0 and px > 0:
            price_rows.append([ts, symbol, px, px, px, px, px, None, "ibkr_live_reconcile", ts])
    if price_rows:
        con.executemany(
            """
            INSERT INTO core.market_prices (
              ts, symbol, open, high, low, close, adj_close, volume, source, "asof"
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (ts, symbol, source) DO NOTHING
            """,
            price_rows,
        )

    return run_id


def compare_positions(con: duckdb.DuckDBPyConnection, positions: list[dict[str, Any]], ledger: dict[str, Any], rates: dict[str, float]) -> dict[str, Any]:
    latest = con.execute(
        """
        SELECT run_id, CAST(ts AS VARCHAR), CAST(cash_eur AS DOUBLE), CAST(equity_eur AS DOUBLE), CAST(total_value_eur AS DOUBLE)
        FROM core.portfolio_snapshot ORDER BY ts DESC LIMIT 1
        """
    ).fetchone()
    duck_positions = con.execute(
        """
        SELECT symbol, CAST(qty AS DOUBLE), CAST(market_value_eur AS DOUBLE)
        FROM core.positions_snapshot
        WHERE run_id = (SELECT run_id FROM core.portfolio_snapshot ORDER BY ts DESC LIMIT 1)
        ORDER BY symbol
        """
    ).fetchall()
    ibkr_map = {}
    for row in positions:
        symbol = ibkr_internal_symbol(row)
        qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
        if symbol and qty > 0:
            ibkr_map[symbol] = {"qty": qty, "market_value_eur": money(position_market_value_eur(row, rates))}
    db_map = {sym: {"qty": qty, "market_value_eur": money(mv)} for sym, qty, mv in duck_positions}
    all_symbols = sorted(set(ibkr_map) | set(db_map))
    diffs = []
    for symbol in all_symbols:
        i = ibkr_map.get(symbol, {"qty": 0.0, "market_value_eur": 0.0})
        d = db_map.get(symbol, {"qty": 0.0, "market_value_eur": 0.0})
        if abs(i["qty"] - d["qty"]) > 1e-6 or abs(i["market_value_eur"] - d["market_value_eur"]) > 1.0:
            diffs.append({"symbol": symbol, "ibkr": i, "duckdb": d})
    base = ledger.get("BASE") if isinstance(ledger, dict) else {}
    if not isinstance(base, dict):
        base = {}
    return {
        "duckdb_latest": latest,
        "ibkr_base": {
            "cash": parse_float(base.get("cashbalance"), 0.0),
            "equity": parse_float(base.get("stockmarketvalue"), 0.0),
            "total": parse_float(base.get("netliquidationvalue"), 0.0),
        },
        "position_diffs": diffs,
    }


def make_backup(db_path: str, backup_dir: str | None) -> str:
    src = Path(db_path)
    if not src.exists():
        raise FileNotFoundError(db_path)
    target_dir = Path(backup_dir) if backup_dir else src.parent / "backups"
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = now_utc().strftime("%Y%m%d_%H%M%S")
    dst = target_dir / f"{src.stem}.pre_ibkr_reconcile_{stamp}{src.suffix}"
    shutil.copy2(src, dst)
    return str(dst)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default=os.getenv("AG1_V4_DUCKDB_PATH", DEFAULT_DB_PATH))
    parser.add_argument("--broker-url", default=os.getenv("IBKR_BROKER_URL", DEFAULT_BROKER_URL))
    parser.add_argument("--apply", action="store_true", help="Write changes. Without this, dry-run only.")
    parser.add_argument("--backup-dir", default=None)
    parser.add_argument("--max-missing-fills", type=int, default=20)
    args = parser.parse_args(argv)

    health = fetch_json(args.broker_url, "/health")
    alignment = health.get("account_alignment") if isinstance(health, dict) else {}
    if not health.get("authenticated"):
        raise SystemExit("IBKR broker not authenticated; aborting.")
    if health.get("dry_run"):
        raise SystemExit("IBKR broker is dry_run=true; aborting live reconciliation.")
    if alignment.get("selected_account") != EXPECTED_ACCOUNT or not alignment.get("aligned"):
        raise SystemExit(f"IBKR account alignment mismatch: {alignment}")

    positions = fetch_json(args.broker_url, "/positions")
    fills = fetch_json(args.broker_url, "/fills")
    ledger = fetch_json(args.broker_url, "/account/ledger")
    if not isinstance(positions, list) or not isinstance(fills, list) or not isinstance(ledger, dict):
        raise SystemExit("Unexpected broker payload shape; aborting.")
    rates = exchange_rates_from_ledger(ledger)

    con = duckdb.connect(args.db_path, read_only=not args.apply)
    try:
        before = compare_positions(con, positions, ledger, rates)
        missing, unmatched = build_missing_fills(con, fills, rates)
        if len(missing) > args.max_missing_fills:
            raise SystemExit(f"Too many missing fills ({len(missing)} > {args.max_missing_fills}); aborting.")

        report = {
            "mode": "apply" if args.apply else "dry-run",
            "db_path": args.db_path,
            "broker_url": args.broker_url,
            "before": before,
            "missing_fills": [
                {
                    "fill_id": m["fill_id"],
                    "order_id": m["order_id"],
                    "run_id": m["run_id"],
                    "symbol": m["symbol"],
                    "side": m["side"],
                    "qty": m["qty"],
                    "price_eur": money(m["price"]),
                    "fees_eur": money(m["fees_eur"]),
                    "broker_order_id": m["broker_order_id"],
                    "execution_id": m["broker_execution_id"],
                }
                for m in missing
            ],
            "unmatched_stock_fills": [
                {
                    "execution_id": u.get("execution_id"),
                    "symbol": ibkr_internal_symbol(u),
                    "qty": parse_float(u.get("size"), 0.0),
                    "broker_order_id": u.get("order_id"),
                    "trade_time": u.get("trade_time"),
                }
                for u in unmatched
            ],
        }

        if not args.apply:
            print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
            return 0

        backup = make_backup(args.db_path, args.backup_dir)
        con.close()
        con = duckdb.connect(args.db_path, read_only=False)
        con.execute("BEGIN")
        try:
            upsert_instruments_for_positions(con, positions)
            insert_missing_fills(con, missing)
            lots_count = rebuild_position_lots(con)
            run_id = insert_reconciliation_run_and_snapshot(con, positions, ledger, rates, missing)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        after = compare_positions(con, positions, ledger, rates)
        report["backup"] = backup
        report["reconciliation_run_id"] = run_id
        report["position_lots_rebuilt"] = lots_count
        report["after"] = after
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
