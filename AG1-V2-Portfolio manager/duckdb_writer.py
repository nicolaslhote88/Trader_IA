
"""
AG1 post-agent DuckDB writer.

Public functions:
  - init_schema(db_path)
  - upsert_run_bundle(db_path, bundle_json)
  - compute_snapshots(db_path, run_id)
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import duckdb

DEFAULT_DB_PATH = os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v2.duckdb")
DEFAULT_TZ = "Europe/Paris"
DEFAULT_CONFIG_VERSION = "default_v1"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "sql" / "portfolio_ledger_schema_v2.sql"


@dataclass
class UpsertSummary:
    run_id: str
    db_path: str
    rows: Dict[str, int]

    def as_dict(self) -> Dict[str, Any]:
        return {"run_id": self.run_id, "db_path": self.db_path, "rows": self.rows}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return float(v)
        if isinstance(v, (int, float)):
            n = float(v)
            return n if n == n else default
        s = str(v).strip()
        if not s:
            return default
        s = s.replace("EUR", "").replace("€", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
        s = s.replace(",", ".")
        n = float(s)
        return n if n == n else default
    except Exception:
        return default


def _to_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    n = _to_float(v, None)
    if n is None:
        return default
    try:
        return int(round(n))
    except Exception:
        return default


def _to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _norm_symbol(v: Any) -> str:
    return str(v or "").strip().upper()


def _clean_text(v: Any, max_len: int = 0) -> str:
    s = " ".join(str(v or "").split()).strip()
    if max_len > 0:
        return s[:max_len]
    return s


def _json_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            json.loads(s)
            return s
        except Exception:
            return json.dumps(v, ensure_ascii=False)
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return json.dumps(str(v), ensure_ascii=False)


def _parse_ts(v: Any, fallback: Optional[str] = None) -> str:
    if v is None:
        return fallback or _iso_now()
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip()
        if not s:
            return fallback or _iso_now()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return fallback or _iso_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _split_sql_statements(sql_text: str) -> List[str]:
    statements: List[str] = []
    buff: List[str] = []
    in_single = False
    in_double = False
    for ch in sql_text:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        if ch == ";" and not in_single and not in_double:
            stmt = "".join(buff).strip()
            if stmt:
                statements.append(stmt)
            buff = []
        else:
            buff.append(ch)
    tail = "".join(buff).strip()
    if tail:
        statements.append(tail)
    return statements


@contextmanager
def _db_con(db_path: str, retries: int = 6, base_delay: float = 0.2):
    con = None
    err = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(db_path)
            break
        except Exception as exc:
            err = exc
            msg = str(exc).lower()
            if ("lock" in msg or "busy" in msg) and attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            break
    if con is None:
        raise RuntimeError(f"DuckDB connection failed: {err}")
    try:
        yield con
    finally:
        try:
            con.close()
        except Exception:
            pass
        gc.collect()


def _require_run(bundle: Mapping[str, Any]) -> Dict[str, Any]:
    run = dict(bundle.get("run") or {})
    run_id = _clean_text(run.get("run_id") or run.get("runId"))
    if not run_id:
        run_id = f"RUN_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    run["run_id"] = run_id
    run["ts_start"] = _parse_ts(run.get("ts_start") or run.get("tsStart") or run.get("ts") or run.get("timestamp"))
    run["ts_end"] = _parse_ts(run.get("ts_end") or run.get("tsEnd") or run.get("ts_start"))
    run["tz"] = _clean_text(run.get("tz"), 64) or DEFAULT_TZ
    run["strategy_version"] = _clean_text(run.get("strategy_version") or run.get("strategyVersion"), 128) or None
    run["config_version"] = _clean_text(run.get("config_version") or run.get("configVersion"), 128) or DEFAULT_CONFIG_VERSION
    run["prompt_version"] = _clean_text(run.get("prompt_version") or run.get("promptVersion"), 128) or None
    run["model"] = _clean_text(run.get("model"), 128) or None
    run["n8n_execution_id"] = _clean_text(run.get("n8n_execution_id") or run.get("n8nExecutionId"), 128) or None
    run["decision_summary"] = _clean_text(run.get("decision_summary") or run.get("decisionSummary"), 512) or None
    run["data_ok_for_trading"] = _to_bool(run.get("data_ok_for_trading"), default=True)
    run["price_coverage_pct"] = _to_float(run.get("price_coverage_pct"), None)
    run["news_count"] = _to_int(run.get("news_count"), 0)
    run["ai_cost_eur"] = _to_float(run.get("ai_cost_eur"), 0.0)
    run["expected_fees_eur"] = _to_float(run.get("expected_fees_eur"), 0.0)
    run["warnings_json"] = run.get("warnings_json")
    run["agent_output_json"] = run.get("agent_output_json")
    run["risk_gate_json"] = run.get("risk_gate_json")
    return run


def _upsert_run(con: duckdb.DuckDBPyConnection, run: Mapping[str, Any]) -> int:
    con.execute(
        """
        INSERT INTO core.runs (
          run_id, ts_start, ts_end, tz, strategy_version, config_version, prompt_version, model,
          n8n_execution_id, decision_summary, data_ok_for_trading, price_coverage_pct, news_count,
          ai_cost_eur, expected_fees_eur, warnings_json, agent_output_json, risk_gate_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO UPDATE SET
          ts_start = excluded.ts_start,
          ts_end = excluded.ts_end,
          tz = excluded.tz,
          strategy_version = excluded.strategy_version,
          config_version = excluded.config_version,
          prompt_version = excluded.prompt_version,
          model = excluded.model,
          n8n_execution_id = excluded.n8n_execution_id,
          decision_summary = excluded.decision_summary,
          data_ok_for_trading = excluded.data_ok_for_trading,
          price_coverage_pct = excluded.price_coverage_pct,
          news_count = excluded.news_count,
          ai_cost_eur = excluded.ai_cost_eur,
          expected_fees_eur = excluded.expected_fees_eur,
          warnings_json = excluded.warnings_json,
          agent_output_json = excluded.agent_output_json,
          risk_gate_json = excluded.risk_gate_json
        """,
        [
            run["run_id"],
            run["ts_start"],
            run["ts_end"],
            run["tz"],
            run["strategy_version"],
            run["config_version"],
            run["prompt_version"],
            run["model"],
            run["n8n_execution_id"],
            run["decision_summary"],
            run["data_ok_for_trading"],
            run["price_coverage_pct"],
            run["news_count"],
            run["ai_cost_eur"],
            run["expected_fees_eur"],
            _json_text(run.get("warnings_json")),
            _json_text(run.get("agent_output_json")),
            _json_text(run.get("risk_gate_json")),
        ],
    )
    return 1


def _ensure_instruments_for_symbols(con: duckdb.DuckDBPyConnection, symbols: Iterable[str]) -> int:
    rows = []
    now_ts = _iso_now()
    for symbol in sorted({_norm_symbol(s) for s in symbols if _norm_symbol(s)}):
        rows.append((symbol, None, "Equity", None, "EUR", None, None, None, True, now_ts))
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.instruments (
          symbol, name, asset_class, exchange, currency, isin, sector, industry, is_active, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (symbol) DO UPDATE SET
          updated_at = excluded.updated_at,
          is_active = COALESCE(core.instruments.is_active, excluded.is_active)
        """,
        rows,
    )
    return len(rows)


def _upsert_instruments(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]]) -> int:
    rows: List[Tuple[Any, ...]] = []
    now_ts = _iso_now()
    for r in rows_in:
        symbol = _norm_symbol(r.get("symbol"))
        if not symbol:
            continue
        rows.append(
            (
                symbol,
                _clean_text(r.get("name"), 256) or None,
                _clean_text(r.get("asset_class"), 64) or "Equity",
                _clean_text(r.get("exchange"), 128) or None,
                (_clean_text(r.get("currency"), 3) or "EUR"),
                _clean_text(r.get("isin"), 64) or None,
                _clean_text(r.get("sector"), 128) or None,
                _clean_text(r.get("industry"), 128) or None,
                _to_bool(r.get("is_active"), True),
                _parse_ts(r.get("updated_at"), now_ts),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.instruments (
          symbol, name, asset_class, exchange, currency, isin, sector, industry, is_active, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (symbol) DO UPDATE SET
          name = COALESCE(excluded.name, core.instruments.name),
          asset_class = COALESCE(excluded.asset_class, core.instruments.asset_class),
          exchange = COALESCE(excluded.exchange, core.instruments.exchange),
          currency = COALESCE(excluded.currency, core.instruments.currency),
          isin = COALESCE(excluded.isin, core.instruments.isin),
          sector = COALESCE(excluded.sector, core.instruments.sector),
          industry = COALESCE(excluded.industry, core.instruments.industry),
          is_active = excluded.is_active,
          updated_at = excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def _upsert_market_prices(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], default_ts: str) -> int:
    rows: List[Tuple[Any, ...]] = []
    for r in rows_in:
        symbol = _norm_symbol(r.get("symbol"))
        if not symbol:
            continue
        ts = _parse_ts(r.get("ts"), default_ts)
        source = _clean_text(r.get("source"), 64) or "YF"
        rows.append(
            (
                ts,
                symbol,
                _to_float(r.get("open"), None),
                _to_float(r.get("high"), None),
                _to_float(r.get("low"), None),
                _to_float(r.get("close"), None),
                _to_float(r.get("adj_close"), None),
                _to_int(r.get("volume"), None),
                source,
                _parse_ts(r.get("asof"), ts),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.market_prices (
          ts, symbol, open, high, low, close, adj_close, volume, source, "asof"
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ts, symbol, source) DO UPDATE SET
          open = excluded.open,
          high = excluded.high,
          low = excluded.low,
          close = excluded.close,
          adj_close = excluded.adj_close,
          volume = excluded.volume,
          "asof" = excluded.asof
        """,
        rows,
    )
    return len(rows)

def _upsert_orders(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], run_id: str, default_ts: str) -> int:
    rows: List[Tuple[Any, ...]] = []
    for idx, r in enumerate(rows_in, start=1):
        order_id = _clean_text(r.get("order_id") or r.get("orderId"), 128) or f"ORD|{run_id}|{idx:03d}"
        symbol = _norm_symbol(r.get("symbol"))
        side = _clean_text(r.get("side"), 16).upper()
        if not symbol or side not in {"BUY", "SELL"}:
            continue
        qty = _to_float(r.get("qty"), _to_float(r.get("quantity"), None))
        if qty is None or qty <= 0:
            continue
        rows.append(
            (
                order_id,
                run_id,
                _parse_ts(r.get("ts_created"), default_ts),
                symbol,
                side,
                _clean_text(r.get("intent"), 32) or "REBALANCE",
                _clean_text(r.get("order_type"), 32) or "MARKET",
                qty,
                _to_float(r.get("limit_price"), None),
                _to_float(r.get("stop_price"), None),
                _clean_text(r.get("tif") or r.get("time_in_force"), 16) or "DAY",
                _clean_text(r.get("status"), 32) or "PLANNED",
                _clean_text(r.get("broker"), 64) or "SIM",
                _clean_text(r.get("broker_order_id"), 128) or None,
                _clean_text(r.get("reason"), 512) or None,
                _json_text(r.get("rationale_json")),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.orders (
          order_id, run_id, ts_created, symbol, side, intent, order_type, qty, limit_price,
          stop_price, time_in_force, status, broker, broker_order_id, reason, rationale_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
          run_id = excluded.run_id,
          ts_created = excluded.ts_created,
          symbol = excluded.symbol,
          side = excluded.side,
          intent = excluded.intent,
          order_type = excluded.order_type,
          qty = excluded.qty,
          limit_price = excluded.limit_price,
          stop_price = excluded.stop_price,
          time_in_force = excluded.time_in_force,
          status = excluded.status,
          broker = excluded.broker,
          broker_order_id = excluded.broker_order_id,
          reason = excluded.reason,
          rationale_json = excluded.rationale_json
        """,
        rows,
    )
    return len(rows)


def _upsert_fills(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], run_id: str, default_ts: str) -> int:
    rows: List[Tuple[Any, ...]] = []
    for idx, r in enumerate(rows_in, start=1):
        fill_id = _clean_text(r.get("fill_id") or r.get("fillId"), 128) or f"FIL|{run_id}|{idx:03d}"
        order_id = _clean_text(r.get("order_id") or r.get("orderId"), 128)
        if not order_id:
            continue
        qty = _to_float(r.get("qty"), _to_float(r.get("quantity"), None))
        price = _to_float(r.get("price"), None)
        if qty is None or qty <= 0 or price is None or price <= 0:
            continue
        rows.append(
            (
                fill_id,
                order_id,
                run_id,
                _parse_ts(r.get("ts_fill"), default_ts),
                qty,
                price,
                _to_float(r.get("fees_eur"), 0.0),
                _to_float(r.get("slippage_bps"), None),
                _clean_text(r.get("liquidity"), 32) or "UNKNOWN",
                _json_text(r.get("raw_fill_json")),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.fills (
          fill_id, order_id, run_id, ts_fill, qty, price, fees_eur, slippage_bps, liquidity, raw_fill_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (fill_id) DO UPDATE SET
          order_id = excluded.order_id,
          run_id = excluded.run_id,
          ts_fill = excluded.ts_fill,
          qty = excluded.qty,
          price = excluded.price,
          fees_eur = excluded.fees_eur,
          slippage_bps = excluded.slippage_bps,
          liquidity = excluded.liquidity,
          raw_fill_json = excluded.raw_fill_json
        """,
        rows,
    )
    return len(rows)


def _upsert_cash_ledger(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], run_id: str, default_ts: str) -> int:
    rows: List[Tuple[Any, ...]] = []
    for idx, r in enumerate(rows_in, start=1):
        cash_tx_id = _clean_text(r.get("cash_tx_id") or r.get("cashTxId"), 128) or f"TX|{run_id}|{idx:03d}"
        amount = _to_float(r.get("amount"), None)
        typ = _clean_text(r.get("type"), 32)
        if amount is None or not typ:
            continue
        rows.append(
            (
                cash_tx_id,
                run_id,
                _parse_ts(r.get("ts"), default_ts),
                _clean_text(r.get("currency"), 3) or "EUR",
                amount,
                typ,
                _norm_symbol(r.get("symbol")) or None,
                _clean_text(r.get("ref_id"), 128) or None,
                _clean_text(r.get("notes"), 512) or None,
                _json_text(r.get("payload_json")),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.cash_ledger (
          cash_tx_id, run_id, ts, currency, amount, type, symbol, ref_id, notes, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (cash_tx_id) DO UPDATE SET
          run_id = excluded.run_id,
          ts = excluded.ts,
          currency = excluded.currency,
          amount = excluded.amount,
          type = excluded.type,
          symbol = excluded.symbol,
          ref_id = excluded.ref_id,
          notes = excluded.notes,
          payload_json = excluded.payload_json
        """,
        rows,
    )
    return len(rows)


def _upsert_ai_signals(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], run_id: str, default_ts: str) -> int:
    rows: List[Tuple[Any, ...]] = []
    for idx, r in enumerate(rows_in, start=1):
        signal_id = _clean_text(r.get("signal_id") or r.get("signalId"), 128) or f"SIG|{run_id}|{idx:03d}"
        symbol = _norm_symbol(r.get("symbol"))
        signal = _clean_text(r.get("signal"), 32).upper()
        if not symbol or not signal:
            continue
        conf = _to_int(r.get("confidence"), None)
        if conf is not None:
            conf = max(0, min(100, conf))
        risk_score = _to_int(r.get("risk_score"), None)
        if risk_score is not None:
            risk_score = max(0, min(100, risk_score))
        rows.append(
            (
                signal_id,
                run_id,
                _parse_ts(r.get("ts"), default_ts),
                symbol,
                signal,
                conf,
                _clean_text(r.get("horizon"), 32) or None,
                _clean_text(r.get("entry_zone"), 128) or None,
                _to_float(r.get("stop_loss"), None),
                _to_float(r.get("take_profit"), None),
                risk_score,
                _clean_text(r.get("catalyst"), 256) or None,
                _clean_text(r.get("rationale"), 2048) or None,
                _json_text(r.get("payload_json")),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.ai_signals (
          signal_id, run_id, ts, symbol, signal, confidence, horizon, entry_zone,
          stop_loss, take_profit, risk_score, catalyst, rationale, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (signal_id) DO UPDATE SET
          run_id = excluded.run_id,
          ts = excluded.ts,
          symbol = excluded.symbol,
          signal = excluded.signal,
          confidence = excluded.confidence,
          horizon = excluded.horizon,
          entry_zone = excluded.entry_zone,
          stop_loss = excluded.stop_loss,
          take_profit = excluded.take_profit,
          risk_score = excluded.risk_score,
          catalyst = excluded.catalyst,
          rationale = excluded.rationale,
          payload_json = excluded.payload_json
        """,
        rows,
    )
    return len(rows)


def _upsert_alerts(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], run_id: str, default_ts: str) -> int:
    rows: List[Tuple[Any, ...]] = []
    for idx, r in enumerate(rows_in, start=1):
        alert_id = _clean_text(r.get("alert_id") or r.get("alertId"), 128) or f"ALT|{run_id}|{idx:03d}"
        message = _clean_text(r.get("message"), 2048)
        if not message:
            continue
        rows.append(
            (
                alert_id,
                run_id,
                _parse_ts(r.get("ts"), default_ts),
                _clean_text(r.get("severity"), 16).upper() or "INFO",
                _clean_text(r.get("category"), 32).upper() or "SYSTEM",
                _norm_symbol(r.get("symbol")) or "GLOBAL",
                message,
                _clean_text(r.get("code"), 64) or None,
                _json_text(r.get("payload_json")),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.alerts (
          alert_id, run_id, ts, severity, category, symbol, message, code, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (alert_id) DO UPDATE SET
          run_id = excluded.run_id,
          ts = excluded.ts,
          severity = excluded.severity,
          category = excluded.category,
          symbol = excluded.symbol,
          message = excluded.message,
          code = excluded.code,
          payload_json = excluded.payload_json
        """,
        rows,
    )
    return len(rows)


def _upsert_backfill(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], run_id: str, default_ts: str) -> int:
    rows: List[Tuple[Any, ...]] = []
    for idx, r in enumerate(rows_in, start=1):
        request_id = _clean_text(r.get("request_id") or r.get("requestId"), 160) or f"BF|{run_id}|{idx:03d}"
        symbol = _norm_symbol(r.get("symbol"))
        needs = r.get("needs")
        if isinstance(needs, (list, tuple)):
            needs_csv = ",".join([_clean_text(x, 32) for x in needs if _clean_text(x, 32)])
        else:
            needs_csv = _clean_text(needs, 512)
        if not symbol or not needs_csv:
            continue
        rows.append(
            (
                request_id,
                run_id,
                _parse_ts(r.get("ts"), default_ts),
                symbol,
                needs_csv,
                _clean_text(r.get("severity"), 16).upper() or "MEDIUM",
                _clean_text(r.get("status"), 16).upper() or "OPEN",
                _clean_text(r.get("why"), 2048) or None,
                _parse_ts(r.get("completed_at"), default_ts) if r.get("completed_at") else None,
                _json_text(r.get("response_json")),
                _clean_text(r.get("notes"), 1024) or None,
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.backfill_queue (
          request_id, run_id, ts, symbol, needs, severity, status, why, completed_at, response_json, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (request_id) DO UPDATE SET
          run_id = excluded.run_id,
          ts = excluded.ts,
          symbol = excluded.symbol,
          needs = excluded.needs,
          severity = excluded.severity,
          status = excluded.status,
          why = excluded.why,
          completed_at = excluded.completed_at,
          response_json = excluded.response_json,
          notes = excluded.notes
        """,
        rows,
    )
    return len(rows)

def _upsert_position_lots_rows(con: duckdb.DuckDBPyConnection, rows_in: Sequence[Mapping[str, Any]], default_method: str = "FIFO") -> int:
    rows: List[Tuple[Any, ...]] = []
    for idx, r in enumerate(rows_in, start=1):
        lot_id = _clean_text(r.get("lot_id") or r.get("lotId"), 160) or f"LOT|CUSTOM|{idx:03d}"
        symbol = _norm_symbol(r.get("symbol"))
        open_fill_id = _clean_text(r.get("open_fill_id"), 128)
        open_qty = _to_float(r.get("open_qty"), None)
        open_price = _to_float(r.get("open_price"), None)
        remaining_qty = _to_float(r.get("remaining_qty"), None)
        status = _clean_text(r.get("status"), 16).upper() or "OPEN"
        if not symbol or not open_fill_id or open_qty is None or open_price is None or remaining_qty is None:
            continue
        rows.append(
            (
                lot_id,
                symbol,
                open_fill_id,
                _parse_ts(r.get("open_ts"), _iso_now()),
                open_qty,
                open_price,
                _to_float(r.get("open_fees_eur"), 0.0),
                remaining_qty,
                status,
                _parse_ts(r.get("close_ts"), _iso_now()) if r.get("close_ts") else None,
                _clean_text(r.get("close_fill_id"), 128) or None,
                _to_float(r.get("realized_pnl_eur"), None),
                _clean_text(r.get("close_method"), 16) or default_method,
                _json_text(r.get("meta_json")),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.position_lots (
          lot_id, symbol, open_fill_id, open_ts, open_qty, open_price, open_fees_eur, remaining_qty,
          status, close_ts, close_fill_id, realized_pnl_eur, close_method, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (lot_id) DO UPDATE SET
          symbol = excluded.symbol,
          open_fill_id = excluded.open_fill_id,
          open_ts = excluded.open_ts,
          open_qty = excluded.open_qty,
          open_price = excluded.open_price,
          open_fees_eur = excluded.open_fees_eur,
          remaining_qty = excluded.remaining_qty,
          status = excluded.status,
          close_ts = excluded.close_ts,
          close_fill_id = excluded.close_fill_id,
          realized_pnl_eur = excluded.realized_pnl_eur,
          close_method = excluded.close_method,
          meta_json = excluded.meta_json
        """,
        rows,
    )
    return len(rows)


def _rebuild_position_lots_from_fills(con: duckdb.DuckDBPyConnection) -> int:
    fills = con.execute(
        """
        SELECT
          f.fill_id,
          f.order_id,
          f.ts_fill,
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

    lots_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    rows_out: List[Dict[str, Any]] = []

    for fill_id, _order_id, ts_fill, qty, price, fees_eur, symbol, side in fills:
        symbol = _norm_symbol(symbol)
        side = _clean_text(side, 8).upper()
        qty = float(qty or 0.0)
        price = float(price or 0.0)
        fees_eur = float(fees_eur or 0.0)
        if not symbol or qty <= 0 or price <= 0:
            continue

        symbol_lots = lots_by_symbol.setdefault(symbol, [])

        if side == "BUY":
            lot = {
                "lot_id": f"LOT|{fill_id}",
                "symbol": symbol,
                "open_fill_id": fill_id,
                "open_ts": _parse_ts(ts_fill),
                "open_qty": qty,
                "open_price": price,
                "open_fees_eur": fees_eur,
                "remaining_qty": qty,
                "status": "OPEN",
                "close_ts": None,
                "close_fill_id": None,
                "realized_pnl_eur": None,
                "close_method": "FIFO",
                "meta_json": {"close_events": [], "realized_pnl_partial": 0.0},
            }
            symbol_lots.append(lot)
            rows_out.append(lot)
            continue

        if side != "SELL":
            continue

        remaining_to_close = qty
        while remaining_to_close > 1e-12:
            open_lot = next((l for l in symbol_lots if l["status"] == "OPEN" and float(l["remaining_qty"]) > 1e-12), None)
            if open_lot is None:
                break

            available = float(open_lot["remaining_qty"])
            take_qty = min(available, remaining_to_close)
            fee_alloc = fees_eur * (take_qty / qty) if qty > 0 else 0.0
            realized_inc = (price - float(open_lot["open_price"])) * take_qty - fee_alloc

            meta = dict(open_lot.get("meta_json") or {})
            events = list(meta.get("close_events") or [])
            events.append(
                {
                    "fill_id": fill_id,
                    "ts": _parse_ts(ts_fill),
                    "qty": take_qty,
                    "price": price,
                    "fee_alloc_eur": fee_alloc,
                    "realized_increment_eur": realized_inc,
                }
            )
            meta["close_events"] = events
            meta["realized_pnl_partial"] = float(meta.get("realized_pnl_partial") or 0.0) + realized_inc
            open_lot["meta_json"] = meta

            new_remaining = available - take_qty
            open_lot["remaining_qty"] = new_remaining
            if new_remaining <= 1e-12:
                open_lot["remaining_qty"] = 0.0
                open_lot["status"] = "CLOSED"
                open_lot["close_ts"] = _parse_ts(ts_fill)
                open_lot["close_fill_id"] = fill_id
                open_lot["realized_pnl_eur"] = round(float(meta.get("realized_pnl_partial") or 0.0), 2)
            remaining_to_close -= take_qty

    if not rows_out:
        return 0
    return _upsert_position_lots_rows(con, rows_out, default_method="FIFO")


def _collect_bundle_symbols(bundle: Mapping[str, Any]) -> List[str]:
    out: set[str] = set()
    for row in bundle.get("instruments") or []:
        s = _norm_symbol((row or {}).get("symbol"))
        if s:
            out.add(s)
    for section in ("orders", "market_prices", "ai_signals", "backfill_queue"):
        for row in bundle.get(section) or []:
            s = _norm_symbol((row or {}).get("symbol"))
            if s:
                out.add(s)
    return sorted(out)


def _upsert_positions_snapshot_rows(
    con: duckdb.DuckDBPyConnection, run_id: str, ts: str, rows_in: Sequence[Mapping[str, Any]]
) -> int:
    con.execute("DELETE FROM core.positions_snapshot WHERE run_id = ?", [run_id])
    rows: List[Tuple[Any, ...]] = []
    for r in rows_in:
        symbol = _norm_symbol(r.get("symbol"))
        qty = _to_float(r.get("qty"), None)
        if not symbol or qty is None or qty <= 0:
            continue
        rows.append(
            (
                run_id,
                _parse_ts(r.get("ts"), ts),
                symbol,
                qty,
                _to_float(r.get("avg_cost"), None),
                _to_float(r.get("last_price"), None),
                _to_float(r.get("market_value_eur"), 0.0),
                _to_float(r.get("unrealized_pnl_eur"), 0.0),
                _to_float(r.get("weight_pct"), 0.0),
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.positions_snapshot (
          run_id, ts, symbol, qty, avg_cost, last_price, market_value_eur, unrealized_pnl_eur, weight_pct
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id, symbol) DO UPDATE SET
          ts = excluded.ts,
          qty = excluded.qty,
          avg_cost = excluded.avg_cost,
          last_price = excluded.last_price,
          market_value_eur = excluded.market_value_eur,
          unrealized_pnl_eur = excluded.unrealized_pnl_eur,
          weight_pct = excluded.weight_pct
        """,
        rows,
    )
    return len(rows)


def _upsert_portfolio_snapshot_row(
    con: duckdb.DuckDBPyConnection, run_id: str, ts: str, row: Mapping[str, Any]
) -> int:
    con.execute(
        """
        INSERT INTO core.portfolio_snapshot (
          run_id, ts, cash_eur, equity_eur, total_value_eur, cum_fees_eur, cum_ai_cost_eur,
          trades_this_run, total_pnl_eur, roi, drawdown_pct, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO UPDATE SET
          ts = excluded.ts,
          cash_eur = excluded.cash_eur,
          equity_eur = excluded.equity_eur,
          total_value_eur = excluded.total_value_eur,
          cum_fees_eur = excluded.cum_fees_eur,
          cum_ai_cost_eur = excluded.cum_ai_cost_eur,
          trades_this_run = excluded.trades_this_run,
          total_pnl_eur = excluded.total_pnl_eur,
          roi = excluded.roi,
          drawdown_pct = excluded.drawdown_pct,
          meta_json = excluded.meta_json
        """,
        [
            run_id,
            _parse_ts(row.get("ts"), ts),
            _to_float(row.get("cash_eur"), 0.0),
            _to_float(row.get("equity_eur"), 0.0),
            _to_float(row.get("total_value_eur"), 0.0),
            _to_float(row.get("cum_fees_eur"), 0.0),
            _to_float(row.get("cum_ai_cost_eur"), 0.0),
            _to_int(row.get("trades_this_run"), 0),
            _to_float(row.get("total_pnl_eur"), 0.0),
            _to_float(row.get("roi"), 0.0),
            _to_float(row.get("drawdown_pct"), 0.0),
            _json_text(row.get("meta_json")),
        ],
    )
    return 1


def _upsert_risk_metrics_row(
    con: duckdb.DuckDBPyConnection, run_id: str, ts: str, row: Mapping[str, Any]
) -> int:
    con.execute(
        """
        INSERT INTO core.risk_metrics (
          run_id, ts, cash_pct, top1_pos_pct, top1_sector_pct, var95_est_eur,
          positions_count, risk_status, limits_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO UPDATE SET
          ts = excluded.ts,
          cash_pct = excluded.cash_pct,
          top1_pos_pct = excluded.top1_pos_pct,
          top1_sector_pct = excluded.top1_sector_pct,
          var95_est_eur = excluded.var95_est_eur,
          positions_count = excluded.positions_count,
          risk_status = excluded.risk_status,
          limits_json = excluded.limits_json
        """,
        [
            run_id,
            _parse_ts(row.get("ts"), ts),
            _to_float(row.get("cash_pct"), 0.0),
            _to_float(row.get("top1_pos_pct"), 0.0),
            _to_float(row.get("top1_sector_pct"), 0.0),
            _to_float(row.get("var95_est_eur"), 0.0),
            _to_int(row.get("positions_count"), 0),
            _clean_text(row.get("risk_status"), 32) or "BALANCED",
            _json_text(row.get("limits_json")),
        ],
    )
    return 1

def _compute_snapshots_with_con(con: duckdb.DuckDBPyConnection, run_id: str) -> Dict[str, Any]:
    run_row = con.execute(
        """
        SELECT run_id, ts_end, ts_start, COALESCE(config_version, ?)
        FROM core.runs
        WHERE run_id = ?
        """,
        [DEFAULT_CONFIG_VERSION, run_id],
    ).fetchone()
    if run_row is None:
        raise ValueError(f"run_id not found in core.runs: {run_id}")

    _, ts_end, ts_start, config_version = run_row
    ts = _parse_ts(ts_end or ts_start or _iso_now())

    cfg_row = con.execute(
        """
        SELECT
          initial_capital_eur,
          lot_close_method,
          default_fee_bps,
          kill_switch_active,
          max_pos_pct,
          max_sector_pct,
          max_daily_drawdown_pct
        FROM cfg.portfolio_config
        WHERE config_version = ?
        """,
        [config_version],
    ).fetchone()
    if cfg_row is None:
        cfg_row = con.execute(
            """
            SELECT
              initial_capital_eur,
              lot_close_method,
              default_fee_bps,
              kill_switch_active,
              max_pos_pct,
              max_sector_pct,
              max_daily_drawdown_pct
            FROM cfg.portfolio_config
            WHERE config_version = ?
            """,
            [DEFAULT_CONFIG_VERSION],
        ).fetchone()
    initial_capital = _to_float(cfg_row[0] if cfg_row else None, 0.0) or 0.0
    kill_switch_active = _to_bool(cfg_row[3] if cfg_row else False, False)
    max_pos_pct = _to_float(cfg_row[4] if cfg_row else None, None)
    max_sector_pct = _to_float(cfg_row[5] if cfg_row else None, None)
    max_daily_drawdown_pct = _to_float(cfg_row[6] if cfg_row else None, None)

    cash_eur = float(
        con.execute("SELECT COALESCE(SUM(CAST(amount AS DOUBLE)), 0) FROM core.cash_ledger").fetchone()[0] or 0.0
    )
    cum_fees_eur = float(
        con.execute("SELECT COALESCE(SUM(CAST(fees_eur AS DOUBLE)), 0) FROM core.fills").fetchone()[0] or 0.0
    )
    cum_ai_cost_eur = float(
        con.execute(
            """
            SELECT COALESCE(SUM(
              CASE
                WHEN UPPER(type) = 'AI_COST' AND amount < 0 THEN -CAST(amount AS DOUBLE)
                WHEN UPPER(type) = 'AI_COST' THEN CAST(amount AS DOUBLE)
                ELSE 0
              END
            ), 0)
            FROM core.cash_ledger
            """
        ).fetchone()[0]
        or 0.0
    )
    trades_this_run = int(
        con.execute("SELECT COUNT(*) FROM core.fills WHERE run_id = ?", [run_id]).fetchone()[0] or 0
    )

    lot_rows = con.execute(
        """
        SELECT
          symbol,
          SUM(CAST(remaining_qty AS DOUBLE)) AS qty,
          SUM(CAST(remaining_qty AS DOUBLE) * CAST(open_price AS DOUBLE)) /
            NULLIF(SUM(CAST(remaining_qty AS DOUBLE)), 0) AS avg_cost
        FROM core.position_lots
        WHERE status = 'OPEN' AND CAST(remaining_qty AS DOUBLE) > 0
        GROUP BY symbol
        """
    ).fetchall()

    price_rows = con.execute(
        """
        SELECT symbol, close
        FROM (
          SELECT
            symbol,
            close,
            ROW_NUMBER() OVER (
              PARTITION BY symbol
              ORDER BY COALESCE("asof", ts) DESC, ts DESC, source DESC
            ) AS rn
          FROM core.market_prices
        )
        WHERE rn = 1
        """
    ).fetchall()
    last_price_by_symbol = {_norm_symbol(s): float(p) for s, p in price_rows if s and p is not None}

    sector_rows = con.execute(
        "SELECT symbol, COALESCE(sector, 'UNKNOWN') FROM core.instruments"
    ).fetchall()
    sector_by_symbol = {_norm_symbol(s): _clean_text(sec, 128) or "UNKNOWN" for s, sec in sector_rows}

    positions: List[Dict[str, Any]] = []
    equity_eur = 0.0
    sector_totals: Dict[str, float] = {}
    for symbol, qty, avg_cost in lot_rows:
        symbol = _norm_symbol(symbol)
        qty = float(qty or 0.0)
        if qty <= 0:
            continue
        avg_cost = float(avg_cost or 0.0)
        last_price = float(last_price_by_symbol.get(symbol, avg_cost))
        market_value = qty * last_price
        unrealized = (last_price - avg_cost) * qty
        equity_eur += market_value
        sector = sector_by_symbol.get(symbol, "UNKNOWN")
        sector_totals[sector] = sector_totals.get(sector, 0.0) + market_value
        positions.append(
            {
                "symbol": symbol,
                "qty": qty,
                "avg_cost": avg_cost,
                "last_price": last_price,
                "market_value_eur": round(market_value, 2),
                "unrealized_pnl_eur": round(unrealized, 2),
                "weight_pct": 0.0,
                "ts": ts,
            }
        )

    total_value_eur = cash_eur + equity_eur
    for p in positions:
        p["weight_pct"] = (p["market_value_eur"] / total_value_eur) if total_value_eur > 0 else 0.0

    pos_rows_written = _upsert_positions_snapshot_rows(con, run_id, ts, positions)

    top1_pos_pct = 0.0
    if positions and total_value_eur > 0:
        top1_pos_pct = max(p["market_value_eur"] for p in positions) / total_value_eur
    top1_sector_pct = 0.0
    if sector_totals and total_value_eur > 0:
        top1_sector_pct = max(sector_totals.values()) / total_value_eur

    peak_total_value = float(
        con.execute("SELECT COALESCE(MAX(CAST(total_value_eur AS DOUBLE)), 0) FROM core.portfolio_snapshot").fetchone()[0]
        or 0.0
    )
    reference_peak = max(peak_total_value, total_value_eur, 1e-9)
    drawdown_pct = (total_value_eur / reference_peak) - 1.0

    total_pnl_eur = total_value_eur - initial_capital if initial_capital else 0.0
    roi = (total_pnl_eur / initial_capital) if initial_capital else 0.0
    cash_pct = (cash_eur / total_value_eur) if total_value_eur > 0 else 0.0
    var95_est_eur = equity_eur * 0.015 * 1.65

    risk_status = "BALANCED"
    if kill_switch_active and max_daily_drawdown_pct is not None and abs(drawdown_pct) * 100 >= max_daily_drawdown_pct:
        risk_status = "RISK_OFF"
    elif cash_pct >= 0.80:
        risk_status = "DEFENSIVE"
    elif cash_pct <= 0.10:
        risk_status = "RISK_ON"

    limits_json = {
        "kill_switch_active": kill_switch_active,
        "max_pos_pct": max_pos_pct,
        "max_sector_pct": max_sector_pct,
        "max_daily_drawdown_pct": max_daily_drawdown_pct,
        "breaches": {
            "max_pos_pct": (top1_pos_pct * 100 > max_pos_pct) if max_pos_pct is not None else False,
            "max_sector_pct": (top1_sector_pct * 100 > max_sector_pct) if max_sector_pct is not None else False,
            "daily_drawdown_pct": (abs(drawdown_pct) * 100 > max_daily_drawdown_pct)
            if max_daily_drawdown_pct is not None
            else False,
        },
    }

    _upsert_portfolio_snapshot_row(
        con,
        run_id,
        ts,
        {
            "ts": ts,
            "cash_eur": round(cash_eur, 2),
            "equity_eur": round(equity_eur, 2),
            "total_value_eur": round(total_value_eur, 2),
            "cum_fees_eur": round(cum_fees_eur, 2),
            "cum_ai_cost_eur": round(cum_ai_cost_eur, 2),
            "trades_this_run": trades_this_run,
            "total_pnl_eur": round(total_pnl_eur, 2),
            "roi": roi,
            "drawdown_pct": drawdown_pct,
            "meta_json": {"config_version": config_version},
        },
    )
    _upsert_risk_metrics_row(
        con,
        run_id,
        ts,
        {
            "ts": ts,
            "cash_pct": cash_pct,
            "top1_pos_pct": top1_pos_pct,
            "top1_sector_pct": top1_sector_pct,
            "var95_est_eur": round(var95_est_eur, 2),
            "positions_count": len(positions),
            "risk_status": risk_status,
            "limits_json": limits_json,
        },
    )

    return {
        "run_id": run_id,
        "ts": ts,
        "cash_eur": round(cash_eur, 2),
        "equity_eur": round(equity_eur, 2),
        "total_value_eur": round(total_value_eur, 2),
        "positions_count": len(positions),
        "positions_rows_written": pos_rows_written,
        "trades_this_run": trades_this_run,
        "risk_status": risk_status,
    }


def init_schema(db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    sql_path = Path(os.getenv("AG1_LEDGER_SCHEMA_PATH", str(SCHEMA_PATH)))
    if not sql_path.exists():
        raise FileNotFoundError(f"Schema SQL not found: {sql_path}")
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)
    with _db_con(db_path) as con:
        for stmt in statements:
            con.execute(stmt)
        con.execute(
            """
            INSERT INTO cfg.portfolio_config (
              config_version, initial_capital_eur, lot_close_method, default_fee_bps, kill_switch_active,
              max_pos_pct, max_sector_pct, max_daily_drawdown_pct, updated_at, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (config_version) DO NOTHING
            """,
            [
                DEFAULT_CONFIG_VERSION,
                50000.00,
                "FIFO",
                10.0,
                True,
                25.0,
                40.0,
                6.0,
                _iso_now(),
                _json_text({"seeded_by": "duckdb_writer.init_schema"}),
            ],
        )
    return {"ok": True, "db_path": db_path, "schema_path": str(sql_path), "statements": len(statements)}


def upsert_run_bundle(db_path: str, bundle_json: Any) -> Dict[str, Any]:
    if isinstance(bundle_json, str):
        bundle = json.loads(bundle_json)
    else:
        bundle = dict(bundle_json or {})
    run = _require_run(bundle)
    run_id = run["run_id"]
    default_ts = run["ts_end"]

    init_schema(db_path)

    rows = {
        "runs": 0,
        "instruments": 0,
        "orders": 0,
        "fills": 0,
        "cash_ledger": 0,
        "position_lots": 0,
        "market_prices": 0,
        "ai_signals": 0,
        "alerts": 0,
        "backfill_queue": 0,
    }

    with _db_con(db_path) as con:
        con.execute("BEGIN")
        try:
            rows["runs"] = _upsert_run(con, run)
            rows["instruments"] += _upsert_instruments(con, bundle.get("instruments") or [])

            symbols_to_seed = _collect_bundle_symbols(bundle)
            rows["instruments"] += _ensure_instruments_for_symbols(con, symbols_to_seed)

            rows["market_prices"] = _upsert_market_prices(con, bundle.get("market_prices") or [], default_ts)
            rows["orders"] = _upsert_orders(con, bundle.get("orders") or [], run_id, default_ts)
            rows["fills"] = _upsert_fills(con, bundle.get("fills") or [], run_id, default_ts)
            rows["cash_ledger"] = _upsert_cash_ledger(con, bundle.get("cash_ledger") or [], run_id, default_ts)

            lots_changes = bundle.get("lots_changes") or []
            if lots_changes:
                rows["position_lots"] = _upsert_position_lots_rows(con, lots_changes)
            else:
                rows["position_lots"] = _rebuild_position_lots_from_fills(con)

            rows["ai_signals"] = _upsert_ai_signals(con, bundle.get("ai_signals") or [], run_id, default_ts)
            rows["alerts"] = _upsert_alerts(con, bundle.get("alerts") or [], run_id, default_ts)
            rows["backfill_queue"] = _upsert_backfill(con, bundle.get("backfill_queue") or [], run_id, default_ts)

            snapshots = bundle.get("snapshots") or {}
            if snapshots.get("positions"):
                _upsert_positions_snapshot_rows(con, run_id, default_ts, snapshots.get("positions") or [])
            if snapshots.get("portfolio"):
                _upsert_portfolio_snapshot_row(con, run_id, default_ts, snapshots.get("portfolio") or {})
            if snapshots.get("risk"):
                _upsert_risk_metrics_row(con, run_id, default_ts, snapshots.get("risk") or {})

            snapshot_summary = _compute_snapshots_with_con(con, run_id)

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    out = UpsertSummary(run_id=run_id, db_path=db_path, rows=rows).as_dict()
    out["snapshots"] = snapshot_summary
    return out


def compute_snapshots(db_path: str, run_id: str) -> Dict[str, Any]:
    init_schema(db_path)
    with _db_con(db_path) as con:
        con.execute("BEGIN")
        try:
            result = _compute_snapshots_with_con(con, run_id)
            con.execute("COMMIT")
            return result
        except Exception:
            con.execute("ROLLBACK")
            raise

def _cli_init_schema(args: argparse.Namespace) -> None:
    result = init_schema(args.db)
    print(json.dumps(result, ensure_ascii=False))


def _cli_upsert_bundle(args: argparse.Namespace) -> None:
    if args.bundle_file:
        bundle_text = Path(args.bundle_file).read_text(encoding="utf-8")
    else:
        bundle_text = args.bundle_json
    if not bundle_text:
        raise ValueError("Provide --bundle-json or --bundle-file")
    result = upsert_run_bundle(args.db, bundle_text)
    print(json.dumps(result, ensure_ascii=False))


def _cli_compute_snapshots(args: argparse.Namespace) -> None:
    result = compute_snapshots(args.db, args.run_id)
    print(json.dumps(result, ensure_ascii=False))


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="AG1 portfolio ledger DuckDB writer")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-schema", help="Create schemas/tables and seed default config")
    p_init.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB file path")
    p_init.set_defaults(func=_cli_init_schema)

    p_upsert = sub.add_parser("upsert-run-bundle", help="Upsert run bundle payload")
    p_upsert.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB file path")
    p_upsert.add_argument("--bundle-json", default="", help="JSON string payload")
    p_upsert.add_argument("--bundle-file", default="", help="Path to JSON payload file")
    p_upsert.set_defaults(func=_cli_upsert_bundle)

    p_snap = sub.add_parser("compute-snapshots", help="Recompute snapshots for run_id")
    p_snap.add_argument("--db", default=DEFAULT_DB_PATH, help="DuckDB file path")
    p_snap.add_argument("--run-id", required=True, help="Run id")
    p_snap.set_defaults(func=_cli_compute_snapshots)

    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
