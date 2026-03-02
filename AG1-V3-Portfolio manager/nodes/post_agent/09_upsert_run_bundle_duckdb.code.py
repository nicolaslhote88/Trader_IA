import importlib.util
import json
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DEFAULT_DB_PATH = os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v3.duckdb")
DEFAULT_WRITER_PATH = os.getenv(
    "AG1_DUCKDB_WRITER_PATH",
    "/files/AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py",
)
DEFAULT_SCHEMA_PATH = os.getenv(
    "AG1_LEDGER_SCHEMA_PATH",
    "/files/AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql",
)
SCHEMA_ENV = "AG1_LEDGER_SCHEMA_PATH"
INLINE_WRITER_SENTINEL = "__INLINE_WRITER__"

STATIC_WRITER_PATHS = (
    "/files/AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-EXPORT/workflow/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
)
STATIC_SCHEMA_PATHS = (
    "/files/AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-EXPORT/workflow/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
)


def _clean_path_text(value):
    return str(value or "").strip()


def _iter_candidate_paths(*candidates):
    seen = set()
    for candidate in candidates:
        text = _clean_path_text(candidate)
        if not text:
            continue
        path = Path(text).expanduser()
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        yield path


def _first_existing_file(*candidates):
    attempted = []
    for path in _iter_candidate_paths(*candidates):
        attempted.append(str(path))
        if path.is_file():
            return str(path), attempted
    return "", attempted


def _resolve_writer_path(preferred_path=""):
    cwd = Path.cwd()
    found, attempted = _first_existing_file(
        preferred_path,
        os.getenv("AG1_DUCKDB_WRITER_PATH", ""),
        DEFAULT_WRITER_PATH,
        *STATIC_WRITER_PATHS,
        cwd / "nodes/post_agent/duckdb_writer.py",
        cwd / "workflow/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
    )
    if found:
        os.environ["AG1_DUCKDB_WRITER_PATH"] = found
        return found

    # Fallback autonome: le noeud embarque un writer minimal inline.
    os.environ["AG1_DUCKDB_WRITER_PATH"] = INLINE_WRITER_SENTINEL
    return INLINE_WRITER_SENTINEL


def _resolve_schema_path(preferred_schema_path="", writer_path_text=""):
    cwd = Path.cwd()
    writer_root = None
    if _clean_path_text(writer_path_text):
        writer_path = Path(writer_path_text)
        if writer_path.name == "duckdb_writer.py":
            # .../nodes/post_agent/duckdb_writer.py -> pack root
            try:
                writer_root = writer_path.parents[2]
            except Exception:
                writer_root = None

    found, _ = _first_existing_file(
        preferred_schema_path,
        os.getenv(SCHEMA_ENV, ""),
        DEFAULT_SCHEMA_PATH,
        *STATIC_SCHEMA_PATHS,
        (writer_root / "sql/portfolio_ledger_schema_v2.sql") if writer_root else "",
        (writer_root / "workflow/sql/portfolio_ledger_schema_v2.sql") if writer_root else "",
        cwd / "sql/portfolio_ledger_schema_v2.sql",
        cwd / "workflow/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
    )
    if found:
        os.environ[SCHEMA_ENV] = found
        return found
    return ""


def _iso_now():
    return datetime.now(timezone.utc).isoformat()


def _to_float(v, default=0.0):
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return float(v)
        if isinstance(v, (int, float)):
            n = float(v)
            return n if n == n else default
        s = str(v).strip().replace(",", ".")
        if not s:
            return default
        n = float(s)
        return n if n == n else default
    except Exception:
        return default


def _to_int(v, default=0):
    n = _to_float(v, None)
    if n is None:
        return default
    try:
        return int(round(n))
    except Exception:
        return default


def _norm_symbol(v):
    return str(v or "").strip().upper()


def _parse_ts(v, fallback=None):
    text = str(v or "").strip()
    if not text:
        return fallback or _iso_now()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return fallback or _iso_now()


def _json_text(v):
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


def _extract_run_id(bundle):
    run = dict(bundle.get("run") or {})
    run_id = str(run.get("run_id") or run.get("runId") or "").strip()
    if not run_id:
        run_id = f"RUN_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    ts = _parse_ts(run.get("ts_end") or run.get("tsEnd") or run.get("ts_start") or run.get("ts") or run.get("timestamp"))
    return run_id, ts, run


def _build_positions_from_bundle(bundle):
    orders = bundle.get("orders") or []
    fills = bundle.get("fills") or []
    prices = bundle.get("market_prices") or []
    cash_rows = bundle.get("cash_ledger") or []

    order_by_id = {}
    for o in orders:
        oid = str(o.get("order_id") or o.get("orderId") or "").strip()
        if oid:
            order_by_id[oid] = o

    last_price = {}
    for p in prices:
        sym = _norm_symbol(p.get("symbol"))
        if not sym:
            continue
        px = _to_float(p.get("close"), None)
        if px is None:
            px = _to_float(p.get("lastPrice"), None)
        if px is None:
            continue
        last_price[sym] = px

    state = {}
    for f in fills:
        oid = str(f.get("order_id") or f.get("orderId") or "").strip()
        order = order_by_id.get(oid, {})
        sym = _norm_symbol(order.get("symbol") or f.get("symbol"))
        side = str(order.get("side") or f.get("side") or "").strip().upper()
        qty = _to_float(f.get("qty"), _to_float(f.get("quantity"), 0.0))
        px = _to_float(f.get("price"), 0.0)
        if not sym or side not in {"BUY", "SELL"} or qty <= 0 or px <= 0:
            continue
        st = state.setdefault(sym, {"qty": 0.0, "avg": 0.0})
        if side == "BUY":
            q0 = st["qty"]
            q1 = q0 + qty
            st["avg"] = ((q0 * st["avg"]) + (qty * px)) / q1 if q1 > 0 else 0.0
            st["qty"] = q1
        else:
            q0 = st["qty"]
            close_qty = min(qty, q0)
            st["qty"] = max(0.0, q0 - close_qty)
            if st["qty"] <= 1e-12:
                st["qty"] = 0.0
                st["avg"] = 0.0

    positions = []
    equity_eur = 0.0
    for sym, st in state.items():
        qty = float(st.get("qty") or 0.0)
        avg = float(st.get("avg") or 0.0)
        if qty <= 0 or avg <= 0:
            continue
        px = float(last_price.get(sym, avg))
        mv = qty * px
        upnl = (px - avg) * qty
        equity_eur += mv
        positions.append(
            {
                "symbol": sym,
                "qty": qty,
                "avg_cost": avg,
                "last_price": px,
                "market_value_eur": mv,
                "unrealized_pnl_eur": upnl,
            }
        )

    cash_eur = 0.0
    for c in cash_rows:
        cash_eur += _to_float(c.get("amount"), 0.0)

    total_value = cash_eur + equity_eur
    for p in positions:
        p["weight_pct"] = (p["market_value_eur"] / total_value) if total_value > 0 else 0.0
    return positions, cash_eur, equity_eur, total_value


def _inline_ensure_schema(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS core")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.runs (
          run_id VARCHAR PRIMARY KEY,
          ts_start TIMESTAMP,
          ts_end TIMESTAMP,
          tz VARCHAR,
          strategy_version VARCHAR,
          config_version VARCHAR,
          prompt_version VARCHAR,
          model VARCHAR,
          n8n_execution_id VARCHAR,
          decision_summary VARCHAR,
          data_ok_for_trading BOOLEAN,
          price_coverage_pct DOUBLE,
          news_count BIGINT,
          ai_cost_eur DOUBLE,
          expected_fees_eur DOUBLE,
          warnings_json JSON,
          agent_output_json JSON,
          risk_gate_json JSON
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.instruments (
          symbol VARCHAR PRIMARY KEY,
          name VARCHAR,
          asset_class VARCHAR,
          exchange VARCHAR,
          currency VARCHAR,
          isin VARCHAR,
          sector VARCHAR,
          industry VARCHAR,
          is_active BOOLEAN,
          updated_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.positions_snapshot (
          run_id VARCHAR,
          ts TIMESTAMP,
          symbol VARCHAR,
          qty DOUBLE,
          avg_cost DOUBLE,
          last_price DOUBLE,
          market_value_eur DOUBLE,
          unrealized_pnl_eur DOUBLE,
          weight_pct DOUBLE,
          PRIMARY KEY(run_id, symbol)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.portfolio_snapshot (
          run_id VARCHAR PRIMARY KEY,
          ts TIMESTAMP,
          cash_eur DOUBLE,
          equity_eur DOUBLE,
          total_value_eur DOUBLE,
          cum_fees_eur DOUBLE,
          cum_ai_cost_eur DOUBLE,
          trades_this_run BIGINT,
          total_pnl_eur DOUBLE,
          roi DOUBLE,
          drawdown_pct DOUBLE,
          meta_json JSON
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.alerts (
          alert_id VARCHAR PRIMARY KEY,
          run_id VARCHAR,
          ts TIMESTAMP,
          severity VARCHAR,
          category VARCHAR,
          symbol VARCHAR,
          message VARCHAR,
          code VARCHAR,
          payload_json JSON
        )
        """
    )


def _inline_init_schema(db_path):
    with duckdb.connect(db_path) as con:
        _inline_ensure_schema(con)
    return {"ok": True, "db_path": db_path, "schema_path": "INLINE_SCHEMA_MINIMAL"}


def _inline_upsert_run_bundle(db_path, bundle_json):
    bundle = json.loads(bundle_json) if isinstance(bundle_json, str) else dict(bundle_json or {})
    run_id, ts, run = _extract_run_id(bundle)
    positions, cash_eur, equity_eur, total_value = _build_positions_from_bundle(bundle)

    with duckdb.connect(db_path) as con:
        con.execute("BEGIN")
        try:
            _inline_ensure_schema(con)
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
                    run_id,
                    ts,
                    ts,
                    str(run.get("tz") or "Europe/Paris"),
                    run.get("strategy_version") or run.get("strategyVersion"),
                    run.get("config_version") or run.get("configVersion"),
                    run.get("prompt_version") or run.get("promptVersion"),
                    run.get("model"),
                    run.get("n8n_execution_id") or run.get("n8nExecutionId"),
                    run.get("decision_summary") or run.get("decisionSummary"),
                    bool(run.get("data_ok_for_trading", True)),
                    _to_float(run.get("price_coverage_pct"), None),
                    _to_int(run.get("news_count"), 0),
                    _to_float(run.get("ai_cost_eur"), 0.0),
                    _to_float(run.get("expected_fees_eur"), 0.0),
                    _json_text(run.get("warnings_json")),
                    _json_text(run.get("agent_output_json")),
                    _json_text(run.get("risk_gate_json")),
                ],
            )

            con.execute("DELETE FROM core.positions_snapshot WHERE run_id = ?", [run_id])
            for p in positions:
                con.execute(
                    """
                    INSERT INTO core.positions_snapshot (
                      run_id, ts, symbol, qty, avg_cost, last_price, market_value_eur, unrealized_pnl_eur, weight_pct
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        run_id,
                        ts,
                        p["symbol"],
                        p["qty"],
                        p["avg_cost"],
                        p["last_price"],
                        p["market_value_eur"],
                        p["unrealized_pnl_eur"],
                        p["weight_pct"],
                    ],
                )
                con.execute(
                    """
                    INSERT INTO core.instruments (
                      symbol, name, asset_class, exchange, currency, isin, sector, industry, is_active, updated_at
                    )
                    VALUES (?, NULL, ?, NULL, 'EUR', NULL, NULL, NULL, TRUE, ?)
                    ON CONFLICT (symbol) DO UPDATE SET
                      asset_class = COALESCE(excluded.asset_class, core.instruments.asset_class),
                      updated_at = excluded.updated_at
                    """,
                    [p["symbol"], "FX" if p["symbol"].startswith("FX:") else "EQUITY", ts],
                )

            con.execute(
                """
                INSERT INTO core.portfolio_snapshot (
                  run_id, ts, cash_eur, equity_eur, total_value_eur, cum_fees_eur, cum_ai_cost_eur,
                  trades_this_run, total_pnl_eur, roi, drawdown_pct, meta_json
                )
                VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, 0, 0, ?)
                ON CONFLICT (run_id) DO UPDATE SET
                  ts = excluded.ts,
                  cash_eur = excluded.cash_eur,
                  equity_eur = excluded.equity_eur,
                  total_value_eur = excluded.total_value_eur,
                  meta_json = excluded.meta_json
                """,
                [run_id, ts, cash_eur, equity_eur, total_value, _json_text({"writer": "INLINE_MINIMAL"})],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    return {
        "run_id": run_id,
        "db_path": db_path,
        "rows": {
            "runs": 1,
            "positions_snapshot": len(positions),
            "portfolio_snapshot": 1,
        },
        "snapshots": {
            "run_id": run_id,
            "ts": ts,
            "cash_eur": round(cash_eur, 2),
            "equity_eur": round(equity_eur, 2),
            "total_value_eur": round(total_value, 2),
            "positions_count": len(positions),
            "risk_status": "INLINE_MINIMAL",
        },
    }


def _inline_compute_snapshots(db_path, run_id):
    _inline_init_schema(db_path)
    with duckdb.connect(db_path) as con:
        row = con.execute(
            """
            SELECT
              COALESCE(cash_eur, 0),
              COALESCE(equity_eur, 0),
              COALESCE(total_value_eur, 0),
              ts
            FROM core.portfolio_snapshot
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
        if row is None:
            raise ValueError(f"run_id not found in core.portfolio_snapshot: {run_id}")
        pos_count = int(con.execute("SELECT COUNT(*) FROM core.positions_snapshot WHERE run_id = ?", [run_id]).fetchone()[0] or 0)
        return {
            "run_id": run_id,
            "ts": _parse_ts(row[3], _iso_now()),
            "cash_eur": float(row[0] or 0.0),
            "equity_eur": float(row[1] or 0.0),
            "total_value_eur": float(row[2] or 0.0),
            "positions_count": pos_count,
            "risk_status": "INLINE_MINIMAL",
        }


def _load_writer_module(writer_path_text):
    if writer_path_text == INLINE_WRITER_SENTINEL:
        return {
            "init_schema": _inline_init_schema,
            "upsert_run_bundle": _inline_upsert_run_bundle,
            "compute_snapshots": _inline_compute_snapshots,
        }, "INLINE:embedded"

    writer_path = Path(writer_path_text)
    if not writer_path.is_file():
        raise FileNotFoundError(f"duckdb_writer.py not found at '{writer_path}'.")

    spec = importlib.util.spec_from_file_location("ag1_duckdb_writer", str(writer_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module spec for '{writer_path}'.")

    module = importlib.util.module_from_spec(spec)
    sys.modules["ag1_duckdb_writer"] = module
    spec.loader.exec_module(module)

    required = ("init_schema", "upsert_run_bundle", "compute_snapshots")
    missing = [name for name in required if not callable(getattr(module, name, None))]
    if missing:
        raise RuntimeError(
            "duckdb_writer.py is missing required callables: " + ", ".join(missing)
        )

    return module, str(writer_path)


def _writer_call(writer, method_name, *args):
    fn = None
    if isinstance(writer, dict):
        fn = writer.get(method_name)
    else:
        fn = getattr(writer, method_name, None)
    if not callable(fn):
        raise RuntimeError(f"Writer method missing or not callable: {method_name}")
    return fn(*args)


try:
    items = _items or []
    if not items:
        return []

    first_json = items[0].get("json", {}) if isinstance(items[0], dict) else {}

    requested_writer_path = _clean_path_text(first_json.get("duckdb_writer_path"))
    writer_path = _resolve_writer_path(requested_writer_path or DEFAULT_WRITER_PATH)

    requested_schema_path = _clean_path_text(
        first_json.get("ledger_schema_path") or first_json.get("schema_path")
    )
    schema_path = _resolve_schema_path(requested_schema_path, writer_path)

    writer, writer_path = _load_writer_module(writer_path)

    out = []
    for idx, it in enumerate(items, start=1):
        incoming = it.get("json", {}) if isinstance(it, dict) else {}
        j = dict(incoming or {})

        bundle = j.get("bundle")
        if not isinstance(bundle, dict):
            bundle = j

        db_path = str(j.get("db_path") or DEFAULT_DB_PATH).strip()
        if not db_path:
            raise ValueError("Missing db_path and AG1_DUCKDB_PATH is empty.")

        init_res = _writer_call(writer, "init_schema", db_path)
        upsert_res = _writer_call(writer, "upsert_run_bundle", db_path, bundle)

        run_id = ""
        if isinstance(upsert_res, dict):
            run_id = str(upsert_res.get("run_id") or "").strip()
        if not run_id and isinstance(bundle, dict):
            run_id = str((bundle.get("run") or {}).get("run_id") or "").strip()

        snap_res = _writer_call(writer, "compute_snapshots", db_path, run_id) if run_id else {}

        out.append(
            {
                "json": {
                    "ok": True,
                    "index": idx,
                    "db_path": db_path,
                    "writer_path": writer_path,
                    "schema_path": schema_path or os.getenv(SCHEMA_ENV, ""),
                    "run_id": run_id,
                    "init": init_res,
                    "upsert": upsert_res,
                    "snapshots": snap_res,
                    "bundle_summary": j.get("summary") or {},
                },
                "pairedItem": it.get("pairedItem"),
            }
        )

    return out

except Exception as e:
    return [
        {
            "json": {
                "status": "FATAL_ERROR",
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            }
        }
    ]
