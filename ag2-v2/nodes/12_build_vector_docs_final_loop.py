import duckdb
import gc
import json
import os
import re
import time
from contextlib import contextmanager
from datetime import date, datetime

DB_PATH = "/files/duckdb/ag2_v2.duckdb"
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333").rstrip("/")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")


@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as exc:
            if "lock" in str(exc).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def sanitize_id(value):
    return re.sub(r"[^a-zA-Z0-9_-]", "_", str(value or ""))


def pick_run_id(items):
    for it in items:
        d = it.get("json", {}) or {}
        run_id = str(d.get("run_id", "") or "").strip()
        if run_id:
            return run_id
    return ""


def prefixed(row, prefix):
    out = {}
    p = prefix + "_"
    for k, v in row.items():
        if k.startswith(p):
            out[k[len(p):]] = v
    return out


def json_default(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


def fmt(v, nd=2):
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            return str(v).lower()
        if isinstance(v, int):
            return str(v)
        if isinstance(v, float):
            return f"{v:.{nd}f}"
        return str(v)
    except Exception:
        return str(v)


def add_line(lines, value):
    if value is None:
        return
    s = str(value)
    if not s.strip():
        return
    lines.append(s)


DROP_KEYS = {
    "h1_bars_60",
    "bars_h1",
    "h1_response",
    "d1_response",
    "ai_raw",
}


def prune_row(row):
    out = {}
    for k, v in row.items():
        if k in DROP_KEYS:
            continue
        if isinstance(v, (list, dict)) and k.endswith("_response"):
            continue
        out[k] = v
    return out


def render_vector_text(payload):
    lines = []

    sym = payload.get("symbol")
    sym_name = payload.get("symbol_name")
    run_id = payload.get("run_id")
    workflow_date = payload.get("workflow_date")

    add_line(lines, f"[ENTITY] {sym}" + (f" ({sym_name})" if sym_name else ""))
    add_line(lines, "[DOC_KIND] TECH")
    add_line(lines, f"[RUN] {run_id} | [ASOF] {workflow_date}")
    add_line(lines, "")

    h1 = payload.get("h1", {}) or {}
    h1m = h1.get("meta") or {}
    h1s = h1.get("signal") or {}
    h1i = h1.get("indicators") or {}
    add_line(lines, "[H1]")
    add_line(lines, f"date={h1m.get('date')} source={h1m.get('source')} status={h1m.get('status')}")
    add_line(lines, f"action={h1s.get('action')} score={h1s.get('score')} conf={h1s.get('confidence')} rationale={h1s.get('rationale')}")
    key_h1 = {
        "last_close": h1i.get("last_close"),
        "sma50": h1i.get("sma50"),
        "sma200": h1i.get("sma200"),
        "rsi14": h1i.get("rsi14"),
        "macd_hist": h1i.get("macd_hist"),
        "adx": h1i.get("adx"),
        "atr_pct": h1i.get("atr_pct"),
        "support": h1i.get("support"),
        "resistance": h1i.get("resistance"),
    }
    add_line(lines, "indicators=" + ", ".join([f"{k}:{fmt(v)}" for k, v in key_h1.items() if v is not None]))
    add_line(lines, "")

    d1 = payload.get("d1", {}) or {}
    d1m = d1.get("meta") or {}
    d1s = d1.get("signal") or {}
    d1i = d1.get("indicators") or {}
    add_line(lines, "[D1]")
    add_line(lines, f"date={d1m.get('date')} source={d1m.get('source')} status={d1m.get('status')}")
    add_line(lines, f"action={d1s.get('action')} score={d1s.get('score')} conf={d1s.get('confidence')} rationale={d1s.get('rationale')}")
    key_d1 = {
        "last_close": d1i.get("last_close"),
        "sma50": d1i.get("sma50"),
        "sma200": d1i.get("sma200"),
        "rsi14": d1i.get("rsi14"),
        "macd_hist": d1i.get("macd_hist"),
        "adx": d1i.get("adx"),
        "atr_pct": d1i.get("atr_pct"),
        "support": d1i.get("support"),
        "resistance": d1i.get("resistance"),
        "stoch_k": d1i.get("stoch_k"),
    }
    add_line(lines, "indicators=" + ", ".join([f"{k}:{fmt(v)}" for k, v in key_d1.items() if v is not None]))
    add_line(lines, "")

    ai = payload.get("ai")
    if ai:
        add_line(lines, "[AI]")
        add_line(lines, f"decision={ai.get('decision')} validated={ai.get('validated')} quality={ai.get('quality')} rr={ai.get('rr_theoretical')}")
        add_line(lines, f"stop_loss={ai.get('stop_loss')} stop_basis={ai.get('stop_basis')} bias_sma200={ai.get('bias_sma200')} regime_d1={ai.get('regime_d1')} alignment={ai.get('alignment')}")
        reasoning = ai.get("reasoning")
        if reasoning:
            reasoning = str(reasoning)
            if len(reasoning) > 800:
                reasoning = reasoning[:800] + "..."
            add_line(lines, f"reasoning={reasoning}")
        add_line(lines, "")

    pr = payload.get("processing", {}) or {}
    add_line(lines, "[PROCESSING]")
    add_line(
        lines,
        " ".join(
            [
                f"filter_reason={pr.get('filter_reason')}",
                f"pass_ai={pr.get('pass_ai')}",
                f"pass_pm={pr.get('pass_pm')}",
                f"call_ai={pr.get('call_ai')}",
                f"dedup_reason={pr.get('dedup_reason')}",
                f"sig_hash={pr.get('sig_hash')}",
                f"vector_status={pr.get('vector_status')}",
            ]
        ).strip(),
    )

    return "\n".join(lines).strip()


items = _items or []
run_id = pick_run_id(items)

with db_con() as con:
    if not run_id:
        r = con.execute("SELECT run_id FROM run_log ORDER BY started_at DESC LIMIT 1").fetchone()
        run_id = str(r[0]) if r else ""

    if not run_id:
        return []

    rows = con.execute(
        """
        SELECT
            ts.*,
            COALESCE(u.name, '') AS symbol_name
        FROM technical_signals ts
        LEFT JOIN universe u ON u.symbol = ts.symbol
        WHERE ts.run_id = ?
          AND (ts.vector_status IS NULL OR ts.vector_status IN ('PENDING','FAILED','SKIPPED'))
        ORDER BY ts.symbol
        """,
        [run_id],
    ).fetchall()
    cols = [d[0] for d in con.description]

out = []
for tup in rows:
    row = dict(zip(cols, tup))

    signal_id = str(row.get("id", "") or f"{row.get('run_id','')}|{row.get('symbol','')}")
    symbol = str(row.get("symbol", "") or "")
    symbol_name = str(row.get("symbol_name", "") or "")

    ai_decision = str(row.get("ai_decision", "") or "").strip()
    has_ai = ai_decision not in ("", "SKIP")
    doc_id = sanitize_id(signal_id)

    payload = {
        "schema_version": "VectorDoc_v2",
        "signal_id": signal_id,
        "run_id": row.get("run_id"),
        "workflow_date": str(row.get("workflow_date", "") or ""),
        "symbol": symbol,
        "symbol_name": symbol_name,
        "h1": {
            "meta": {
                "date": row.get("h1_date"),
                "source": row.get("h1_source"),
                "status": row.get("h1_status"),
                "warnings": row.get("h1_warnings"),
            },
            "signal": {
                "action": row.get("h1_action"),
                "score": row.get("h1_score"),
                "confidence": row.get("h1_confidence"),
                "rationale": row.get("h1_rationale"),
            },
            "indicators": prefixed(row, "h1"),
        },
        "d1": {
            "meta": {
                "date": row.get("d1_date"),
                "source": row.get("d1_source"),
                "status": row.get("d1_status"),
                "warnings": row.get("d1_warnings"),
            },
            "signal": {
                "action": row.get("d1_action"),
                "score": row.get("d1_score"),
                "confidence": row.get("d1_confidence"),
                "rationale": row.get("d1_rationale"),
            },
            "indicators": prefixed(row, "d1"),
        },
        "processing": {
            "filter_reason": row.get("filter_reason"),
            "pass_ai": row.get("pass_ai"),
            "pass_pm": row.get("pass_pm"),
            "call_ai": row.get("call_ai"),
            "dedup_reason": row.get("dedup_reason"),
            "sig_hash": row.get("sig_hash"),
            "vector_status": row.get("vector_status"),
        },
        "ai": None
        if not has_ai
        else {
            "decision": row.get("ai_decision"),
            "validated": row.get("ai_validated"),
            "quality": row.get("ai_quality"),
            "reasoning": row.get("ai_reasoning"),
            "chart_pattern": row.get("ai_chart_pattern"),
            "stop_loss": row.get("ai_stop_loss"),
            "stop_basis": row.get("ai_stop_basis"),
            "bias_sma200": row.get("ai_bias_sma200"),
            "regime_d1": row.get("ai_regime_d1"),
            "alignment": row.get("ai_alignment"),
            "missing": row.get("ai_missing"),
            "anomalies": row.get("ai_anomalies"),
            "output_ref": row.get("ai_output_ref"),
            "rr_theoretical": row.get("ai_rr_theoretical"),
        },
        "technical_signals_row": prune_row(row),
        "raw_ref": {"db": "ag2_v2.duckdb", "table": "technical_signals", "id": signal_id},
    }

    metadata = {
        "id": doc_id,
        "doc_id": doc_id,
        "doc_kind": "TECH",
        "asof_date": str(row.get("workflow_date", "") or ""),
        "schema_version": "VectorDoc_v2",
        "signal_id": signal_id,
        "run_id": str(row.get("run_id", "") or ""),
        "symbol": symbol,
        "symbol_name": symbol_name,
        "workflow_date": str(row.get("workflow_date", "") or ""),
        "h1_date": str(row.get("h1_date", "") or ""),
        "d1_date": str(row.get("d1_date", "") or ""),
        "h1_action": str(row.get("h1_action", "") or ""),
        "d1_action": str(row.get("d1_action", "") or ""),
        "ai_decision": ai_decision,
        "ai_quality": row.get("ai_quality"),
        "has_ai": has_ai,
        "sig_hash": str(row.get("sig_hash", "") or ""),
        "qdrant_url": QDRANT_URL,
        "qdrant_api_key": QDRANT_API_KEY,
        "payload_json": json.dumps(payload, ensure_ascii=False, default=json_default),
    }

    out.append({"json": {"text": render_vector_text(payload), "metadata": metadata}})

return out
