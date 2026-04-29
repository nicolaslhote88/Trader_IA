import duckdb, json, time, gc, re, os
from contextlib import contextmanager
from datetime import date, datetime

DEFAULT_DB_PATH = "/files/duckdb/ag3_v2.duckdb"
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333").rstrip("/")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def to_text(v):
    if v is None:
        return ""
    return str(v).strip()


def sanitize_id(value):
    value = to_text(value)
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value)


def pick_run_id(items):
    for it in items:
        d = it.get("json", {}) or {}
        rid = to_text(d.get("run_id"))
        if rid:
            return rid
    return ""


def pick_db_path(items):
    for it in items:
        d = it.get("json", {}) or {}
        p = to_text(d.get("db_path"))
        if p:
            return p
    return DEFAULT_DB_PATH


def json_default(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


def fmt_num(v, digits=2):
    if v is None:
        return "n/a"
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return to_text(v) or "n/a"


def add_line(lines, value):
    s = to_text(value)
    if s:
        lines.append(s)


def build_metrics_by_symbol(con, run_id):
    rows = con.execute(
        """
        SELECT symbol, section, metric, value_num, value_text, unit
        FROM fundamental_metrics_history
        WHERE run_id = ?
        ORDER BY symbol, section, metric
        """,
        [run_id],
    ).fetchall()

    by_symbol = {}
    for symbol, section, metric, value_num, value_text, unit in rows:
        sym = to_text(symbol).upper()
        if not sym:
            continue
        sec = to_text(section) or "other"
        met = to_text(metric) or "metric"
        value = value_num if value_num is not None else value_text
        unit_txt = to_text(unit)
        by_symbol.setdefault(sym, {}).setdefault(sec, []).append(
            {"metric": met, "value": value, "unit": unit_txt}
        )
    return by_symbol


def render_vector_text(payload):
    lines = []

    add_line(lines, f"[ENTITY] {payload.get('symbol')} ({payload.get('name') or payload.get('symbol')})")
    add_line(lines, "[DOC_KIND] FUNDA")
    add_line(lines, f"[RUN] {payload.get('run_id')} | [ASOF] {payload.get('as_of_date')}")
    add_line(lines, f"[STATUS] {payload.get('status')} | horizon={payload.get('horizon')}")
    add_line(lines, "")

    add_line(lines, "[SCORES]")
    add_line(
        lines,
        "triage={}/100 risk={}/100 quality={}/100 growth={}/100 valuation={}/100 health={}/100 consensus={}/100".format(
            payload.get("score"),
            payload.get("risk_score"),
            payload.get("quality_score"),
            payload.get("growth_score"),
            payload.get("valuation_score"),
            payload.get("health_score"),
            payload.get("consensus_score"),
        ),
    )
    add_line(lines, "")

    add_line(lines, "[THESIS]")
    add_line(lines, f"why={payload.get('why')}")
    add_line(lines, f"risks={payload.get('risks')}")
    add_line(lines, f"next_steps={payload.get('next_steps')}")
    add_line(lines, "")

    add_line(lines, "[VALUATION]")
    add_line(
        lines,
        "current_price={} target_price={} upside_pct={} analyst_count={} recommendation={}".format(
            fmt_num(payload.get("current_price")),
            fmt_num(payload.get("target_price")),
            fmt_num(payload.get("upside_pct")),
            payload.get("analyst_count"),
            payload.get("recommendation"),
        ),
    )
    add_line(lines, f"scenario={payload.get('valuation')}")
    add_line(lines, "")

    add_line(lines, "[CONSENSUS]")
    add_line(
        lines,
        "recommendation_mean={} target_low={} target_high={} dispersion_pct={}".format(
            fmt_num(payload.get("recommendation_mean")),
            fmt_num(payload.get("target_low_price")),
            fmt_num(payload.get("target_high_price")),
            fmt_num(payload.get("dispersion_pct")),
        ),
    )
    add_line(lines, "")

    metrics = payload.get("metrics_by_section") or {}
    if metrics:
        add_line(lines, "[METRICS]")
        for section in sorted(metrics.keys()):
            entries = metrics.get(section) or []
            if not entries:
                continue
            rendered = []
            for entry in entries[:12]:
                metric = to_text(entry.get("metric"))
                value = entry.get("value")
                unit = to_text(entry.get("unit"))
                if value is None:
                    continue
                vv = fmt_num(value)
                rendered.append(f"{metric}:{vv}{unit}")
            if rendered:
                add_line(lines, f"{section} => " + ", ".join(rendered))

    return "\n".join(lines).strip()


items = _items or []
run_id = pick_run_id(items)
db_path = pick_db_path(items)

with db_con(db_path) as con:
    if not run_id:
        row = con.execute(
            "SELECT run_id FROM run_log ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        run_id = to_text(row[0]) if row else ""

    if not run_id:
        return []

    rows = con.execute(
        """
        SELECT
          t.record_id,
          t.run_id,
          t.symbol,
          t.name,
          t.sector,
          t.industry,
          t.country,
          t.status,
          t.error,
          t.as_of_date,
          t.horizon,
          t.score,
          t.risk_score,
          t.quality_score,
          t.growth_score,
          t.valuation_score,
          t.health_score,
          t.consensus_score,
          t.current_price,
          t.target_price,
          t.upside_pct,
          t.analyst_count,
          t.recommendation,
          t.valuation,
          t.why,
          t.risks,
          t.next_steps,
          t.data_coverage_pct,
          t.strategy_version,
          t.config_version,
          c.recommendation_mean,
          c.target_low_price,
          c.target_high_price,
          c.dispersion_pct
        FROM fundamentals_triage_history t
        LEFT JOIN analyst_consensus_history c
          ON c.run_id = t.run_id AND c.symbol = t.symbol
        WHERE t.run_id = ?
          AND (t.vector_status IS NULL OR t.vector_status IN ('PENDING','FAILED'))
        ORDER BY t.symbol
        """,
        [run_id],
    ).fetchall()
    cols = [d[0] for d in con.description]
    metrics_by_symbol = build_metrics_by_symbol(con, run_id)

out = []
for tup in rows:
    row = dict(zip(cols, tup))
    symbol = to_text(row.get("symbol")).upper()
    if not symbol:
        continue

    payload = {
        "schema_version": "VectorDoc_v2",
        "record_id": to_text(row.get("record_id")),
        "run_id": to_text(row.get("run_id")),
        "symbol": symbol,
        "name": to_text(row.get("name")),
        "sector": to_text(row.get("sector")),
        "industry": to_text(row.get("industry")),
        "country": to_text(row.get("country")),
        "status": to_text(row.get("status")),
        "error": to_text(row.get("error")),
        "as_of_date": to_text(row.get("as_of_date")),
        "horizon": to_text(row.get("horizon")),
        "score": row.get("score"),
        "risk_score": row.get("risk_score"),
        "quality_score": row.get("quality_score"),
        "growth_score": row.get("growth_score"),
        "valuation_score": row.get("valuation_score"),
        "health_score": row.get("health_score"),
        "consensus_score": row.get("consensus_score"),
        "current_price": row.get("current_price"),
        "target_price": row.get("target_price"),
        "upside_pct": row.get("upside_pct"),
        "analyst_count": row.get("analyst_count"),
        "recommendation": to_text(row.get("recommendation")),
        "valuation": to_text(row.get("valuation")),
        "why": to_text(row.get("why")),
        "risks": to_text(row.get("risks")),
        "next_steps": to_text(row.get("next_steps")),
        "data_coverage_pct": row.get("data_coverage_pct"),
        "strategy_version": to_text(row.get("strategy_version")),
        "config_version": to_text(row.get("config_version")),
        "recommendation_mean": row.get("recommendation_mean"),
        "target_low_price": row.get("target_low_price"),
        "target_high_price": row.get("target_high_price"),
        "dispersion_pct": row.get("dispersion_pct"),
        "metrics_by_section": metrics_by_symbol.get(symbol, {}),
    }

    vector_text = render_vector_text(payload)
    point_id = sanitize_id(payload.get("record_id") or f"{payload.get('run_id')}|{symbol}")
    metadata = {
        "id": point_id,
        "doc_id": point_id,
        "doc_kind": "FUNDA",
        "schema_version": "VectorDoc_v2",
        "record_id": payload.get("record_id"),
        "run_id": payload.get("run_id"),
        "symbol": symbol,
        "name": payload.get("name"),
        "sector": payload.get("sector"),
        "status": payload.get("status"),
        "horizon": payload.get("horizon"),
        "score": payload.get("score"),
        "risk_score": payload.get("risk_score"),
        "valuation_score": payload.get("valuation_score"),
        "quality_score": payload.get("quality_score"),
        "health_score": payload.get("health_score"),
        "consensus_score": payload.get("consensus_score"),
        "upside_pct": payload.get("upside_pct"),
        "recommendation": payload.get("recommendation"),
        "analyst_count": payload.get("analyst_count"),
        "data_coverage_pct": payload.get("data_coverage_pct"),
        "as_of_date": payload.get("as_of_date"),
        "strategy_version": payload.get("strategy_version"),
        "config_version": payload.get("config_version"),
        "qdrant_url": QDRANT_URL,
        "qdrant_api_key": QDRANT_API_KEY,
        "db_path": db_path,
        "payload_json": json.dumps(payload, ensure_ascii=False, default=json_default),
    }

    out.append({"json": {"text": vector_text, "metadata": metadata}})

return out
