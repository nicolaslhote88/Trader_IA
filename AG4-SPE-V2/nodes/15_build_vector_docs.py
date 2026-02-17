import duckdb
import gc
import json
import re
import time
from contextlib import contextmanager
from datetime import date, datetime

DEFAULT_DB_PATH = "/files/duckdb/ag4_spe_v2.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
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


def add_line(lines, value):
    s = to_text(value)
    if s:
        lines.append(s)


def build_text(payload):
    lines = []

    add_line(lines, f"[TARGET] {payload.get('symbol')} ({payload.get('company_name') or payload.get('symbol')})")
    add_line(lines, f"[RUN] {payload.get('run_id')} | [DATE] {payload.get('published_at')}")
    add_line(lines, f"[SOURCE] {payload.get('source')} | urgency={payload.get('urgency')} | horizon={payload.get('horizon')}")
    add_line(lines, "")

    add_line(lines, "[HEADLINE]")
    add_line(lines, payload.get("title"))
    add_line(lines, "")

    add_line(lines, "[ANALYSIS]")
    add_line(lines, f"summary={payload.get('summary')}")
    add_line(lines, f"impact_score={payload.get('impact_score')} sentiment={payload.get('sentiment')} confidence={payload.get('confidence_score')}")
    add_line(lines, f"signal={payload.get('suggested_signal')} category={payload.get('category')}")
    add_line(lines, f"relevant={payload.get('is_relevant')} reason={payload.get('relevance_reason')}")
    add_line(lines, f"key_drivers={payload.get('key_drivers')}")
    add_line(lines, f"needs_follow_up={payload.get('needs_follow_up')}")
    add_line(lines, "")

    add_line(lines, "[CONTENT]")
    add_line(lines, payload.get("snippet"))
    add_line(lines, payload.get("text"))
    add_line(lines, "")

    add_line(lines, "[URL]")
    add_line(lines, payload.get("canonical_url") or payload.get("url"))

    return "\n".join(lines).strip()


items = _items or []
run_id = pick_run_id(items)
db_path = pick_db_path(items)

with db_con(db_path) as con:
    if not run_id:
        row = con.execute("SELECT run_id FROM run_log ORDER BY started_at DESC LIMIT 1").fetchone()
        run_id = to_text(row[0]) if row else ""

    if not run_id:
        return []

    rows = con.execute(
        """
        SELECT
          news_id,
          run_id,
          symbol,
          company_name,
          source,
          boursorama_ref,
          url,
          canonical_url,
          title,
          published_at,
          snippet,
          text,
          summary,
          category,
          impact_score,
          sentiment,
          confidence_score,
          horizon,
          urgency,
          suggested_signal,
          key_drivers,
          needs_follow_up,
          is_relevant,
          relevance_reason,
          analyzed_at
        FROM news_history
        WHERE run_id = ?
          AND vector_status = 'PENDING'
          AND action = 'analyze'
          AND COALESCE(is_relevant, TRUE) = TRUE
          AND LENGTH(TRIM(COALESCE(text, summary, snippet, ''))) > 0
        ORDER BY analyzed_at DESC, news_id
        """,
        [run_id],
    ).fetchall()
    cols = [d[0] for d in con.description]

out = []
for tup in rows:
    row = dict(zip(cols, tup))
    news_id = to_text(row.get("news_id"))
    if not news_id:
        continue

    payload = {k: row.get(k) for k in cols}
    payload["schema_version"] = "ag4_spe_news_vector_v1"
    payload["db_path"] = db_path

    point_id = sanitize_id(f"news_{news_id}")
    text = build_text(payload)

    metadata = {
        "id": point_id,
        "news_id": news_id,
        "run_id": to_text(payload.get("run_id")),
        "symbol": to_text(payload.get("symbol")),
        "company_name": to_text(payload.get("company_name")),
        "source": to_text(payload.get("source")),
        "published_at": to_text(payload.get("published_at")),
        "impact_score": payload.get("impact_score"),
        "confidence_score": payload.get("confidence_score"),
        "sentiment": to_text(payload.get("sentiment")),
        "category": to_text(payload.get("category")),
        "suggested_signal": to_text(payload.get("suggested_signal")),
        "urgency": to_text(payload.get("urgency")),
        "horizon": to_text(payload.get("horizon")),
        "is_relevant": bool(payload.get("is_relevant")),
        "db_path": db_path,
        "payload_json": json.dumps(payload, ensure_ascii=False, default=json_default),
    }

    out.append({"json": {"text": text, "metadata": metadata}})

return out

