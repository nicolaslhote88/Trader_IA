import json
import duckdb, time, gc
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v3.duckdb"

@contextmanager
def db_con(path=DB_PATH):
    con = duckdb.connect(path)
    try:
        yield con
    finally:
        try:
            con.close()
        except Exception:
            pass
        gc.collect()

items = _items
out = []

for it in items:
    d = it.get("json", {}) or {}
    symbol = d.get("symbol", "")
    run_id = d.get("run_id", "")
    signal_id = run_id + "|" + symbol

    ai_data = {
        "ai_decision": "SKIP",
        "ai_validated": False,
        "ai_quality": 0,
        "ai_reasoning": "[CACHE] No AI call (TTL/filtered) and no cache record.",
        "ai_chart_pattern": "None",
        "ai_stop_loss": None,
        "ai_stop_basis": "NONE",
        "ai_bias_sma200": "",
        "ai_regime_d1": "",
        "ai_alignment": "UNKNOWN",
        "ai_missing": "[]",
        "ai_anomalies": "[]",
        "ai_output_ref": "",
        "pass_pm": False,
    }

    with db_con() as con:
        row = con.execute(
            "SELECT sig_json, last_ai_run_id, last_ai_output_ref FROM ai_dedup_cache WHERE symbol=? AND interval_key='combined'",
            [symbol]
        ).fetchone()

        if row:
            sig_json, last_ai_run_id, last_ai_output_ref = row
            try:
                sj = json.loads(sig_json or "{}")
            except Exception:
                sj = {}

            decision = sj.get("decision", "WATCH")
            quality = int(sj.get("quality", 5) or 5)

            ai_data["ai_decision"] = decision
            ai_data["ai_quality"] = quality
            ai_data["ai_reasoning"] = f"[CACHE] Reused AI decision from {last_ai_run_id}."
            ai_data["ai_output_ref"] = last_ai_output_ref or ""

            # recompute pass_pm same rule as Extract
            ai_data["pass_pm"] = (decision == "APPROVE") or (decision == "WATCH" and quality >= 5)

        # write back to technical_signals for consistency
        sets = ", ".join(k + " = ?" for k in ai_data.keys())
        con.execute(
            "UPDATE technical_signals SET " + sets + ", updated_at=CURRENT_TIMESTAMP WHERE id=?",
            list(ai_data.values()) + [signal_id]
        )

    dd = dict(d)
    dd.update(ai_data)
    dd["should_vectorize"] = dd["pass_pm"] or dd["ai_decision"] in ("APPROVE", "WATCH")
    out.append({"json": dd})

return out
