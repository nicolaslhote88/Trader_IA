import json
import duckdb, time, gc
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v2.duckdb"

@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
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
            try:
                con.close()
            except Exception:
                pass
        gc.collect()

items = _items
results = []

for it in items:
    d = it.get("json", {}) or {}
    symbol = d.get("symbol", "")
    run_id = d.get("run_id", "")
    signal_id = run_id + "|" + symbol
    sig_hash = d.get("sig_hash", "")

    try:
        ai_raw = d.get("message", {}).get("content", d.get("text", "{}"))
        ai = json.loads(ai_raw) if isinstance(ai_raw, str) else ai_raw

        decision = ai.get("decision", "WATCH")
        validated = ai.get("validated", False)
        quality = ai.get("quality_score", 5)
        reasoning = ai.get("reasoning", "")
        chart_pattern = ai.get("chart_pattern", "None")
        stop_loss = ai.get("stop_loss_suggestion")
        stop_basis = ai.get("stop_loss_basis", "NONE")
        bias = ai.get("bias_sma200", "")
        regime = ai.get("regime_d1", "")
        alignment = ai.get("h1_d1_alignment", "UNKNOWN")
        missing = json.dumps(ai.get("missing_fields", []))
        anomalies = json.dumps(ai.get("anomalies", []))

        # Coherence guards
        if validated and decision != "APPROVE":
            validated = False
        if decision == "APPROVE" and stop_loss is None:
            decision = "WATCH"
            validated = False
        if not reasoning:
            reasoning = "(no reasoning returned)"

        pass_pm = decision == "APPROVE" or (decision == "WATCH" and alignment == "WITH_BIAS" and quality >= 5)
        output_ref = "AG2AI|" + symbol + "|" + sig_hash

        ai_data = {
            "ai_decision": decision, "ai_validated": validated, "ai_quality": quality,
            "ai_reasoning": reasoning, "ai_chart_pattern": chart_pattern,
            "ai_stop_loss": stop_loss, "ai_stop_basis": stop_basis,
            "ai_bias_sma200": bias, "ai_regime_d1": regime, "ai_alignment": alignment,
            "ai_missing": missing, "ai_anomalies": anomalies,
            "ai_output_ref": output_ref, "pass_pm": pass_pm
        }

        with db_con() as con:
            sets = ", ".join(k + " = ?" for k in ai_data.keys())
            con.execute(
                "UPDATE technical_signals SET " + sets + ", updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                list(ai_data.values()) + [signal_id]
            )

            h1_action = d.get("h1_action", "NEUTRAL")
            ttl = 60 if h1_action == "SELL" else 240
            sig_json = json.dumps({"action": h1_action, "score": d.get("h1_score"), "decision": decision, "quality": quality})
            con.execute(
                "INSERT OR REPLACE INTO ai_dedup_cache (symbol, interval_key, sig_hash, sig_json, last_ai_at, last_ai_run_id, last_ai_reason, last_ai_output_ref, ttl_minutes, updated_at) VALUES (?, 'combined', ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                [symbol, sig_hash, sig_json, run_id, d.get("filter_reason", ""), output_ref, ttl]
            )

        out = dict(d)
        out.update(ai_data)
        out["should_vectorize"] = pass_pm or decision in ("APPROVE", "WATCH")
        results.append({"json": out})

    except Exception as e:
        out = dict(d)
        out["ai_decision"] = "ERROR"
        out["ai_reasoning"] = str(e)
        out["should_vectorize"] = False
        results.append({"json": out})

return results
