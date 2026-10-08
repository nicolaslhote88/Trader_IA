import json
import duckdb
import time
import gc
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v3.duckdb"


@contextmanager
def db_con(path=DB_PATH, retries=10, delay=0.2):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as exc:
            if "lock" in str(exc).lower() and attempt < retries - 1:
                time.sleep(min(1.5, delay * (2 ** attempt)))
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
out = []

for it in items:
    d = it.get("json", {}) or {}
    symbol = d.get("symbol", "")
    run_id = d.get("run_id", "")
    signal_id = run_id + "|" + symbol

    ai_data = {
        "ai_decision": "SKIP",
        "ai_validated": False,
        "ai_quality": None,
        "ai_reasoning": "[NOT_ANALYZED] " + str(d.get("filter_reason") or d.get("dedup_reason") or "NO_CACHE"),
        "ai_chart_pattern": "None",
        "ai_stop_loss": None,
        "ai_stop_basis": "NONE",
        "ai_bias_sma200": "",
        "ai_regime_d1": "",
        "ai_alignment": "UNKNOWN",
        "ai_bb_status": "UNKNOWN",
        "ai_rsi_status": "UNKNOWN",
        "ai_missing": "[]",
        "ai_anomalies": "[]",
        "ai_output_ref": "",
        "ai_model": "",
        "pass_pm": False,
    }

    try:
        with db_con() as con:
            row = con.execute(
                "SELECT sig_json, last_ai_run_id, last_ai_output_ref, date_diff('minute', CAST(last_ai_at AS TIMESTAMP), CAST(now() AS TIMESTAMP)), ttl_minutes, sig_hash FROM ai_dedup_cache WHERE symbol=? AND interval_key='combined'",
                [symbol],
            ).fetchone()

            if row:
                sig_json, last_ai_run_id, last_ai_output_ref, cache_age_min, ttl_minutes, cached_hash = row
                try:
                    sj = json.loads(sig_json or "{}")
                except Exception:
                    sj = {}

                decision = str(sj.get("decision", "WATCH") or "WATCH").strip().upper()
                quality = int(sj["quality"]) if sj.get("quality") is not None else 0
                try:
                    age_min = float(cache_age_min) if cache_age_min is not None else None
                except Exception:
                    age_min = None
                try:
                    ttl = float(ttl_minutes) if ttl_minutes else 0.0
                except Exception:
                    ttl = 0.0
                cap = 10080.0 if ttl <= 0 else (ttl if ttl < 10080.0 else 10080.0)
                fresh = age_min is not None and 0 <= age_min <= cap
                if fresh and decision != "REJECT" and (not d.get("is_held") or cached_hash == d.get("sig_hash")):
                    for field in ("ai_validated", "ai_reasoning", "ai_chart_pattern", "ai_stop_loss", "ai_stop_basis", "ai_bias_sma200", "ai_regime_d1", "ai_alignment", "ai_missing", "ai_anomalies", "ai_rr_theoretical"):
                        if field in sj:
                            ai_data[field] = sj[field]
                    ai_data["ai_decision"] = decision
                    ai_data["ai_quality"] = quality
                    ai_data["ai_reasoning"] = f"[CACHE] Reused AI decision from {last_ai_run_id} (age {age_min:.0f}min). " + str(sj.get("ai_reasoning") or "")
                    ai_data["ai_output_ref"] = last_ai_output_ref or ""
                    ai_data["ai_model"] = "gpt-5-mini"
                    ai_data["ai_bb_status"] = str(sj.get("bb_status", "UNKNOWN") or "UNKNOWN").strip().upper()
                    ai_data["ai_rsi_status"] = str(sj.get("rsi_status", "UNKNOWN") or "UNKNOWN").strip().upper()
                    ai_data["pass_pm"] = decision == "APPROVE" or (decision == "WATCH" and quality >= 5)
                else:
                    ai_data["ai_reasoning"] = "[CACHE] Not reused (stale or REJECT not carried); reset to SKIP."

            sets = ", ".join(k + " = ?" for k in ai_data.keys())
            con.execute(
                "UPDATE technical_signals SET " + sets + ", updated_at=CURRENT_TIMESTAMP WHERE id=?",
                list(ai_data.values()) + [signal_id],
            )
    except Exception as exc:
        # Une contention de cache ne doit pas faire échouer tout le run.
        ai_data["db_write_error"] = str(exc)[:500]
        ai_data["ai_anomalies"] = json.dumps(["CACHE_DB_UNAVAILABLE"])
        ai_data["ai_reasoning"] = "[CACHE] DuckDB unavailable after retries; safe SKIP."

    dd = dict(d)
    dd.update(ai_data)
    out.append({"json": dd})

return out
