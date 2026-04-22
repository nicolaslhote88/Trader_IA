import json, math, time, gc
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

DB_PATH = "/files/duckdb/ag2_v3.duckdb"


# ---------------------------
# Helpers
# ---------------------------
def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None


def safe_int(v, default=None):
    if v is None:
        return default
    try:
        i = int(v)
        return i
    except Exception:
        try:
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                return default
            return int(f)
        except Exception:
            return default


def safe_bool(v, default=False):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y", "ok"):
            return True
        if s in ("false", "0", "no", "n"):
            return False
    return default


def get_nested(d, path, default=None):
    """
    path: list of keys
    """
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return default
        if k not in cur:
            return default
        cur = cur[k]
    return cur


def try_parse_json(x):
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        # tolerate markdown code fences
        if s.startswith("```"):
            s = s.strip("`").strip()
        # try direct json
        try:
            return json.loads(s)
        except Exception:
            # sometimes: {"output":"{...}"} or leading text
            # attempt to find first { ... } block
            start = s.find("{")
            end = s.rfind("}")
            if start != -1 and end != -1 and end > start:
                try:
                    return json.loads(s[start : end + 1])
                except Exception:
                    return None
    return None


def extract_ai_object(d):
    """
    Try to extract the AI JSON object from various possible n8n/OpenAI node shapes.
    Returns (ai_obj, ai_raw_str, parse_note)
    """
    # Most common candidates (dict or str)
    candidates = [
        d.get("ai_validation"),
        d.get("ai_result"),
        d.get("ai"),
        d.get("openai"),
        d.get("response"),
        d.get("output"),
        d.get("text"),
        get_nested(d, ["data"]),
        get_nested(d, ["result"]),
        get_nested(d, ["choices", 0, "message", "content"]),
    ]

    for c in candidates:
        ai_obj = try_parse_json(c)
        if isinstance(ai_obj, dict) and ("decision" in ai_obj or "validated" in ai_obj or "quality_score" in ai_obj):
            return ai_obj, (c if isinstance(c, str) else json.dumps(c, ensure_ascii=False) if c is not None else ""), "parsed_primary"

    # Sometimes the OpenAI node returns something like { "json": {...} } or nested
    deeper = [
        get_nested(d, ["response", "json"]),
        get_nested(d, ["response", "output"]),
        get_nested(d, ["output", "json"]),
        get_nested(d, ["output", "output"]),
        get_nested(d, ["result", "output"]),
        get_nested(d, ["result", "text"]),
    ]
    for c in deeper:
        ai_obj = try_parse_json(c)
        if isinstance(ai_obj, dict) and ("decision" in ai_obj or "validated" in ai_obj or "quality_score" in ai_obj):
            return ai_obj, (c if isinstance(c, str) else json.dumps(c, ensure_ascii=False) if c is not None else ""), "parsed_nested"

    # Fallback: try to parse entire item as JSON
    ai_obj = try_parse_json(d)
    if isinstance(ai_obj, dict) and ("decision" in ai_obj or "validated" in ai_obj):
        return ai_obj, json.dumps(d, ensure_ascii=False), "parsed_full_item"

    return None, "", "no_ai_json_found"


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


def ensure_schema(con):
    """
    Be defensive: create minimal table if absent + add AI columns if missing.
    This won't override your existing schema; it only adds columns if absent.
    """
    con.execute("""
    CREATE TABLE IF NOT EXISTS technical_signals (
        id TEXT,
        run_id TEXT,
        symbol TEXT,
        updated_at TIMESTAMP
    );
    """)

    # Add AI columns (IF NOT EXISTS is supported by DuckDB)
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_decision TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_validated BOOLEAN;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_quality INTEGER;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_reasoning TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_chart_pattern TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_stop_loss DOUBLE;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_stop_basis TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_bias_sma200 TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_regime_d1 TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_alignment TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_bb_status TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_rsi_status TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_missing TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_anomalies TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS ai_output_ref TEXT;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS pass_pm BOOLEAN;")
    con.execute("ALTER TABLE technical_signals ADD COLUMN IF NOT EXISTS should_vectorize BOOLEAN;")


# ---------------------------
# Main
# ---------------------------
items = _items
out = []

now_iso = datetime.now(timezone.utc).isoformat()

with db_con() as con:
    ensure_schema(con)

    for it in items:
        d = (it.get("json") or {})

        # Core identifiers
        symbol = d.get("symbol") or d.get("Symbol") or d.get("ticker") or ""
        run_id = d.get("run_id") or d.get("Run_ID") or d.get("runId") or ""
        # default id convention used elsewhere in your workflow
        signal_id = d.get("id") or d.get("signal_id") or (f"{run_id}|{symbol}" if run_id and symbol else "")

        # Extract AI object
        ai_obj, ai_raw, parse_note = extract_ai_object(d)

        # Defaults (safe)
        decision = "REJECT"
        validated = False
        quality = 0
        bias = "UNKNOWN"
        regime = "UNKNOWN"
        alignment = "UNKNOWN"
        bb_status = "UNKNOWN"
        rsi_status = "UNKNOWN"
        reasoning = ""
        chart_pattern = "None"
        stop_loss = None
        stop_basis = "NONE"
        missing_fields = []
        anomalies_list = []

        if ai_obj is None:
            anomalies_list.append("AI_OUTPUT_MISSING_OR_UNPARSABLE")
            reasoning = f"[AUTO] No valid AI JSON found ({parse_note})."
        else:
            # Normalize fields from AI schema
            decision = (ai_obj.get("decision") or ai_obj.get("Decision") or "REJECT")
            decision = str(decision).strip().upper()
            if decision not in ("APPROVE", "REJECT", "WATCH"):
                anomalies_list.append("AI_DECISION_INVALID_ENUM")
                decision = "REJECT"

            validated = safe_bool(ai_obj.get("validated", ai_obj.get("Validated", False)), default=False)
            quality = safe_int(ai_obj.get("quality_score", ai_obj.get("quality", ai_obj.get("QualityScore"))), default=0)
            quality = max(0, min(10, quality if quality is not None else 0))

            bias = (
                ai_obj.get("bias_sma200")
                or ai_obj.get("bias")
                or ai_obj.get("Bias_SMA200")
                or get_nested(d, ["ai_context", "d1", "bias_sma200"])
                or "UNKNOWN"
            )
            bias = str(bias).strip().upper()
            if bias not in ("BULLISH", "BEARISH", "UNKNOWN"):
                bias = "UNKNOWN"

            regime = (
                ai_obj.get("regime_d1")
                or ai_obj.get("regime")
                or ai_obj.get("Regime_D1")
                or get_nested(d, ["ai_context", "d1", "regime_d1"])
                or "UNKNOWN"
            )
            regime = str(regime).strip().upper()
            if regime not in ("BULLISH", "BEARISH", "NEUTRAL_RANGE", "TRANSITION", "UNKNOWN"):
                regime = "UNKNOWN"

            alignment = (ai_obj.get("h1_d1_alignment") or ai_obj.get("alignment") or ai_obj.get("H1_D1_Alignment") or "UNKNOWN")
            alignment = str(alignment).strip().upper()
            if alignment not in ("WITH_BIAS", "AGAINST_BIAS", "MIXED", "UNKNOWN"):
                alignment = "UNKNOWN"

            bb_status = (ai_obj.get("bb_status") or ai_obj.get("BB_Status") or "UNKNOWN")
            bb_status = str(bb_status).strip().upper()
            if bb_status not in ("AT_UPPER_BAND", "AT_LOWER_BAND", "MID_RANGE", "SQUEEZE", "UNKNOWN"):
                bb_status = "UNKNOWN"

            rsi_status = (ai_obj.get("rsi_status") or ai_obj.get("RSI_Status") or "UNKNOWN")
            rsi_status = str(rsi_status).strip().upper()
            if rsi_status not in ("OVERBOUGHT", "OVERSOLD", "NEUTRAL", "UNKNOWN"):
                rsi_status = "UNKNOWN"

            reasoning = (ai_obj.get("reasoning") or ai_obj.get("Reasoning") or "").strip()
            chart_pattern = (ai_obj.get("chart_pattern") or ai_obj.get("ChartPattern") or "None").strip()

            stop_loss = safe_float(ai_obj.get("stop_loss_suggestion", ai_obj.get("stop_loss", ai_obj.get("StopLoss"))))
            stop_basis = (ai_obj.get("stop_loss_basis") or ai_obj.get("StopLoss_Basis") or "NONE").strip()

            mf = ai_obj.get("missing_fields", ai_obj.get("MissingFields", []))
            if isinstance(mf, list):
                missing_fields = [str(x) for x in mf]
            elif isinstance(mf, str) and mf.strip():
                missing_fields = [mf.strip()]

            an = ai_obj.get("anomalies", ai_obj.get("Anomalies", []))
            if isinstance(an, list):
                anomalies_list.extend([str(x) for x in an])
            elif isinstance(an, str) and an.strip():
                anomalies_list.append(an.strip())

        # ---------------------------
        # Pull D1 numeric facts from item for consistency checks
        # ---------------------------
        d1_close = safe_float(d.get("d1_last_close") or d.get("D1_Last_Close_Ind") or get_nested(d, ["d1_indicators", "last_close"]))
        d1_sma200 = safe_float(d.get("d1_sma200") or d.get("D1_SMA200") or get_nested(d, ["d1_indicators", "sma200"]))

        # ---------------------------
        # ANTI-CONTRADICTION GUARD (your requested modification)
        # ---------------------------
        if d1_close is not None and d1_sma200 is not None and bias in ("BULLISH", "BEARISH"):
            expected = "BULLISH" if float(d1_close) > float(d1_sma200) else "BEARISH"
            if bias != expected:
                anomalies_list.append("AI_BIAS_CONTRADICTION_D1_CLOSE_VS_SMA200")
                # hard downgrade
                decision = "REJECT"
                validated = False
                quality = min(int(quality or 5), 3)
                reasoning = f"[AUTO] Contradiction biais SMA200 (expected={expected}, got={bias}). " + (reasoning or "")
                # optionally recenter stored bias to expected for downstream sanity
                bias = expected

        # If AI says APPROVE but stop loss missing -> invalid (avoid silent approvals)
        if decision == "APPROVE" and stop_loss is None:
            anomalies_list.append("AI_APPROVE_WITHOUT_STOPLOSS")
            decision = "REJECT"
            validated = False
            quality = min(int(quality or 5), 3)
            reasoning = "[AUTO] APPROVE sans stop-loss => invalide. " + (reasoning or "")

        # Compute pass_pm / should_vectorize (simple, deterministic)
        pass_pm = (decision == "APPROVE") or (decision == "WATCH" and (quality or 0) >= 5)
        should_vectorize = pass_pm  # you can loosen this later if you want WATCH always vectorized

        # Store raw AI output ref (keep short)
        ai_output_ref = d.get("ai_output_ref") or d.get("AI_OutputRef") or ""
        if not ai_output_ref:
            # fallback: short hash-like reference from raw
            if isinstance(ai_raw, str) and ai_raw.strip():
                ai_output_ref = f"ai_raw_len={len(ai_raw)}"

        # Serialize lists
        ai_missing = json.dumps(missing_fields, ensure_ascii=False)
        ai_anomalies = json.dumps(sorted(set([x for x in anomalies_list if x])), ensure_ascii=False)

        # Prepare update payload
        upd = {
            "ai_decision": decision,
            "ai_validated": bool(validated),
            "ai_quality": int(quality or 0),
            "ai_reasoning": reasoning[:5000] if reasoning else "",
            "ai_chart_pattern": chart_pattern[:500] if chart_pattern else "None",
            "ai_stop_loss": stop_loss,
            "ai_stop_basis": stop_basis[:200] if stop_basis else "NONE",
            "ai_bias_sma200": bias,
            "ai_regime_d1": regime,
            "ai_alignment": alignment,
            "ai_bb_status": bb_status,
            "ai_rsi_status": rsi_status,
            "ai_missing": ai_missing,
            "ai_anomalies": ai_anomalies,
            "ai_output_ref": ai_output_ref[:500],
            "pass_pm": bool(pass_pm),
            "should_vectorize": bool(should_vectorize),
        }

        # ---------------------------
        # Write to DuckDB (update preferred)
        # ---------------------------
        if signal_id:
            sets = ", ".join([f"{k} = ?" for k in upd.keys()])
            params = list(upd.values()) + [signal_id]

            try:
                con.execute(
                    f"UPDATE technical_signals SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    params
                )
                # If nothing was updated, optionally insert minimal row
                exists = con.execute("SELECT COUNT(*) FROM technical_signals WHERE id = ?", [signal_id]).fetchone()[0]
                if exists == 0:
                    cols = ["id", "run_id", "symbol"] + list(upd.keys()) + ["updated_at"]
                    placeholders = ", ".join(["?"] * len(cols))
                    con.execute(
                        f"INSERT INTO technical_signals ({', '.join(cols)}) VALUES ({placeholders})",
                        [signal_id, run_id, symbol] + list(upd.values()) + [datetime.now()]
                    )
            except Exception as e:
                # don't hard fail the whole run; attach error to item
                upd["db_write_error"] = str(e)[:500]

        # Push fields back to n8n item for downstream nodes (Sheets / Qdrant)
        dd = dict(d)
        dd.update({
            "ai_decision": upd["ai_decision"],
            "ai_validated": upd["ai_validated"],
            "ai_quality": upd["ai_quality"],
            "ai_reasoning": upd["ai_reasoning"],
            "ai_chart_pattern": upd["ai_chart_pattern"],
            "ai_stop_loss": upd["ai_stop_loss"],
            "ai_stop_basis": upd["ai_stop_basis"],
            "ai_bias_sma200": upd["ai_bias_sma200"],
            "ai_regime_d1": upd["ai_regime_d1"],
            "ai_alignment": upd["ai_alignment"],
            "ai_bb_status": upd["ai_bb_status"],
            "ai_rsi_status": upd["ai_rsi_status"],
            "ai_missing": upd["ai_missing"],
            "ai_anomalies": upd["ai_anomalies"],
            "ai_output_ref": upd["ai_output_ref"],
            "pass_pm": upd["pass_pm"],
            "should_vectorize": upd["should_vectorize"],
            "ai_parse_note": parse_note,
        })

        if "db_write_error" in upd:
            dd["db_write_error"] = upd["db_write_error"]

        out.append({"json": dd})

return out
