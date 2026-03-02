"""
Node 11 — Sync DuckDB → Google Sheets format.
Reads signals from the LATEST RUN only and outputs items matching
the expected legacy column format.
Runs AFTER Finalize (node 10) — delta sync (batch only).
"""
import duckdb, gc, json, time
from contextlib import contextmanager

DB_PATH = "/files/duckdb/ag2_v3.duckdb"

@contextmanager
def db_con(path=DB_PATH, retries=5, base_delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except duckdb.IOException:
            if attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass
        gc.collect()

# ── Column mapping: DuckDB snake_case → Sheets PascalCase ──
# Existing columns (85 from the old AG2 sheet)
FIELD_MAP = {
    # Identity
    "symbol": "Symbol",
    "run_id": "Run_ID",
    "workflow_date": "Workflow_Date",
    # H1 signal
    "h1_action": "H1_Action",
    "h1_score": "H1_Score",
    "h1_confidence": "H1_Confidence",
    "h1_rationale": "H1_Rationale",
    "h1_date": "H1_Date",
    "h1_source": "H1_Source",
    "h1_status": "H1_Status",
    "h1_warnings": "H1_Warnings",
    # D1 signal
    "d1_action": "D1_Action",
    "d1_score": "D1_Score",
    "d1_confidence": "D1_Confidence",
    "d1_rationale": "D1_Rationale",
    "d1_date": "D1_Date",
    "d1_source": "D1_Source",
    "d1_status": "D1_Status",
    "d1_warnings": "D1_Warnings",
    # Price
    "last_close": "Last_Close",
    # Signal processing
    "filter_reason": "Reason",
    "pass_ai": "Pass_AI",
    "pass_pm": "Pass_PM",
    "sig_hash": "Sig_JSON",
    "vector_status": "vector_status",
    "vectorized_at": "vectorizedAt",
    # H1 indicators
    "h1_sma20": "H1_SMA20",
    "h1_sma50": "H1_SMA50",
    "h1_sma200": "H1_SMA200",
    "h1_ema12": "H1_EMA12",
    "h1_ema26": "H1_EMA26",
    "h1_macd": "H1_MACD",
    "h1_macd_signal": "H1_MACD_Signal",
    "h1_macd_hist": "H1_MACD_Hist",
    "h1_rsi14": "H1_RSI14",
    "h1_volatility": "H1_Volatility_Ann",
    "h1_last_close": "H1_Last_Close_Ind",
    "h1_atr": "H1_ATR_Value",
    "h1_atr_pct": "H1_ATR_Pct",
    "h1_resistance": "H1_Resistance_50",
    "h1_dist_res_pct": "H1_Dist_To_Res_Pct_50",
    # D1 indicators
    "d1_sma20": "D1_SMA20",
    "d1_sma50": "D1_SMA50",
    "d1_sma200": "D1_SMA200",
    "d1_ema12": "D1_EMA12",
    "d1_ema26": "D1_EMA26",
    "d1_macd": "D1_MACD",
    "d1_macd_signal": "D1_MACD_Signal",
    "d1_macd_hist": "D1_MACD_Hist",
    "d1_rsi14": "D1_RSI14",
    "d1_volatility": "D1_Volatility_Ann",
    "d1_last_close": "D1_Last_Close_Ind",
    "d1_atr": "D1_ATR_Value",
    "d1_atr_pct": "D1_ATR_Pct",
    "d1_resistance": "D1_Resistance_50",
    "d1_dist_res_pct": "D1_Dist_To_Res_Pct_50",
    # AI fields
    "ai_decision": "AI_Decision",
    "ai_validated": "AI_Validated",
    "ai_quality": "AI_QualityScore",
    "ai_reasoning": "AI_Reasoning",
    "ai_bias_sma200": "AI_Bias_SMA200",
    "ai_regime_d1": "AI_Regime_D1",
    "ai_alignment": "AI_Alignment",
    "ai_bb_status": "AI_BB_Status",
    "ai_rsi_status": "AI_RSI_Status",
    "ai_stop_loss": "AI_StopLoss",
    "ai_stop_basis": "AI_StopLoss_Basis",
    "ai_missing": "AI_MissingFields",
    "ai_anomalies": "AI_Anomalies",
    "ai_output_ref": "AI_Output",
    "ai_chart_pattern": "AI_ChartPattern",
    "ai_rr_theoretical": "AI_RR_Theoretical",
}

# "Combined" columns: same as H1 values (backward compat with old AG2)
COMBINED_MAP = {
    "h1_sma20": "SMA20",
    "h1_sma50": "SMA50",
    "h1_sma200": "SMA_200",
    "h1_ema12": "EMA12",
    "h1_ema26": "EMA26",
    "h1_macd": "MACD",
    "h1_macd_signal": "MACD_Signal",
    "h1_macd_hist": "MACD_Hist",
    "h1_rsi14": "RSI14",
    "h1_volatility": "Volatility_Ann",
    "h1_last_close": "Last_Close_Ind",
    "h1_atr": "ATR_Value",
    "h1_atr_pct": "ATR_Pct",
    "h1_resistance": "Resistance_50",
    "h1_dist_res_pct": "Dist_To_Res_Pct_50",
}

# V2-only columns (new indicators not in old sheet — will create headers)
V2_EXTRA_MAP = {
    "h1_bb_upper": "H1_BB_Upper",
    "h1_bb_lower": "H1_BB_Lower",
    "h1_bb_width": "H1_BB_Width",
    "h1_stoch_k": "H1_Stoch_K",
    "h1_stoch_d": "H1_Stoch_D",
    "h1_adx": "H1_ADX",
    "h1_obv_slope": "H1_OBV_Slope",
    "h1_support": "H1_Support",
    "h1_dist_sup_pct": "H1_Dist_To_Sup_Pct",
    "d1_bb_upper": "D1_BB_Upper",
    "d1_bb_lower": "D1_BB_Lower",
    "d1_bb_width": "D1_BB_Width",
    "d1_stoch_k": "D1_Stoch_K",
    "d1_stoch_d": "D1_Stoch_D",
    "d1_adx": "D1_ADX",
    "d1_obv_slope": "D1_OBV_Slope",
    "d1_support": "D1_Support",
    "d1_dist_sup_pct": "D1_Dist_To_Sup_Pct",
}

def _fmt(val):
    """Format a value for Sheets: convert None/NaN to empty string, bools to strings."""
    if val is None:
        return ""
    s = str(val)
    if s.lower() in ("nan", "nat", "none"):
        return ""
    if isinstance(val, bool):
        return str(val).upper()
    return val

# ── Main ──
items = []

with db_con() as con:
    # Delta sync: only symbols from the latest run (not the full universe)
    rows = con.execute("""
        SELECT ts.*
        FROM technical_signals ts
        WHERE ts.run_id = (
            SELECT run_id FROM run_log ORDER BY started_at DESC LIMIT 1
        )
        ORDER BY ts.symbol
    """).fetchall()
    col_names = [desc[0] for desc in con.description]

for row_tuple in rows:
    row = dict(zip(col_names, row_tuple))
    item = {}

    # Map standard fields
    for db_col, sheet_col in FIELD_MAP.items():
        item[sheet_col] = _fmt(row.get(db_col))

    # Map combined fields (H1 values as default "combined" for backward compat)
    for db_col, sheet_col in COMBINED_MAP.items():
        item[sheet_col] = _fmt(row.get(db_col))

    # Map V2 extra fields
    for db_col, sheet_col in V2_EXTRA_MAP.items():
        item[sheet_col] = _fmt(row.get(db_col))

    # Static/computed fields
    item["TTL_Minutes"] = 180
    item["row_hash"] = _fmt(row.get("sig_hash", ""))

    items.append({"json": item})

# Output for n8n
return items if items else [{"json": {"_sync": "no_data"}}]
