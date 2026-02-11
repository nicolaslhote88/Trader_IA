#!/usr/bin/env python3
"""
AG2-V2 Workflow Builder — Generates importable n8n workflow JSON.
Run: cd ag2-v2 && python3 build_workflow.py > AG2-V2-workflow.json

Architecture: Cron/Manual → Read Universe → Init Config → DuckDB Init → Loop Symbols
  Loop (each) → HTTP H1 + HTTP D1 (parallel) → Wrap H1 + Wrap D1 → Merge → Compute
  → IF Call AI? → Snapshot → AI GPT → Extract AI → IF Vectorize?
  → Prep Vector → Qdrant Upsert → Mark Vectorized → Loop
  Loop (done) → Finalize Run → Sync Sheets Format → Write Sheets
"""
import json, uuid, os

DIR = os.path.dirname(os.path.abspath(__file__))
NODES_DIR = os.path.join(DIR, "nodes")

def load(filename):
    with open(os.path.join(NODES_DIR, filename), "r") as f:
        return f.read()

# Credentials (from V1 export)
GS_CRED = "aX5iAQEN9HK4UGjr"
OAI_CRED = "rILpYjTayqc4jXXZ"
QD_CRED = "q1CRmg2N6AmW6pC1"
SHEET_ID = "1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I"
UNI_GID = 1078848687

nid = lambda: str(uuid.uuid4())
IDS = {k: nid() for k in [
    "cron","manual","read_universe","init_config","duckdb_init","loop",
    "http_h1","http_d1","wrap_h1","wrap_d1","merge",
    "compute","if_ai","snapshot","ai_validate","extract_ai",
    "if_vector","prep_vector","embed_openai","data_loader","text_splitter",
    "qdrant","mark_vector","finalize","sync_sheets","write_sheets"]}

AI_SYSTEM = (
    "Tu es l'Agent n\u00b02 : Validateur Technique H1 \u2194 D1. Senior Technical Analyst et Risk Manager.\n\n"
    "R\u00c8GLES ABSOLUES :\n"
    "1. SMA200 PRIORIT\u00c9 ABSOLUE : bias_sma200=BULLISH si prix>SMA200_D1, sinon BEARISH.\n"
    "2. R\u00c9GIME D1 : BULLISH (prix>SMA200 ET SMA50>SMA200), BEARISH (prix<SMA200 ET SMA50<SMA200), TRANSITION, NEUTRAL_RANGE.\n"
    "3. Z\u00c9RO HALLUCINATION : Ne jamais inventer. Manquant -> missing_fields.\n"
    "4. COH\u00c9RENCE : validated=true exige decision=APPROVE + stop_loss non null. stop_loss=null exige validated=false.\n"
    "5. DONN\u00c9ES OBLIGATOIRES : SMA200_D1 et prix_D1. Si absents -> REJECT, quality_score<=3.\n"
    "6. Si bars H1<20 : WATCH si WITH_BIAS, sinon REJECT. Pas de stop loss.\n"
    "7. ALIGNEMENT : WITH_BIAS (BUY+BULLISH ou SELL+BEARISH), AGAINST_BIAS, MIXED, UNKNOWN.\n"
    "8. GATE RR (BUY) : RR<1.2->REJECT, 1.2-1.5->WATCH, >=1.5->APPROVE, null->WATCH si WITH_BIAS.\n"
    "9. Suivi tendance->APPROVE candidat (quality 7-10). Contre-tendance->REJECT sauf RSI extr\u00eame.\n"
    "10. STOP LOSS : swing H1, buffer 0.1-0.3%.\n"
    "11. REASONING : D\u00e9buter par 'Tendance de fond Haussiere car Prix > SMA200.' ou 'Baissiere'. Max 2 phrases."
)

AI_USER = (
    "=Tu vas recevoir un objet JSON pour un symbole contenant :\n"
    "- signal_tactical_H1 : signal + indicateurs H1\n"
    "- primary_context_D1 : indicateurs D1 (Prix_D1/last_close, SMA200_D1, SMA50_D1, etc.)\n"
    "- bars : derni\u00e8res bougies H1 (OHLCV)\n"
    "- rr_theoretical : ratio R/R th\u00e9orique\n"
    "- rr_meta : champs d'audit (m\u00e9thode, distances)\n\n"
    "Ta mission : appliquer STRICTEMENT les r\u00e8gles H1\u2194D1 et retourner UNIQUEMENT le JSON de d\u00e9cision conforme au sch\u00e9ma.\n\n"
    "R\u00c8GLES D'ACC\u00c8S AUX DONN\u00c9ES :\n"
    "- SMA200_D1, SMA50_D1, Prix_D1 : dans primary_context_D1.indicators\n"
    "- RSI_H1 : dans signal_tactical_H1.indicators\n"
    "- Bougies H1 : dans bars\n"
    "- RR : rr_theoretical (ne pas r\u00e9inventer TP/SL si fourni)\n"
    "- Si bars < 20 : tu NE PEUX PAS proposer de stop \u21d2 validated=false\n\n"
    "DONN\u00c9ES :\n"
    "{{ JSON.stringify($json.ai_context, null, 2) }}"
)

AI_SCHEMA = json.dumps({
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "validated": {"type": "boolean"},
        "decision": {"type": "string", "enum": ["APPROVE", "REJECT", "WATCH"]},
        "quality_score": {"type": "integer", "minimum": 1, "maximum": 10},
        "bias_sma200": {"type": "string", "enum": ["BULLISH", "BEARISH"]},
        "regime_d1": {"type": "string", "enum": ["BULLISH", "BEARISH", "NEUTRAL_RANGE", "TRANSITION"]},
        "h1_d1_alignment": {"type": "string", "enum": ["WITH_BIAS", "AGAINST_BIAS", "MIXED", "UNKNOWN"]},
        "reasoning": {"type": "string"},
        "chart_pattern": {"type": "string"},
        "stop_loss_suggestion": {"type": ["number", "null"]},
        "stop_loss_basis": {"type": "string", "enum": ["SWING_H1", "BAR_ANCHOR", "NONE"]},
        "missing_fields": {"type": "array", "items": {"type": "string"}},
        "anomalies": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["validated", "decision", "quality_score", "reasoning",
                  "bias_sma200", "regime_d1", "h1_d1_alignment",
                  "chart_pattern", "stop_loss_suggestion", "stop_loss_basis",
                  "missing_fields", "anomalies"],
})

nodes = []
connections = {}

def add(nid, name, ntype, ver, pos, params, creds=None, extra=None):
    n = {"parameters": params, "type": ntype, "typeVersion": ver, "position": pos, "id": nid, "name": name}
    if creds: n["credentials"] = creds
    if extra:
        for k,v in extra.items(): n[k] = v
    nodes.append(n)

def conn(fr, to, fo=0, ti=0, ct="main"):
    connections.setdefault(fr, {}).setdefault(ct, [])
    while len(connections[fr][ct]) <= fo: connections[fr][ct].append([])
    connections[fr][ct][fo].append({"node": to, "type": ct, "index": ti})

# ─── TRIGGERS ───
add(IDS["cron"], "Cron Trigger", "n8n-nodes-base.scheduleTrigger", 1.2, [-1248, 112],
    {"rule": {"interval": [{"field": "cronExpression", "expression": "10 9-17 * * 1-5"}]}})

add(IDS["manual"], "Manual Trigger", "n8n-nodes-base.manualTrigger", 1, [-1248, 304], {})

# ─── READ UNIVERSE ───
add(IDS["read_universe"], "Read Universe", "n8n-nodes-base.googleSheets", 4.5, [-1008, 208],
    {"documentId": {"__rl": True, "mode": "list", "value": SHEET_ID,
                     "cachedResultName": "TradingSim_GoogleSheet_Template",
                     "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit?usp=drivesdk"},
     "sheetName": {"__rl": True, "mode": "list", "value": UNI_GID,
                    "cachedResultName": "Universe",
                    "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid={UNI_GID}"},
     "filtersUI": {"values": [{"lookupColumn": "Enabled", "lookupValue": "true"}]},
     "options": {}},
    creds={"googleSheetsOAuth2Api": {"id": GS_CRED, "name": "Google Sheets account"}})

# ─── INIT CONFIG ───
add(IDS["init_config"], "Init Config + Batch", "n8n-nodes-base.code", 2, [-752, 208],
    {"jsCode": load("01_init_config.js")})

# ─── DUCKDB INIT ───
add(IDS["duckdb_init"], "DuckDB Init Schema", "n8n-nodes-base.code", 2, [-496, 208],
    {"language": "pythonNative", "pythonCode": load("02_duckdb_init.py")})

# ─── LOOP ───
add(IDS["loop"], "Loop Symbols", "n8n-nodes-base.splitInBatches", 3, [-272, 208],
    {"options": {}}, extra={"onError": "continueRegularOutput"})

# ─── FINALIZE ───
add(IDS["finalize"], "Finalize Run", "n8n-nodes-base.code", 2, [-16, 0],
    {"language": "pythonNative", "pythonCode": load("10_finalize.py")})

# ─── SYNC SHEETS (DuckDB → Sheets format) ───
SORTIE_GID = 1444959091
add(IDS["sync_sheets"], "Sync DuckDB → Sheets", "n8n-nodes-base.code", 2, [224, 0],
    {"language": "pythonNative", "pythonCode": load("11_sync_sheets.py")},
    extra={"onError": "continueRegularOutput"})

# Build column mapping for Google Sheets node (all columns as expressions)
_SHEET_COLS = [
    "Symbol","Run_ID","Workflow_Date",
    "H1_Action","H1_Score","H1_Confidence","H1_Rationale",
    "H1_Date","H1_Source","H1_Status","H1_Warnings",
    "D1_Action","D1_Score","D1_Confidence","D1_Rationale",
    "D1_Date","D1_Source","D1_Status","D1_Warnings",
    "Last_Close","SMA_200","Reason","Pass_AI","Pass_PM",
    "Sig_JSON","AI_Output","vector_status","vectorizedAt","row_hash","TTL_Minutes",
    "AI_Decision","AI_Validated","AI_QualityScore","AI_Reasoning",
    "AI_Alignment","AI_Bias_SMA200","AI_Regime_D1",
    "AI_StopLoss","AI_StopLoss_Basis","AI_MissingFields","AI_Anomalies",
    "AI_ChartPattern","AI_RR_Theoretical",
    # H1 indicators
    "H1_SMA20","H1_SMA50","H1_SMA200","H1_EMA12","H1_EMA26",
    "H1_MACD","H1_MACD_Signal","H1_MACD_Hist","H1_RSI14",
    "H1_Volatility_Ann","H1_Last_Close_Ind","H1_ATR_Value","H1_ATR_Pct",
    "H1_Resistance_50","H1_Dist_To_Res_Pct_50",
    # D1 indicators
    "D1_SMA20","D1_SMA50","D1_SMA200","D1_EMA12","D1_EMA26",
    "D1_MACD","D1_MACD_Signal","D1_MACD_Hist","D1_RSI14",
    "D1_Volatility_Ann","D1_Last_Close_Ind","D1_ATR_Value","D1_ATR_Pct",
    "D1_Resistance_50","D1_Dist_To_Res_Pct_50",
    # Combined (H1 as default)
    "SMA20","SMA50","EMA12","EMA26","MACD","MACD_Signal","MACD_Hist",
    "RSI14","Volatility_Ann","Last_Close_Ind","ATR_Value","ATR_Pct",
    "Resistance_50","Dist_To_Res_Pct_50",
    # V2 extras
    "H1_BB_Upper","H1_BB_Lower","H1_BB_Width","H1_Stoch_K","H1_Stoch_D",
    "H1_ADX","H1_OBV_Slope","H1_Support","H1_Dist_To_Sup_Pct",
    "D1_BB_Upper","D1_BB_Lower","D1_BB_Width","D1_Stoch_K","D1_Stoch_D",
    "D1_ADX","D1_OBV_Slope","D1_Support","D1_Dist_To_Sup_Pct",
]
_col_value = {c: "={{ $json." + c + " }}" for c in _SHEET_COLS}

add(IDS["write_sheets"], "Write AG2 Sortie", "n8n-nodes-base.googleSheets", 4.5, [480, 0],
    {"operation": "appendOrUpdate",
     "documentId": {"__rl": True, "mode": "list", "value": SHEET_ID,
                     "cachedResultName": "TradingSim_GoogleSheet_Template",
                     "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit?usp=drivesdk"},
     "sheetName": {"__rl": True, "mode": "list", "value": SORTIE_GID,
                    "cachedResultName": "AG2 - étape 1 - sortie",
                    "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid={SORTIE_GID}"},
     "columns": {"mappingMode": "defineBelow", "value": _col_value,
                  "matchingColumns": ["Symbol"], "schema": [{"id": c, "displayName": c, "required": False, "defaultMatch": c == "Symbol", "display": True, "type": "string", "canBeUsedToMatch": True} for c in _SHEET_COLS]},
     "options": {}},
    creds={"googleSheetsOAuth2Api": {"id": GS_CRED, "name": "Google Sheets account"}},
    extra={"onError": "continueRegularOutput"})

# ─── HTTP FETCH (parallel H1 + D1) ───
http_h1_params = {
    "url": "={{$json.yfinance_api_base}}/history",
    "sendQuery": True,
    "queryParameters": {"parameters": [
        {"name": "symbol", "value": "={{$json.symbol}}"},
        {"name": "interval", "value": "={{ $json.intraday.interval }}"},
        {"name": "lookback_days", "value": "={{ $json.intraday.lookback_days }}"},
        {"name": "max_bars", "value": "={{ $json.intraday.max_bars }}"},
        {"name": "min_bars", "value": "={{ $json.intraday.min_bars }}"},
        {"name": "allow_stale", "value": "true"},
    ]},
    "options": {"response": {"response": {"responseFormat": "json"}}, "timeout": 60000}
}
add(IDS["http_h1"], "AG2.10 \u2014 HTTP \u2014 Fetch Yahoo OHLCV (1H Timing)",
    "n8n-nodes-base.httpRequest", 4.1, [80, 160], http_h1_params,
    extra={"onError": "continueRegularOutput"})

http_d1_params = {
    "url": "={{$json.yfinance_api_base}}/history",
    "sendQuery": True,
    "queryParameters": {"parameters": [
        {"name": "symbol", "value": "={{$json.symbol}}"},
        {"name": "interval", "value": "={{ $json.daily.interval }}"},
        {"name": "lookback_days", "value": "={{ $json.daily.lookback_days }}"},
        {"name": "max_bars", "value": "={{ $json.daily.max_bars }}"},
        {"name": "min_bars", "value": "={{ $json.daily.min_bars }}"},
        {"name": "allow_stale", "value": "true"},
    ]},
    "options": {"response": {"response": {"responseFormat": "json"}}, "timeout": 60000}
}
add(IDS["http_d1"], "AG2.15 \u2014 HTTP \u2014 Fetch Yahoo OHLCV (1D Strategy)",
    "n8n-nodes-base.httpRequest", 4.1, [80, 336], http_d1_params,
    extra={"onError": "continueRegularOutput"})

# ─── WRAP H1 / D1 ───
add(IDS["wrap_h1"], "AG2.11 \u2014 Code \u2014 Wrap H1", "n8n-nodes-base.code", 2, [304, 160],
    {"jsCode": load("03a_wrap_h1.js")})

add(IDS["wrap_d1"], "AG2.16 \u2014 Code \u2014 Wrap D1", "n8n-nodes-base.code", 2, [304, 336],
    {"jsCode": load("03b_wrap_d1.js")})

# ─── MERGE ───
add(IDS["merge"], "Merge", "n8n-nodes-base.merge", 3.2, [544, 240],
    {"mode": "combine", "combineBy": "combineByPosition", "options": {}})

# ─── COMPUTE + FILTER + WRITE ───
add(IDS["compute"], "Compute + Filter + Write", "n8n-nodes-base.code", 2, [736, 240],
    {"language": "pythonNative", "pythonCode": load("04_compute.py")},
    extra={"onError": "continueRegularOutput"})

# ─── IF CALL AI? ───
add(IDS["if_ai"], "IF Call AI?", "n8n-nodes-base.if", 2.2, [960, 320],
    {"conditions": {"options": {"version": 2, "caseSensitive": True, "leftValue": ""},
                    "combinator": "and",
                    "conditions": [{"id": "1", "operator": {"type": "boolean", "operation": "true"},
                                    "leftValue": "={{ $json.call_ai }}", "rightValue": ""}]},
     "options": {}},
    extra={"alwaysOutputData": True})

# ─── SNAPSHOT CONTEXT ───
add(IDS["snapshot"], "Snapshot Context", "n8n-nodes-base.code", 2, [1168, 208],
    {"jsCode": load("05_snapshot.js")})

# ─── AI VALIDATION GPT ───
add(IDS["ai_validate"], "AI Validation GPT", "@n8n/n8n-nodes-langchain.openAi", 2.1, [1376, 208],
    {"modelId": {"__rl": True, "mode": "list", "value": "gpt-4o-mini"},
     "responses": {"values": [
         {"role": "system", "content": AI_SYSTEM},
         {"content": AI_USER},
     ]},
     "builtInTools": {},
     "options": {"maxTokens": 512,
                 "textFormat": {"textOptions": {"type": "json_schema", "schema": "=" + AI_SCHEMA}},
                 "temperature": 0.1}},
    creds={"openAiApi": {"id": OAI_CRED, "name": "OpenAi account"}})

# ─── EXTRACT AI + WRITE ───
add(IDS["extract_ai"], "Extract AI + Write", "n8n-nodes-base.code", 2, [1680, 208],
    {"language": "pythonNative", "pythonCode": load("06_extract_ai.py")},
    extra={"onError": "continueRegularOutput"})

# ─── IF VECTORIZE? ───
add(IDS["if_vector"], "IF Vectorize?", "n8n-nodes-base.if", 2.2, [1968, 464],
    {"conditions": {"options": {"version": 2, "caseSensitive": True, "leftValue": ""},
                    "combinator": "and",
                    "conditions": [{"id": "2", "operator": {"type": "boolean", "operation": "true"},
                                    "leftValue": "={{ $json.should_vectorize }}", "rightValue": ""}]},
     "options": {}},
    extra={"alwaysOutputData": True})

# ─── PREP VECTOR TEXT ───
add(IDS["prep_vector"], "Prep Vector Text", "n8n-nodes-base.code", 2, [2176, 112],
    {"jsCode": load("08_prep_vector.js")})

# ─── EMBEDDINGS ───
add(IDS["embed_openai"], "Embeddings OpenAI", "@n8n/n8n-nodes-langchain.embeddingsOpenAi", 1.2, [2336, 288],
    {"options": {}},
    creds={"openAiApi": {"id": OAI_CRED, "name": "OpenAi account"}})

# ─── DATA LOADER ───
add(IDS["data_loader"], "Default Data Loader", "@n8n/n8n-nodes-langchain.documentDefaultDataLoader", 1, [2384, 416],
    {"options": {}})

# ─── TEXT SPLITTER ───
add(IDS["text_splitter"], "Text Splitter", "@n8n/n8n-nodes-langchain.textSplitterRecursiveCharacterTextSplitter", 1, [2384, 608],
    {"chunkSize": 10000, "chunkOverlap": 200, "options": {}})

# ─── QDRANT ───
add(IDS["qdrant"], "Qdrant Upsert", "@n8n/n8n-nodes-langchain.vectorStoreQdrant", 1.1, [2416, 112],
    {"mode": "insert", "qdrantCollection": {"__rl": True, "mode": "list", "value": "financial_tech_v1"},
     "options": {"collectionConfig": {"similarity": "Cosine"}}},
    creds={"qdrantApi": {"id": QD_CRED, "name": "QdrantApi account"}})

# ─── MARK VECTORIZED ───
add(IDS["mark_vector"], "Mark Vectorized", "n8n-nodes-base.code", 2, [2848, 576],
    {"language": "pythonNative", "pythonCode": load("09_mark_vector.py")})

# ─── CONNECTIONS ───
conn("Cron Trigger", "Read Universe")
conn("Manual Trigger", "Read Universe")
conn("Read Universe", "Init Config + Batch")
conn("Init Config + Batch", "DuckDB Init Schema")
conn("DuckDB Init Schema", "Loop Symbols")

# Loop output 0 = done → Finalize, output 1 = each → both HTTP nodes
conn("Loop Symbols", "Finalize Run", fo=0)
conn("Loop Symbols", "AG2.15 \u2014 HTTP \u2014 Fetch Yahoo OHLCV (1D Strategy)", fo=1)
conn("Loop Symbols", "AG2.10 \u2014 HTTP \u2014 Fetch Yahoo OHLCV (1H Timing)", fo=1)

# HTTP → Wrap
conn("AG2.10 \u2014 HTTP \u2014 Fetch Yahoo OHLCV (1H Timing)", "AG2.11 \u2014 Code \u2014 Wrap H1")
conn("AG2.15 \u2014 HTTP \u2014 Fetch Yahoo OHLCV (1D Strategy)", "AG2.16 \u2014 Code \u2014 Wrap D1")

# Wrap → Merge (H1 to input 0, D1 to input 1)
conn("AG2.11 \u2014 Code \u2014 Wrap H1", "Merge", ti=0)
conn("AG2.16 \u2014 Code \u2014 Wrap D1", "Merge", ti=1)

# Merge → Compute → IF
conn("Merge", "Compute + Filter + Write")
conn("Compute + Filter + Write", "IF Call AI?")

# IF Call AI → true: Snapshot, false: Loop
conn("IF Call AI?", "Snapshot Context", fo=0)
conn("IF Call AI?", "Loop Symbols", fo=1)

# AI pipeline
conn("Snapshot Context", "AI Validation GPT")
conn("AI Validation GPT", "Extract AI + Write")
conn("Extract AI + Write", "IF Vectorize?")

# IF Vectorize → true: Prep, false: Loop
conn("IF Vectorize?", "Prep Vector Text", fo=0)
conn("IF Vectorize?", "Loop Symbols", fo=1)

# Vectorization pipeline
conn("Prep Vector Text", "Qdrant Upsert")
conn("Qdrant Upsert", "Mark Vectorized")
conn("Mark Vectorized", "Loop Symbols")

# LangChain sub-connections
conn("Embeddings OpenAI", "Qdrant Upsert", ct="ai_embedding")
conn("Default Data Loader", "Qdrant Upsert", ct="ai_document")
conn("Text Splitter", "Default Data Loader", ct="ai_textSplitter")

# Finalize → Sync Sheets → Write Sheets
conn("Finalize Run", "Sync DuckDB → Sheets")
conn("Sync DuckDB → Sheets", "Write AG2 Sortie")

wf = {
    "nodes": nodes,
    "connections": connections,
    "pinData": {},
    "meta": {"instanceId": "093ba1266e85fcaaa6abca411d9f7941951737b3920bfaa6e2db89226df2c82d"},
}

print(json.dumps(wf, indent=2, ensure_ascii=False))
