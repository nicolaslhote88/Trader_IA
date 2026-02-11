#!/usr/bin/env python3
"""
AG2-V2 Workflow Builder — Generates importable n8n workflow JSON.
Run: cd ag2-v2 && python3 build_workflow.py > AG2-V2-workflow.json
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
UNI_GID = "1078848687"

nid = lambda: str(uuid.uuid4())
IDS = {k: nid() for k in [
    "cron","manual","read_universe","init_config","duckdb_init","loop",
    "fetch_data","compute","if_ai","snapshot","ai_validate","extract_ai",
    "if_vector","prep_vector","embed_openai","data_loader","text_splitter",
    "qdrant","mark_vector","finalize"]}

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

AI_USER = "Analyse ce signal technique :\n\n{{ JSON.stringify($json.ai_context, null, 2) }}\n\nR\u00e9ponds en JSON strict."

AI_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "validated": {"type": "boolean"},
        "decision": {"type": "string", "enum": ["APPROVE","WATCH","REJECT"]},
        "quality_score": {"type": "integer", "minimum": 1, "maximum": 10},
        "bias_sma200": {"type": "string", "enum": ["BULLISH","BEARISH"]},
        "regime_d1": {"type": "string", "enum": ["BULLISH","BEARISH","NEUTRAL_RANGE","TRANSITION"]},
        "h1_d1_alignment": {"type": "string", "enum": ["WITH_BIAS","AGAINST_BIAS","MIXED","UNKNOWN"]},
        "reasoning": {"type": "string"},
        "chart_pattern": {"type": "string"},
        "stop_loss_suggestion": {"type": ["number","null"]},
        "stop_loss_basis": {"type": ["string","null"], "enum": ["SWING_H1","BAR_ANCHOR","NONE",None]},
        "missing_fields": {"type": "array", "items": {"type": "string"}},
        "anomalies": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["validated","decision","quality_score","reasoning","bias_sma200","regime_d1","h1_d1_alignment"],
    "additionalProperties": False,
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

# ─── NODES ───
add(IDS["cron"], "Cron Trigger", "n8n-nodes-base.scheduleTrigger", 1.2, [200,0],
    {"rule": {"interval": [{"field": "cronExpression", "expression": "10 9-17 * * 1-5"}]}})

add(IDS["manual"], "Manual Trigger", "n8n-nodes-base.manualTrigger", 1, [200,200], {})

add(IDS["read_universe"], "Read Universe", "n8n-nodes-base.googleSheets", 4.5, [450,100],
    {"operation": "read",
     "documentId": {"__rl": True, "mode": "id", "value": SHEET_ID},
     "sheetName": {"__rl": True, "mode": "gid", "value": UNI_GID},
     "filtersUI": {"values": [{"lookupColumn": "Enabled", "lookupValue": "TRUE"}]}, "options": {}},
    creds={"googleSheetsOAuth2Api": {"id": GS_CRED, "name": "Google Sheets account"}})

add(IDS["init_config"], "Init Config + Batch", "n8n-nodes-base.code", 2, [700,100],
    {"jsCode": load("01_init_config.js"), "mode": "runOnceForAllItems"})

add(IDS["duckdb_init"], "DuckDB Init Schema", "n8n-nodes-base.code", 2, [950,100],
    {"language": "python", "pythonCode": load("02_duckdb_init.py"), "mode": "runOnceForAllItems"})

add(IDS["loop"], "Loop Symbols", "n8n-nodes-base.splitInBatches", 3, [1200,100],
    {"batchSize": 1, "options": {}}, extra={"onError": "continueRegularOutput"})

add(IDS["fetch_data"], "Fetch H1 + D1", "n8n-nodes-base.code", 2, [1450,200],
    {"jsCode": load("03_fetch_data.js"), "mode": "runOnceForEachItem"})

add(IDS["compute"], "Compute + Filter + Write", "n8n-nodes-base.code", 2, [1700,200],
    {"language": "python", "pythonCode": load("04_compute.py"), "mode": "runOnceForAllItems"},
    extra={"onError": "continueRegularOutput"})

add(IDS["if_ai"], "IF Call AI?", "n8n-nodes-base.if", 2.2, [1950,200],
    {"conditions": {"options": {"version": 2, "caseSensitive": True, "leftValue": ""},
                    "combinator": "and",
                    "conditions": [{"id": "1", "operator": {"type": "boolean", "operation": "true"},
                                    "leftValue": "={{ $json.call_ai }}", "rightValue": ""}]}},
    extra={"alwaysOutputData": True})

add(IDS["snapshot"], "Snapshot Context", "n8n-nodes-base.code", 2, [2200,100],
    {"jsCode": load("05_snapshot.js"), "mode": "runOnceForEachItem"})

add(IDS["ai_validate"], "AI Validation GPT", "@n8n/n8n-nodes-langchain.openAi", 2.1, [2450,100],
    {"modelId": {"__rl": True, "mode": "list", "value": "gpt-4o-mini"},
     "messages": {"values": [{"content": AI_USER, "role": "user"}]},
     "jsonOutput": True, "jsonSchemaType": "manual", "inputSchema": AI_SCHEMA,
     "options": {"systemMessage": AI_SYSTEM, "maxTokens": 512, "temperature": 0.1}},
    creds={"openAiApi": {"id": OAI_CRED, "name": "OpenAi account"}})

add(IDS["extract_ai"], "Extract AI + Write", "n8n-nodes-base.code", 2, [2700,100],
    {"language": "python", "pythonCode": load("06_extract_ai.py"), "mode": "runOnceForAllItems"},
    extra={"onError": "continueRegularOutput"})

add(IDS["if_vector"], "IF Vectorize?", "n8n-nodes-base.if", 2.2, [2950,100],
    {"conditions": {"options": {"version": 2, "caseSensitive": True, "leftValue": ""},
                    "combinator": "and",
                    "conditions": [{"id": "2", "operator": {"type": "boolean", "operation": "true"},
                                    "leftValue": "={{ $json.should_vectorize }}", "rightValue": ""}]}},
    extra={"alwaysOutputData": True})

add(IDS["prep_vector"], "Prep Vector Text", "n8n-nodes-base.code", 2, [3200,0],
    {"jsCode": load("08_prep_vector.js"), "mode": "runOnceForEachItem"})

add(IDS["embed_openai"], "Embeddings OpenAI", "@n8n/n8n-nodes-langchain.embeddingsOpenAi", 1.2, [3550,200],
    {"model": "text-embedding-3-small", "options": {}},
    creds={"openAiApi": {"id": OAI_CRED, "name": "OpenAi account"}})

add(IDS["data_loader"], "Default Data Loader", "@n8n/n8n-nodes-langchain.documentDefaultDataLoader", 1, [3550,350],
    {"dataType": "json", "jsonData": "={{ $json.text }}", "options": {"metadata": "={{ JSON.stringify($json.metadata) }}"}})

add(IDS["text_splitter"], "Text Splitter", "@n8n/n8n-nodes-langchain.textSplitterRecursiveCharacterTextSplitter", 1, [3550,500],
    {"chunkSize": 2000, "chunkOverlap": 200})

add(IDS["qdrant"], "Qdrant Upsert", "@n8n/n8n-nodes-langchain.vectorStoreQdrant", 1.1, [3450,0],
    {"mode": "insert", "qdrantCollection": {"__rl": True, "mode": "list", "value": "financial_tech_v1"},
     "options": {"collectionConfig": {"similarity": "Cosine"}}},
    creds={"qdrantApi": {"id": QD_CRED, "name": "QdrantApi account"}})

add(IDS["mark_vector"], "Mark Vectorized", "n8n-nodes-base.code", 2, [3700,0],
    {"language": "python", "pythonCode": load("09_mark_vector.py"), "mode": "runOnceForAllItems"})

add(IDS["finalize"], "Finalize Run", "n8n-nodes-base.code", 2, [1450,-100],
    {"language": "python", "pythonCode": load("10_finalize.py"), "mode": "runOnceForAllItems"})

# ─── CONNECTIONS ───
conn("Cron Trigger", "Read Universe")
conn("Manual Trigger", "Read Universe")
conn("Read Universe", "Init Config + Batch")
conn("Init Config + Batch", "DuckDB Init Schema")
conn("DuckDB Init Schema", "Loop Symbols")
conn("Loop Symbols", "Fetch H1 + D1", fo=1)
conn("Loop Symbols", "Finalize Run", fo=0)
conn("Fetch H1 + D1", "Compute + Filter + Write")
conn("Compute + Filter + Write", "IF Call AI?")
conn("IF Call AI?", "Snapshot Context", fo=0)
conn("IF Call AI?", "Loop Symbols", fo=1)
conn("Snapshot Context", "AI Validation GPT")
conn("AI Validation GPT", "Extract AI + Write")
conn("Extract AI + Write", "IF Vectorize?")
conn("IF Vectorize?", "Prep Vector Text", fo=0)
conn("IF Vectorize?", "Loop Symbols", fo=1)
conn("Prep Vector Text", "Qdrant Upsert")
conn("Qdrant Upsert", "Mark Vectorized")
conn("Mark Vectorized", "Loop Symbols")
conn("Embeddings OpenAI", "Qdrant Upsert", ct="ai_embedding")
conn("Default Data Loader", "Qdrant Upsert", ct="ai_document")
conn("Text Splitter", "Default Data Loader", ct="ai_textSplitter")

wf = {
    "meta": {"instanceId": "ag2-v2-generated", "templateCredsSetupCompleted": True},
    "nodes": nodes, "connections": connections,
    "active": False,
    "settings": {"executionOrder": "v1", "saveManualExecutions": True, "callerPolicy": "workflowsFromSameOwner"},
    "versionId": "ag2-v2-1.0",
    "name": "AG2-V2 - Technical Analyst (unified)",
    "tags": [{"name": "AG2"}, {"name": "V2"}, {"name": "Technical Analysis"}],
}

print(json.dumps(wf, indent=2, ensure_ascii=False))
