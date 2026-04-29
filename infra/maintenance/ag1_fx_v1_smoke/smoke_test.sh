#!/usr/bin/env bash
set -euo pipefail

ROOT="${TRADER_IA_ROOT:-/files/Trader_IA}"
DUCKDB_DIR="${DUCKDB_DIR:-/files/duckdb}"

AG2_DB="${AG2_FX_V1_DUCKDB_PATH:-$DUCKDB_DIR/ag2_fx_v1.duckdb}"
AG4_DB="${AG4_FX_V1_DUCKDB_PATH:-$DUCKDB_DIR/ag4_fx_v1.duckdb}"
AG1_GPT_DB="${AG1_FX_V1_CHATGPT52_DUCKDB_PATH:-$DUCKDB_DIR/ag1_fx_v1_chatgpt52.duckdb}"
AG1_GROK_DB="${AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH:-$DUCKDB_DIR/ag1_fx_v1_grok41_reasoning.duckdb}"
AG1_GEMINI_DB="${AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH:-$DUCKDB_DIR/ag1_fx_v1_gemini30_pro.duckdb}"

"${PYTHON_BIN:-python3}" - <<'PY'
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

root = Path(os.environ.get("TRADER_IA_ROOT", "/files/Trader_IA"))
dbs = {
    "ag2": Path(os.environ.get("AG2_FX_V1_DUCKDB_PATH", "/files/duckdb/ag2_fx_v1.duckdb")),
    "ag4": Path(os.environ.get("AG4_FX_V1_DUCKDB_PATH", "/files/duckdb/ag4_fx_v1.duckdb")),
    "chatgpt52": Path(os.environ.get("AG1_FX_V1_CHATGPT52_DUCKDB_PATH", "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb")),
    "grok41_reasoning": Path(os.environ.get("AG1_FX_V1_GROK41_REASONING_DUCKDB_PATH", "/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb")),
    "gemini30_pro": Path(os.environ.get("AG1_FX_V1_GEMINI30_PRO_DUCKDB_PATH", "/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb")),
}

pairs = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD", "AUDJPY", "AUDNZD", "AUDCAD",
    "NZDJPY", "NZDCAD", "CADJPY", "CHFJPY", "CADCHF", "CHFCAD", "JPYNZD",
]

def run_sql(path, sql_file):
    path.parent.mkdir(parents=True, exist_ok=True)
    sql = Path(sql_file).read_text(encoding="utf-8")
    with duckdb.connect(str(path)) as con:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            con.execute(stmt)

run_sql(dbs["ag2"], root / "infra/migrations/ag2_fx_v1/20260426_init.sql")
run_sql(dbs["ag4"], root / "infra/migrations/ag4_fx_v1/20260426_init.sql")
for key in ("chatgpt52", "grok41_reasoning", "gemini30_pro"):
    run_sql(dbs[key], root / "infra/migrations/ag1_fx_v1/20260426_init.sql")

run_id = "SMOKE_AG2FX_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
with duckdb.connect(str(dbs["ag2"])) as con:
    con.executemany(
        """
        INSERT OR REPLACE INTO main.universe_fx VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [(p, f"{p}=X", p[:3], p[3:], 0.01 if p.endswith("JPY") else 0.0001, 3 if p.endswith("JPY") else 5, "major" if p in pairs[:7] else "cross") for p in pairs],
    )
    rows = []
    for i, p in enumerate(pairs):
        px = 1.0 + (i / 100.0)
        if p.endswith("JPY"):
            px *= 100
        rows.append((run_id, datetime.now(timezone.utc), p, px, 0.001, 0.002, 0.004, 52, 0.003, px*0.99, px*0.98, px*0.95, px*0.995, px*0.996, 0.01, 0.005, 0.005, px*1.02, px*0.98, 0.04, px, px*1.01, px*1.02, px*0.99, px*0.98, "range", 0.1, "neutral", 0.01 if p.endswith("JPY") else 0.0001, p[:3], p[3:]))
    con.executemany(
        "INSERT OR REPLACE INTO main.technical_signals_fx VALUES (" + ",".join(["?"] * 31) + ")",
        rows,
    )
    con.execute("INSERT OR REPLACE INTO main.run_log VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 27, 27, 0, 'smoke')", [run_id])
    signals = con.execute("SELECT COUNT(DISTINCT pair) FROM main.technical_signals_fx WHERE run_id=?", [run_id]).fetchone()[0]

run4 = "SMOKE_AG4FX_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
with duckdb.connect(str(dbs["ag4"])) as con:
    sections = {
        "top_news": {"items": [], "as_of": datetime.now(timezone.utc).isoformat(), "lookback_hours": 24},
        "pair_focus": {"pairs": {}},
        "macro_regime": {"market_regime": "smoke", "drivers": "", "confidence": 0.0, "biases": {}, "as_of": datetime.now(timezone.utc).isoformat()},
    }
    for name, payload in sections.items():
        con.execute("INSERT OR REPLACE INTO main.fx_digest VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, CURRENT_TIMESTAMP)", [run4, name, json.dumps(payload), 0])
    con.execute("INSERT OR REPLACE INTO main.run_log VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, 0, 0, 3, 0, 'smoke')", [run4])
    digest_sections = con.execute("SELECT COUNT(*) FROM main.fx_digest WHERE run_id=?", [run4]).fetchone()[0]

with duckdb.connect(str(dbs["chatgpt52"])) as con:
    cfg = con.execute("SELECT leverage_max, kill_switch_active FROM cfg.portfolio_config WHERE config_key='default'").fetchone()
    if cfg != (1.0, False):
        raise SystemExit(f"AG1 cfg invalid: {cfg}")
    run1 = "SMOKE_AG1FX_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    con.execute("INSERT OR REPLACE INTO core.runs VALUES (?, 'gpt-5.2-2025-12-11', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, 27, 0, 0, 0, 1.0, FALSE, 'smoke dry-run hold')", [run1, json.dumps({"decisions": [{"pair": p, "decision": "hold", "conviction": 0.1} for p in pairs]})])
    decisions = con.execute("SELECT decisions_count FROM core.runs WHERE run_id=?", [run1]).fetchone()[0]
    orders_valid = con.execute("SELECT COUNT(*) FROM core.orders WHERE run_id=? AND status='filled'", [run1]).fetchone()[0]
    orders_rejected = con.execute("SELECT COUNT(*) FROM core.orders WHERE run_id=? AND status='rejected'", [run1]).fetchone()[0]

print(json.dumps({
    "dbs_checked": {k: str(v) for k, v in dbs.items()},
    "ag2_signals": signals,
    "ag4_digest_sections": digest_sections,
    "ag1_chatgpt52_decisions": decisions,
    "orders_valid": orders_valid,
    "orders_rejected": orders_rejected,
}, indent=2))
PY
