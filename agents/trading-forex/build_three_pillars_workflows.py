#!/usr/bin/env python3
"""Génère les déclencheurs n8n minces AG5-AG9 et synthèse.

Les formules et écritures DuckDB vivent exclusivement dans les services.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
COMMON = ROOT.parent / "common" / "global-context"

WORKFLOWS = [
    {"id": "AG5FXMacroPillarsV1", "name": "AG5 — Macro & Flows V2", "cron": "20 7 * * 1-5", "url": "http://macro-data-api:8081/components/ag5/refresh?refresh_sources=true", "kind": "component", "legacy": "AG5-FX-Macro/workflow/AG5-FX-Macro_workflow_v1.json", "common": "AG5-Macro/AG5-Macro-V2-workflow.json"},
    {"id": "AG6FXValuationPillarsV1", "name": "AG6 — FX Relative Valuation V2", "cron": "40 7 * * 1-5", "url": "http://macro-data-api:8081/components/ag6/compute", "kind": "component", "legacy": "AG6-FX-Valuation/workflow/AG6-FX-Valuation_workflow_v1.json", "common": "AG6-FX-Valuation/AG6-FX-Valuation-V2-workflow.json"},
    {"id": "AG7FXPositioningPillarsV1", "name": "AG7 — Positioning V2", "cron": "0 8 * * 1-5", "url": "http://macro-data-api:8081/components/ag7/refresh?refresh_source=true", "kind": "component", "legacy": "AG7-FX-Positioning/workflow/AG7-FX-Positioning_workflow_v1.json", "common": "AG7-Positioning/AG7-Positioning-V2-workflow.json"},
    {"id": "AG8FXRatesPillarsV1", "name": "AG8 — Rates & Liquidity V2", "cron": "20 8 * * 1-5", "url": "http://macro-data-api:8081/components/ag8/compute", "kind": "component", "legacy": "AG8-FX-Rates/workflow/AG8-FX-Rates_workflow_v1.json", "common": "AG8-Rates-Liquidity/AG8-Rates-Liquidity-V2-workflow.json"},
    {"id": "AG9GLOBALRISK20260805", "name": "AG9 — Global Risk Intelligence", "cron": "35 9,12,15 * * 1-5", "url": "={{ ($env.WORLD_MONITOR_ADAPTER_URL || 'http://worldmonitor-adapter:8082') + '/ag9/refresh' }}", "kind": "snapshot", "common": "AG9-Global-Risk/AG9-Global-Risk-V1-workflow.json"},
    {"id": "GLOBALCONTEXTSYNTH20260805", "name": "Global Context — AG5-AG9 Synthesizer", "cron": "5 10,13,16 * * 1-5", "url": "http://global-context-synthesizer:8083/synthesize", "kind": "snapshot", "common": "Global-Context-Synthesizer/Global-Context-Synthesizer-V1-workflow.json"},
]


def workflow(spec: dict) -> dict:
    strict_component = spec["kind"] == "component"
    strict_line = "" if not strict_component else "if (true && status !== 'OK') throw new Error(`GLOBAL_CONTEXT_COMPONENT_DEGRADED:${r.component || 'UNKNOWN'}:coverage=${r.coverage_ratio ?? 'NA'}:confidence=${r.confidence ?? 'NA'}`);\n"
    result_line = (
        "return [{json: {status, run_id: r.run_id || null, component_snapshot_id: r.component_snapshot_id || null, snapshot_id: hasSnapshot ? r.snapshot.snapshot_id : null, rows_written: rows, coverage_ratio: r.coverage_ratio ?? r.snapshot?.coverage_ratio ?? null, confidence: r.confidence ?? r.snapshot?.confidence ?? null, usable_row_ratio: r.usable_row_ratio ?? null}}];"
        if strict_component else
        "return [{json: {status, run_id: r.run_id || null, component_snapshot_id: r.component_snapshot_id || null, snapshot_id: hasSnapshot ? r.snapshot.snapshot_id : null, rows_written: rows, coverage_ratio: r.coverage_ratio ?? r.snapshot?.coverage_ratio ?? null}}];"
    )
    validate_code = """const r = $json || {};
const status = String(r.status || '').toUpperCase();
const rows = Number(r.rows_written || 0);
const hasSnapshot = Boolean(r.snapshot && r.snapshot.snapshot_id);
if (!['OK', 'DEGRADED'].includes(status)) throw new Error(`GLOBAL_CONTEXT_RUN_BAD_STATUS:${status || 'MISSING'}`);
if (!hasSnapshot && rows <= 0) throw new Error('GLOBAL_CONTEXT_ZERO_ROWS_OR_SNAPSHOT');
__STRICT_LINE____RESULT_LINE__
""".replace("__STRICT_LINE__", strict_line).replace("__RESULT_LINE__", result_line)
    nodes = [
        {"parameters": {"rule": {"interval": [{"field": "cronExpression", "expression": spec["cron"]}]}}, "type": "n8n-nodes-base.scheduleTrigger", "typeVersion": 1.3, "position": [-520, -100], "id": "schedule", "name": "Schedule Trigger"},
        {"parameters": {}, "type": "n8n-nodes-base.manualTrigger", "typeVersion": 1, "position": [-520, 100], "id": "manual", "name": "Manual Trigger"},
        {"parameters": {"method": "POST", "url": spec["url"], "sendBody": True, "specifyBody": "json", "jsonBody": "{}", "options": {"timeout": 1200000}}, "type": "n8n-nodes-base.httpRequest", "typeVersion": 4, "position": [-220, 0], "id": "invoke-service", "name": "Invoke Canonical Service", "retryOnFail": True, "maxTries": 3, "waitBetweenTries": 10000},
        {"parameters": {"jsCode": validate_code}, "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [60, 0], "id": "validate-contract", "name": "Validate Non-Empty Contract"},
    ]
    edge = [[{"node": "Invoke Canonical Service", "type": "main", "index": 0}]]
    return {"id": spec["id"], "name": spec["name"], "active": False, "nodes": nodes, "connections": {"Schedule Trigger": {"main": edge}, "Manual Trigger": {"main": edge}, "Invoke Canonical Service": {"main": [[{"node": "Validate Non-Empty Contract", "type": "main", "index": 0}]]}}, "settings": {"timezone": "Europe/Paris", "executionTimeout": 1200}}


def main() -> None:
    for spec in WORKFLOWS:
        payload = json.dumps(workflow(spec), ensure_ascii=False, indent=2) + "\n"
        outputs = [COMMON / spec["common"]]
        if spec.get("legacy"):
            outputs.append(ROOT / spec["legacy"])
        for output in outputs:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(payload, encoding="utf-8")
            print(output)


if __name__ == "__main__":
    main()
