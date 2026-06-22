import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CODE = (ROOT / "nodes" / "12_universe_quarantine_audit.py").read_text(encoding="utf-8")
OUT = ROOT / "AG2-Universe-Health-Quarantine.workflow.json"

workflow = {
    "name": "AG2 — Universe Health Quarantine",
    "nodes": [
        {
            "parameters": {
                "rule": {
                    "interval": [
                        {
                            "field": "cronExpression",
                            "expression": "35 18 * * 1-5",
                        }
                    ]
                }
            },
            "id": "7f3e9240-6d0d-4c8b-ae72-c59d2a5ab901",
            "name": "Schedule — Weekdays 18:35 UTC",
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position": [240, 220],
        },
        {
            "parameters": {},
            "id": "3f7fc482-bd65-4f8b-9a56-67c894e66c7f",
            "name": "Manual Trigger",
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [240, 400],
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": CODE},
            "id": "1bde0dd1-a6ce-4ec5-8d23-d74d7c5d29c7",
            "name": "Audit + Quarantine",
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [520, 320],
        },
    ],
    "connections": {
        "Schedule — Weekdays 18:35 UTC": {
            "main": [[{"node": "Audit + Quarantine", "type": "main", "index": 0}]]
        },
        "Manual Trigger": {
            "main": [[{"node": "Audit + Quarantine", "type": "main", "index": 0}]]
        },
    },
    "active": True,
    "settings": {"executionOrder": "v1", "timezone": "Europe/Paris"},
    "versionId": "20260619-universe-health-quarantine-v1",
    "id": "AG2UHQ20260619",
    "meta": {"templateCredsSetupCompleted": True},
    "tags": [],
}

OUT.write_text(json.dumps(workflow, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(OUT)
