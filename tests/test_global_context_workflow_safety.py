from __future__ import annotations

import json
from pathlib import Path


ROOT = Path("agents/common/global-context")


def _workflows():
    return [json.loads(path.read_text(encoding="utf-8")) for path in ROOT.rglob("*.json")]


def test_collection_workflows_are_inactive_thin_and_have_no_trading_transport():
    workflows = _workflows()
    assert len(workflows) == 6
    for workflow in workflows:
        assert workflow["active"] is False
        types = {node["type"] for node in workflow["nodes"]}
        assert types <= {
            "n8n-nodes-base.scheduleTrigger",
            "n8n-nodes-base.manualTrigger",
            "n8n-nodes-base.httpRequest",
            "n8n-nodes-base.code",
        }
        text = json.dumps(workflow, ensure_ascii=False).lower()
        assert "ibkr" not in text
        assert "/orders" not in text
        assert "telegram" not in text


def test_forex_execution_is_not_reintroduced_by_global_context_workflows():
    text = "\n".join(json.dumps(row, ensure_ascii=False).lower() for row in _workflows())
    assert "fx_orders_enabled" not in text
    assert "place_order" not in text
    assert "send_orders" not in text
