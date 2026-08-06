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


def test_deployed_workflows_do_not_depend_on_n8n_env_expression_access():
    for workflow in _workflows():
        if workflow["id"] == "AG9GLOBALRISK20260805":
            continue
        http_nodes = [node for node in workflow["nodes"] if node["type"] == "n8n-nodes-base.httpRequest"]
        assert len(http_nodes) == 1
        assert "$env" not in http_nodes[0]["parameters"]["url"]


def test_ag5_to_ag8_turn_degraded_quality_into_visible_n8n_failures():
    component_ids = {
        "AG5FXMacroPillarsV1", "AG6FXValuationPillarsV1",
        "AG7FXPositioningPillarsV1", "AG8FXRatesPillarsV1",
    }
    for workflow in _workflows():
        code = next(node for node in workflow["nodes"] if node["type"] == "n8n-nodes-base.code")["parameters"]["jsCode"]
        if workflow["id"] in component_ids:
            assert "GLOBAL_CONTEXT_COMPONENT_DEGRADED" in code
            assert "if (true && status !== 'OK')" in code
        else:
            assert "GLOBAL_CONTEXT_COMPONENT_DEGRADED" not in code
