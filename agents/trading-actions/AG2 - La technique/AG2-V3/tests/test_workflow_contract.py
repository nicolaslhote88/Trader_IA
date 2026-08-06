import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load(name):
    value = json.loads((ROOT / name).read_text(encoding="utf-8-sig"))
    return value[0] if isinstance(value, list) else value


def node(workflow, name):
    return next(item for item in workflow["nodes"] if item["name"] == name)


class WorkflowContractTests(unittest.TestCase):
    def assert_variant(self, filename, cron, mode, size, state_key):
        workflow = load(filename)
        trigger = next(item for item in workflow["nodes"] if item["type"] == "n8n-nodes-base.scheduleTrigger")
        self.assertEqual(cron, trigger["parameters"]["rule"]["interval"][0]["expression"])
        init = node(workflow, "Init Config + Batch")["parameters"]["jsCode"]
        self.assertIn(f'const DEFAULT_ROTATION_MODE = "{mode}";', init)
        self.assertIn(f"const DEFAULT_BATCH_SIZE = {size};", init)
        self.assertIn(f'const DEFAULT_BATCH_STATE_KEY = "{state_key}";', init)
        for fetch_name in (
            "AG2.10 - HTTP - Fetch Yahoo OHLCV (1H Timing)",
            "AG2.15 - HTTP - Fetch Yahoo OHLCV (1D Strategy)",
        ):
            params = node(workflow, fetch_name)["parameters"]["queryParameters"]["parameters"]
            values = {item["name"]: item["value"] for item in params}
            self.assertEqual("true", values["closed_only"])
            self.assertEqual("true", values["validated_only"])
        finalize = node(workflow, "Finalize Run")["parameters"]["pythonCode"]
        self.assertIn("AG2_RUN_FAILED", finalize)
        self.assertIn("AG2_CURSOR_GUARD_FAILED", finalize)
        self.assertIn('"cursor_advanced": cursor_advanced', finalize)
        compute_code = node(workflow, "Compute + Filter + Write")["parameters"]["pythonCode"]
        self.assertIn('out["batch_info"] = batch_info', compute_code)
        self.assertIn('"batch_info": batch_info', compute_code)
        merge_code = node(workflow, "Merge AI + Context")["parameters"]["jsCode"]
        self.assertIn("raw?.output && !Array.isArray(raw.output)", merge_code)
        extract_code = node(workflow, "Extract AI + Write")["parameters"]["pythonCode"]
        self.assertIn('get_nested(d, ["ai_validation", "output"])', extract_code)

    def test_held_core_contract(self):
        filename = "AG2-V3-Technical-Held-Core.workflow.json"
        self.assert_variant(
            filename,
            "0 9,13,15 * * 1-5", "HELD_CORE", 18,
            "last_index_actions_held_core",
        )
        workflow = load(filename)
        chain = node(workflow, "AI Validation DeepSeek - ACTIONS/ETF")
        model = node(workflow, "DeepSeek Chat Model")
        parser = node(workflow, "AG2 — Structured Output DeepSeek")
        self.assertEqual("@n8n/n8n-nodes-langchain.chainLlm", chain["type"])
        self.assertTrue(chain["parameters"]["hasOutputParser"])
        self.assertIn("ZERO HALLUCINATION", chain["parameters"]["messages"]["messageValues"][0]["message"])
        self.assertIn("JSON.stringify($json.ai_context", chain["parameters"]["text"])
        self.assertEqual("@n8n/n8n-nodes-langchain.lmChatDeepSeek", model["type"])
        self.assertEqual("deepseek-v4-pro", model["parameters"]["model"])
        self.assertEqual("BlSCC28mzKodkfO5", model["credentials"]["deepSeekApi"]["id"])
        self.assertEqual("@n8n/n8n-nodes-langchain.outputParserStructured", parser["type"])
        self.assertEqual([-16, 7312], model["position"])
        self.assertEqual([160, 7312], parser["position"])
        self.assertIn('"decision"', parser["parameters"]["inputSchema"])
        self.assertEqual(
            "AI Validation DeepSeek - ACTIONS/ETF",
            workflow["connections"]["Snapshot Context"]["main"][0][0]["node"],
        )
        self.assertEqual(
            "AI Validation DeepSeek - ACTIONS/ETF",
            workflow["connections"]["DeepSeek Chat Model"]["ai_languageModel"][0][0]["node"],
        )
        self.assertEqual(
            "AI Validation DeepSeek - ACTIONS/ETF",
            workflow["connections"]["AG2 — Structured Output DeepSeek"]["ai_outputParser"][0][0]["node"],
        )
        self.assertIn(
            "model=deepseek-v4-pro",
            node(workflow, "Compute + Filter + Write")["parameters"]["pythonCode"],
        )
        self.assertIn(
            '"ai_model": "deepseek-v4-pro"',
            node(workflow, "Extract AI + Write")["parameters"]["pythonCode"],
        )
        self.assertIn(
            'ai_data["ai_model"] = "deepseek-v4-pro"',
            node(workflow, "Hydrate AI from cache")["parameters"]["pythonCode"],
        )
        self.assertNotIn("gpt-5-mini", json.dumps(workflow, ensure_ascii=False))

    def test_watchlist_contract(self):
        filename = "AG2-V3-Technical-Watchlist-Nightly.workflow.json"
        self.assert_variant(
            filename,
            "0 22,2 * * *", "WATCHLIST", 40,
            "last_index_actions_watchlist",
        )
        workflow = load(filename)
        chain = node(workflow, "AI Validation DeepSeek - ACTIONS/ETF")
        model = node(workflow, "DeepSeek Chat Model")
        parser = node(workflow, "AG2 — Structured Output DeepSeek")
        self.assertEqual("@n8n/n8n-nodes-langchain.chainLlm", chain["type"])
        self.assertTrue(chain["parameters"]["hasOutputParser"])
        self.assertEqual("deepseek-v4-pro", model["parameters"]["model"])
        self.assertEqual("BlSCC28mzKodkfO5", model["credentials"]["deepSeekApi"]["id"])
        self.assertEqual("@n8n/n8n-nodes-langchain.outputParserStructured", parser["type"])
        self.assertEqual([-16, 7728], model["position"])
        self.assertEqual([128, 7728], parser["position"])
        self.assertEqual(
            "AI Validation DeepSeek - ACTIONS/ETF",
            workflow["connections"]["Snapshot Context"]["main"][0][0]["node"],
        )
        self.assertIn(
            "model=deepseek-v4-pro",
            node(workflow, "Compute + Filter + Write")["parameters"]["pythonCode"],
        )
        self.assertIn(
            '"ai_model": "deepseek-v4-pro"',
            node(workflow, "Extract AI + Write")["parameters"]["pythonCode"],
        )
        self.assertNotIn("gpt-5-mini", json.dumps(workflow, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
