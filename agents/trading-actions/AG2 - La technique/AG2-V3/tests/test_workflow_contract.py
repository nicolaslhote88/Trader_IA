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

    def test_held_core_contract(self):
        self.assert_variant(
            "AG2-V3-Technical-Held-Core.workflow.json",
            "0 9,13,15 * * 1-5", "HELD_CORE", 18,
            "last_index_actions_held_core",
        )

    def test_watchlist_contract(self):
        self.assert_variant(
            "AG2-V3-Technical-Watchlist-Nightly.workflow.json",
            "0 22,2 * * *", "WATCHLIST", 40,
            "last_index_actions_watchlist",
        )


if __name__ == "__main__":
    unittest.main()
