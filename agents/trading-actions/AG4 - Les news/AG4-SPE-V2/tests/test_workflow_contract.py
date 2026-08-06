import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_workflow():
    value = json.loads((ROOT / "AG4-SPE-V2-workflow.json").read_text(encoding="utf-8-sig"))
    return value[0] if isinstance(value, list) else value


def node(workflow, name):
    return next(item for item in workflow["nodes"] if item["name"] == name)


def sha256(value):
    return hashlib.sha256(value.encode()).hexdigest()


class WorkflowContractTests(unittest.TestCase):
    def test_deepseek_analysis_contract(self):
        workflow = load_workflow()
        chain_name = "S19 - Analyze with DeepSeek"
        model_name = "S19A - DeepSeek Chat Model"
        parser_name = "S19B - Structured Output DeepSeek"
        chain = node(workflow, chain_name)
        model = node(workflow, model_name)
        parser = node(workflow, parser_name)

        self.assertEqual("@n8n/n8n-nodes-langchain.chainLlm", chain["type"])
        self.assertEqual(1.5, chain["typeVersion"])
        self.assertTrue(chain["parameters"]["hasOutputParser"])
        self.assertEqual("continueRegularOutput", chain["onError"])
        self.assertEqual("@n8n/n8n-nodes-langchain.lmChatDeepSeek", model["type"])
        self.assertEqual("deepseek-v4-pro", model["parameters"]["model"])
        self.assertEqual("BlSCC28mzKodkfO5", model["credentials"]["deepSeekApi"]["id"])
        self.assertEqual("@n8n/n8n-nodes-langchain.outputParserStructured", parser["type"])

        system_prompt = chain["parameters"]["messages"]["messageValues"][0]["message"]
        user_prompt = chain["parameters"]["text"]
        schema = parser["parameters"]["inputSchema"]
        self.assertEqual("f836597a00b092b3f8f22bd4fa91f9499dd149a8efe6abe0b6107e9c8feaa137", sha256(system_prompt))
        self.assertEqual("90f0d4407a57c6a09ed24613b0b5a42322182e4216aa9dd4cb0c5220f9840d66", sha256(user_prompt))
        self.assertEqual("4829777c4d8e8ff2aea0ba8e4eb542394e58f628c053263db960f5ec4e466b73", sha256(schema))

        connections = workflow["connections"]
        self.assertEqual(chain_name, connections[model_name]["ai_languageModel"][0][0]["node"])
        self.assertEqual(chain_name, connections[parser_name]["ai_outputParser"][0][0]["node"])
        self.assertEqual("S19M - Merge AI + Context", connections[chain_name]["main"][0][0]["node"])
        self.assertEqual(1, connections[chain_name]["main"][0][0]["index"])
        if_targets = connections["S18 - IF Run AI?"]["main"][0]
        self.assertEqual({chain_name, "S19M - Merge AI + Context"}, {item["node"] for item in if_targets})

        serialized = json.dumps(workflow, ensure_ascii=False)
        self.assertNotIn("gpt-5-mini", serialized)
        self.assertFalse(any(item.get("type") == "@n8n/n8n-nodes-langchain.openAi" for item in workflow["nodes"]))
        self.assertFalse(any("openAiApi" in item.get("credentials", {}) for item in workflow["nodes"]))

    def test_parser_accepts_structured_and_legacy_outputs(self):
        code = node(load_workflow(), "S20 - Parse LLM Output")["parameters"]["jsCode"]
        self.assertIn("j.output && !Array.isArray(j.output)", code)
        self.assertIn("j.output?.[0]?.content?.[0]?.text", code)
        self.assertIn("j.content || j.text", code)

    def test_schedule_and_settings_are_unchanged(self):
        workflow = load_workflow()
        schedule = next(item for item in workflow["nodes"] if item["type"] == "n8n-nodes-base.scheduleTrigger")
        expression = schedule["parameters"]["rule"]["interval"][0]["expression"]
        self.assertEqual("0 5 8,11,14,17 * * 1-5", expression)
        self.assertEqual("none", workflow["settings"]["saveDataSuccessExecution"])

    def test_builder_is_idempotent(self):
        import build_workflow

        workflow = load_workflow()
        rebuilt = build_workflow.build(ROOT / "AG4-SPE-V2-workflow.json")
        self.assertEqual(workflow["nodes"], rebuilt["nodes"])
        self.assertEqual(workflow["connections"], rebuilt["connections"])
        self.assertEqual(workflow["versionId"], rebuilt["versionId"])


if __name__ == "__main__":
    unittest.main()
