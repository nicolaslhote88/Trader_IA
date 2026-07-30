import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_workflow():
    value = json.loads((ROOT / "AG1_workflow_v4_consensus.json").read_text(encoding="utf-8-sig"))
    return value[0] if isinstance(value, list) else value


def node(workflow, name):
    return next(item for item in workflow["nodes"] if item["name"] == name)


class ModelContractTests(unittest.TestCase):
    def test_live_model_nodes_are_mirrored(self):
        workflow = load_workflow()
        gpt = node(workflow, "OpenAI Chat Model - GPT5.6sol")
        deepseek = node(workflow, "DeepSeek Chat Model")
        claude = node(workflow, "Anthropic Chat Model")

        self.assertEqual("gpt-5.6-sol", gpt["parameters"]["model"]["value"])
        self.assertEqual("deepseek-v4-pro", deepseek["parameters"]["model"])
        self.assertEqual("claude-opus-4-8", claude["parameters"]["model"]["value"])

    def test_extractors_use_actual_models_and_stable_storage_keys(self):
        workflow = load_workflow()
        contracts = {
            "Information Extractor": ("chatgpt52", "OpenAI GPT-5.6 Sol", "gpt-5.6-sol"),
            "Information Extractor1": ("grok41_reasoning", "DeepSeek V4 Pro", "deepseek-v4-pro"),
            "Information Extractor2": ("claude_sonnet46", "Anthropic Claude Opus 4.8", "claude-opus-4-8"),
        }
        for name, expected in contracts.items():
            code = node(workflow, name)["parameters"]["jsCode"]
            self.assertIn(f'const MODEL_KEY = "{expected[0]}";', code)
            self.assertIn(f'const MODEL_NAME = "{expected[1]}";', code)
            self.assertIn(f'const MODEL_ID = "{expected[2]}";', code)
            self.assertIn('status: "UPSTREAM_ERROR"', code)
            self.assertIn('status: "INVALID_SHAPE"', code)

    def test_deepseek_uses_chain_parser_and_retry(self):
        workflow = load_workflow()
        proposal = node(workflow, "Agent #1 - Portfolio manager1")
        self.assertEqual("@n8n/n8n-nodes-langchain.chainLlm", proposal["type"])
        self.assertEqual(1.5, proposal["typeVersion"])
        self.assertTrue(proposal["parameters"]["hasOutputParser"])
        self.assertTrue(proposal["retryOnFail"])
        self.assertEqual(2, proposal["maxTries"])
        self.assertIn("CONTRAT DE SORTIE DEEPSEEK", proposal["parameters"]["messages"]["messageValues"][0]["message"])

        connections = workflow["connections"]
        self.assertEqual(
            "Agent #1 - Portfolio manager1",
            connections["DeepSeek Chat Model"]["ai_languageModel"][0][0]["node"],
        )
        self.assertEqual(
            "Agent #1 - Portfolio manager1",
            connections["AG1.V4 — Structured Output DeepSeek"]["ai_outputParser"][0][0]["node"],
        )
        self.assertNotIn("AG1.V4 — Structured Output Grok", connections)

    def test_other_branches_keep_agent_contract(self):
        workflow = load_workflow()
        self.assertEqual("@n8n/n8n-nodes-langchain.agent", node(workflow, "Agent #1 - Portfolio manager")["type"])
        self.assertEqual("@n8n/n8n-nodes-langchain.agent", node(workflow, "Agent #1 - Portfolio manager2")["type"])

    def test_builder_is_idempotent(self):
        import build_v4_workflow

        workflow = load_workflow()
        rebuilt = build_v4_workflow.build(ROOT / "AG1_workflow_v4_consensus.json")
        self.assertEqual(workflow["nodes"], rebuilt["nodes"])
        self.assertEqual(workflow["connections"], rebuilt["connections"])

    def test_template_build_uses_active_models(self):
        import build_v4_workflow

        rebuilt = build_v4_workflow.build(ROOT / "AG1_workflow_template_v4.json")
        self.assertEqual("gpt-5.6-sol", node(rebuilt, "OpenAI Chat Model - GPT5.6sol")["parameters"]["model"]["value"])
        self.assertEqual("deepseek-v4-pro", node(rebuilt, "DeepSeek Chat Model")["parameters"]["model"])
        self.assertEqual("claude-opus-4-8", node(rebuilt, "Anthropic Chat Model")["parameters"]["model"]["value"])
        self.assertNotIn("xAI Grok Chat Model", rebuilt["connections"])


if __name__ == "__main__":
    unittest.main()
