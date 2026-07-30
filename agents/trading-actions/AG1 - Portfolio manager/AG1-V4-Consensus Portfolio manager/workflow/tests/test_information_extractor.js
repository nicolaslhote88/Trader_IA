#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const template = fs.readFileSync(
  path.join(root, "nodes", "agent_input", "information_extractor_v4.code.js"),
  "utf8"
)
  .replace("__MODEL_KEY__", "grok41_reasoning")
  .replace("__MODEL_NAME__", "DeepSeek V4 Pro")
  .replace("__MODEL_ID__", "deepseek-v4-pro");

function run(json) {
  return new Function("$json", template)(json)[0].json;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const decision = {
  marketRegime: "ROTATION",
  targetExposurePct: 70,
  maxNewPositions: 1,
  actions: [{
    symbol: "NVDA",
    action: "INCREASE",
    confidence: 74,
    targetWeightPct: 10,
    rationale: "Setup valide et liquidité fraîche.",
    nextReviewDays: 3,
  }],
  riskNotes: [],
  dataCaveats: [],
};

const structured = run({ output: decision });
assert(structured.extractorStatus === "OK_OBJECT", "structured object rejected");
assert(structured.modelId === "deepseek-v4-pro", "model metadata stale");

const fenced = run({ output: `\`\`\`json\n${JSON.stringify(decision)}\n\`\`\`` });
assert(fenced.extractorStatus === "OK_JSON", "fenced JSON rejected");
assert(fenced.output.actions[0].symbol === "NVDA", "fenced JSON changed");

const recovered = run({ output: `Réponse: ${JSON.stringify(decision)} fin.` });
assert(recovered.extractorStatus === "OK_RECOVERED_JSON", "balanced JSON recovery failed");

const upstream = run({ error: "OUTPUT_PARSING_FAILURE" });
assert(upstream.extractorStatus === "UPSTREAM_ERROR", "upstream error hidden");
assert(upstream.extractorError === "OUTPUT_PARSING_FAILURE", "upstream error lost");

const invalid = run({ output: { marketRegime: "ROTATION", riskNotes: [], dataCaveats: [] } });
assert(invalid.extractorStatus === "INVALID_SHAPE", "invalid decision marked OK");
assert(invalid.extractorError === "ACTIONS_NOT_ARRAY", "invalid shape reason missing");

console.log(JSON.stringify({ ok: true, cases: 5 }));
