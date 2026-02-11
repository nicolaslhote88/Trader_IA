// AG2-V2 — Merge AI output with original Snapshot context
// The OpenAI node may replace input data with only its response.
// This node re-attaches the original context for Extract AI.
const snapshot = $('Snapshot Context').item.json;
const aiOutput = $json;

return [{
  json: {
    ...snapshot,
    ai_raw: aiOutput,
  }
}];
