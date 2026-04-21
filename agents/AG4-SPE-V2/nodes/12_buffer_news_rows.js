const staticData = $getWorkflowStaticData("global");
const items = $input.all();

if (!staticData.ag4SpeNewsBuffers || typeof staticData.ag4SpeNewsBuffers !== "object") {
  staticData.ag4SpeNewsBuffers = {};
}

for (const item of items) {
  const j = item.json || {};
  const runId = String(j.run_id || staticData.ag4SpeCurrentRunId || "").trim();
  if (!runId) continue;

  if (!Array.isArray(staticData.ag4SpeNewsBuffers[runId])) {
    staticData.ag4SpeNewsBuffers[runId] = [];
  }

  staticData.ag4SpeNewsBuffers[runId].push(JSON.parse(JSON.stringify(j)));
  staticData.ag4SpeCurrentRunId = runId;
}

return items;