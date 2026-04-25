const staticData = $getWorkflowStaticData("global");
const items = $input.all();

if (!staticData.ag4SpeNewsBuffers || typeof staticData.ag4SpeNewsBuffers !== "object") {
  staticData.ag4SpeNewsBuffers = {};
}

const runId = String(items[0]?.json?.run_id || "").trim();
if (runId) {
  staticData.ag4SpeNewsBuffers[runId] = [];
  staticData.ag4SpeCurrentRunId = runId;
}

return items;