const staticData = $getWorkflowStaticData("global");
const items = $input.all();

if (!staticData.ag4SpeNewsBuffers || typeof staticData.ag4SpeNewsBuffers !== "object") {
  staticData.ag4SpeNewsBuffers = {};
}

let runId = "";
for (const item of items) {
  const candidate = String(item.json?.run_id || "").trim();
  if (candidate) {
    runId = candidate;
    break;
  }
}
if (!runId) {
  runId = String(staticData.ag4SpeCurrentRunId || "").trim();
}

let rows = [];
if (runId && Array.isArray(staticData.ag4SpeNewsBuffers[runId])) {
  rows = staticData.ag4SpeNewsBuffers[runId];
  staticData.ag4SpeNewsBuffers[runId] = [];
}

const out = {
  ...(items[0]?.json || {}),
  run_id: runId || items[0]?.json?.run_id || "",
  _flushRows: rows.map((row) => JSON.parse(JSON.stringify(row))),
  _flushCount: rows.length,
};

if (!out.db_path && rows.length) {
  out.db_path = rows[0]?.db_path || "";
}

return [{ json: out }];
