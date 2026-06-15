// AG1 V4 - Information Extractor with model tagging.
// MODEL_KEY and MODEL_NAME are replaced by build_v4_workflow.py for each branch.

const MODEL_KEY = "__MODEL_KEY__";
const MODEL_NAME = "__MODEL_NAME__";

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }

function stripCodeFence(raw) {
  let s = String(raw ?? "").trim();
  const fenced = s.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  if (fenced) return fenced[1].trim();
  if (s.includes("```")) {
    const parts = s.split("```");
    if (parts.length >= 3) {
      s = parts[1].trim();
      if (s.toLowerCase().startsWith("json")) s = s.slice(4).trim();
    }
  }
  return s.trim();
}

function safeParseJson(text) {
  try { return JSON.parse(text); } catch { return null; }
}

function parseAgentOutput(value) {
  if (Array.isArray(value)) return { output: value, status: "OK_ARRAY" };
  if (isObj(value)) return { output: value, status: "OK_OBJECT" };

  const raw = String(value ?? "").trim();
  const cleaned = stripCodeFence(raw);
  let parsed = safeParseJson(cleaned);
  if (parsed !== null) return { output: parsed, status: "OK_JSON" };

  const firstArray = cleaned.indexOf("[");
  const lastArray = cleaned.lastIndexOf("]");
  if (firstArray !== -1 && lastArray > firstArray) {
    parsed = safeParseJson(cleaned.slice(firstArray, lastArray + 1));
    if (parsed !== null) return { output: parsed, status: "OK_EXTRACTED_ARRAY" };
  }

  const firstObj = cleaned.indexOf("{");
  const lastObj = cleaned.lastIndexOf("}");
  if (firstObj !== -1 && lastObj > firstObj) {
    parsed = safeParseJson(cleaned.slice(firstObj, lastObj + 1));
    if (parsed !== null) return { output: parsed, status: "OK_EXTRACTED_OBJECT" };
  }

  return { output: raw, status: "UNPARSED_TEXT" };
}

const input = $json ?? {};
const parsed = parseAgentOutput(input.output ?? input.text ?? input);

return [{
  json: {
    ...input,
    output: parsed.output,
    extractorStatus: parsed.status,
    modelKey: MODEL_KEY,
    modelName: MODEL_NAME,
  },
}];
