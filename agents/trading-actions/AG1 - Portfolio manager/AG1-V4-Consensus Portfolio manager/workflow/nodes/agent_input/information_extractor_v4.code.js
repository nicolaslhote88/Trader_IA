// AG1 V4 - Information Extractor with model tagging.
// Placeholders are replaced by build_v4_workflow.py for each branch.

const MODEL_KEY = "__MODEL_KEY__";
const MODEL_NAME = "__MODEL_NAME__";
const MODEL_ID = "__MODEL_ID__";

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

function firstBalancedObject(raw) {
  const s = String(raw ?? "");
  let start = -1;
  let depth = 0;
  let quoted = false;
  let escaped = false;
  for (let i = 0; i < s.length; i += 1) {
    const ch = s[i];
    if (quoted) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === '"') quoted = false;
      continue;
    }
    if (ch === '"') {
      quoted = true;
      continue;
    }
    if (ch === "{") {
      if (start < 0) start = i;
      depth += 1;
    } else if (ch === "}" && start >= 0) {
      depth -= 1;
      if (depth === 0) return s.slice(start, i + 1);
    }
  }
  return "";
}

function toFiniteOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeDecision(value) {
  let candidate = value;
  if (isObj(candidate?.output)) candidate = candidate.output;
  if (!isObj(candidate)) return { output: candidate, error: "ROOT_NOT_OBJECT" };

  const marketRegime = String(candidate.marketRegime ?? "").trim().toUpperCase();
  if (!["RISK_ON", "RISK_OFF", "ROTATION", "NEUTRAL"].includes(marketRegime)) {
    return { output: candidate, error: "INVALID_MARKET_REGIME" };
  }
  if (!Array.isArray(candidate.actions)) return { output: candidate, error: "ACTIONS_NOT_ARRAY" };
  if (!Array.isArray(candidate.riskNotes)) return { output: candidate, error: "RISK_NOTES_NOT_ARRAY" };
  if (!Array.isArray(candidate.dataCaveats)) return { output: candidate, error: "DATA_CAVEATS_NOT_ARRAY" };

  const targetExposurePct = toFiniteOrNull(candidate.targetExposurePct);
  if (targetExposurePct !== null && (targetExposurePct < 0 || targetExposurePct > 100)) {
    return { output: candidate, error: "INVALID_TARGET_EXPOSURE" };
  }
  const maxNewPositionsRaw = toFiniteOrNull(candidate.maxNewPositions);
  const maxNewPositions = maxNewPositionsRaw === null ? null : Math.trunc(maxNewPositionsRaw);
  if (maxNewPositions !== null && (maxNewPositions < 0 || maxNewPositions > 15)) {
    return { output: candidate, error: "INVALID_MAX_NEW_POSITIONS" };
  }

  const allowedActions = new Set(["OPEN", "INCREASE", "DECREASE", "CLOSE", "HOLD", "WATCH"]);
  const allowedReviewDays = new Set([1, 3, 5, 7]);
  const actions = [];
  for (let index = 0; index < candidate.actions.length; index += 1) {
    const action = candidate.actions[index];
    if (!isObj(action)) return { output: candidate, error: `ACTION_${index}_NOT_OBJECT` };
    const symbol = String(action.symbol ?? "").trim();
    const actionName = String(action.action ?? "").trim().toUpperCase();
    const confidence = toFiniteOrNull(action.confidence);
    const targetWeightPct = toFiniteOrNull(action.targetWeightPct);
    const rationale = String(action.rationale ?? "").trim();
    const nextReviewDays = Number(action.nextReviewDays);
    if (!symbol) return { output: candidate, error: `ACTION_${index}_SYMBOL_MISSING` };
    if (!allowedActions.has(actionName)) return { output: candidate, error: `ACTION_${index}_INVALID_ACTION` };
    if (confidence !== null && (confidence < 0 || confidence > 100)) {
      return { output: candidate, error: `ACTION_${index}_INVALID_CONFIDENCE` };
    }
    if (targetWeightPct !== null && (targetWeightPct < 0 || targetWeightPct > 100)) {
      return { output: candidate, error: `ACTION_${index}_INVALID_WEIGHT` };
    }
    if (!rationale) return { output: candidate, error: `ACTION_${index}_RATIONALE_MISSING` };
    if (!allowedReviewDays.has(nextReviewDays)) {
      return { output: candidate, error: `ACTION_${index}_INVALID_REVIEW_DAYS` };
    }
    actions.push({
      symbol,
      action: actionName,
      confidence: confidence === null ? null : Math.round(confidence),
      targetWeightPct,
      rationale: rationale.slice(0, 800),
      nextReviewDays,
    });
  }

  return {
    output: {
      marketRegime,
      targetExposurePct,
      maxNewPositions,
      actions: actions.slice(0, 15),
      riskNotes: candidate.riskNotes.map((item) => String(item ?? "").trim().slice(0, 400)).filter(Boolean).slice(0, 10),
      dataCaveats: candidate.dataCaveats.map((item) => String(item ?? "").trim().slice(0, 400)).filter(Boolean).slice(0, 10),
    },
    error: null,
  };
}

function parseAgentOutput(value) {
  if (isObj(value)) {
    const normalized = normalizeDecision(value);
    return normalized.error
      ? { output: normalized.output, status: "INVALID_SHAPE", error: normalized.error }
      : { output: normalized.output, status: "OK_OBJECT", error: null };
  }

  const raw = String(value ?? "").trim();
  const cleaned = stripCodeFence(raw);
  let parsed = safeParseJson(cleaned);
  let status = "OK_JSON";
  if (parsed === null) {
    const recovered = firstBalancedObject(cleaned);
    parsed = recovered ? safeParseJson(recovered) : null;
    status = "OK_RECOVERED_JSON";
  }
  if (parsed !== null) {
    const normalized = normalizeDecision(parsed);
    return normalized.error
      ? { output: normalized.output, status: "INVALID_SHAPE", error: normalized.error }
      : { output: normalized.output, status, error: null };
  }

  return { output: raw, status: "UNPARSED_TEXT", error: "JSON_PARSE_FAILED" };
}

const input = $json ?? {};
const upstreamError = input.error && input.output === undefined && input.text === undefined
  ? String(input.error)
  : "";
const parsed = upstreamError
  ? { output: { error: upstreamError }, status: "UPSTREAM_ERROR", error: upstreamError }
  : parseAgentOutput(input.output ?? input.text ?? input);

return [{
  json: {
    ...input,
    output: parsed.output,
    extractorStatus: parsed.status,
    extractorError: parsed.error || null,
    modelKey: MODEL_KEY,
    modelName: MODEL_NAME,
    modelId: MODEL_ID,
  },
}];
