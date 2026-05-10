/**
 * Node 10 — Parse LLM decision for AG1-FX-V2 (3-Pillar framework)
 *
 * V2 LLM outputs { actions: [...], market_view, usd_thesis }
 * Each action: { intent, pair, side, size_lots, rationale: { conviction, ... } }
 */

function safeParse(v) {
  if (!v) return {};
  if (typeof v === 'object') return v;
  let s = String(v).trim();
  const fenced = s.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  if (fenced) s = fenced[1].trim();
  try { return JSON.parse(s); } catch { return {}; }
}

const j = $json || {};
let raw = j.agent_output || j.output || j.text || j.response || j.decision_json;
if (Array.isArray(raw)) raw = raw[0];
if (raw && raw.content) raw = raw.content;
if (raw && raw.text) raw = raw.text;
if (raw && raw.output) raw = raw.output;
let decision = safeParse(raw);

// Dry-run fallback: hold all pairs
if (!decision.actions && j.dry_run) {
  decision = {
    as_of: j.as_of,
    market_view: 'dry-run hold payload',
    usd_thesis: 'N/A',
    actions: [],
  };
}

if (!Array.isArray(decision.actions)) {
  decision.actions = [];
}

const VALID_INTENTS = new Set(['OPEN', 'INCREASE', 'DECREASE', 'CLOSE']);
const VALID_SIDES = new Set(['buy_base', 'sell_base', 'close']);

const cleaned = decision.actions.slice(0, 30).map((a) => {
  const rationale = a.rationale || {};
  return {
    intent: String(a.intent || 'OPEN').toUpperCase(),
    pair: String(a.pair || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 6),
    side: String(a.side || 'buy_base').toLowerCase(),
    size_lots: Math.max(0, Number(a.size_lots || 0)),
    rationale: {
      thesis: String(rationale.thesis || '').slice(0, 400),
      pillar_1_macro: String(rationale.pillar_1_macro || '').slice(0, 300),
      pillar_2_valuation: String(rationale.pillar_2_valuation || '').slice(0, 300),
      pillar_3_positioning: String(rationale.pillar_3_positioning || '').slice(0, 300),
      exit_conditions: String(rationale.exit_conditions || '').slice(0, 300),
      conviction: Math.max(0, Math.min(1, Number(rationale.conviction || 0))),
    },
  };
}).filter((a) => VALID_INTENTS.has(a.intent) && a.pair.length === 6 && VALID_SIDES.has(a.side));

return [{
  json: {
    ...j,
    decision: {
      as_of: j.as_of,
      market_view: String(decision.market_view || '').slice(0, 600),
      usd_thesis: String(decision.usd_thesis || '').slice(0, 400),
      actions: cleaned,
    },
  },
}];
