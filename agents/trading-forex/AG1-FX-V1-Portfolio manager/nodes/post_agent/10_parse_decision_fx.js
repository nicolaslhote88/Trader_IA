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

if (!decision.decisions && j.dry_run) {
  decision = {
    as_of: j.as_of,
    narrative: 'dry-run hold payload',
    decisions: (j.brief?.universe?.pairs || []).map((pair) => ({ pair, decision: 'hold', conviction: 0.1 })),
  };
}

if (!Array.isArray(decision.decisions)) {
  decision.decisions = [];
}

const cleaned = decision.decisions.slice(0, 30).map((d) => ({
  pair: String(d.pair || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 6),
  decision: String(d.decision || 'hold'),
  conviction: Math.max(0, Math.min(1, Number(d.conviction || 0))),
  size_lots: d.size_lots === undefined ? undefined : Number(d.size_lots),
  size_pct_equity: d.size_pct_equity === undefined ? undefined : Number(d.size_pct_equity),
  stop_loss_price: d.stop_loss_price === undefined ? undefined : Number(d.stop_loss_price),
  take_profit_price: d.take_profit_price === undefined ? undefined : Number(d.take_profit_price),
  horizon: d.horizon || '1d',
  rationale: String(d.rationale || '').slice(0, 600),
  lot_id_to_close: d.lot_id_to_close || '',
}));

return [{ json: { ...j, decision_json: { ...decision, decisions: cleaned } } }];
