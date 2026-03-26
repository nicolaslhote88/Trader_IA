// AG2-V3 - Merge AI output with original Snapshot context
const snapshot = $('Snapshot Context').item.json;
const raw = $json;

// n8n OpenAI node often returns: { output: [ { content: [ { type:'output_text', text: <obj|string> } ] } ] }
let extracted = null;

const contentArr = raw?.output?.[0]?.content;
if (Array.isArray(contentArr)) {
  const ot = contentArr.find(x => x?.type === 'output_text');
  extracted = ot?.text ?? null;
}

// If extracted is a JSON string, parse it
if (typeof extracted === 'string') {
  try { extracted = JSON.parse(extracted); } catch (e) {}
}

// Fallbacks
if (!extracted && raw?.text) extracted = raw.text;
if (!extracted) extracted = raw;

return [{
  json: {
    ...snapshot,
    ai_validation: extracted,
    ai_raw: raw,
  }
}];
