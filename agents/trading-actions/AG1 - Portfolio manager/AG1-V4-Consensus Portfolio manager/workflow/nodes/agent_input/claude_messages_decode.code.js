// SCHEMA is injected by the builder. Keep full bounds locally: Anthropic's
// constrained decoder accepts only a subset of the business JSON Schema.
function validate(value, schema, path = '$') {
  const types = Array.isArray(schema.type) ? schema.type : [schema.type];
  const matches = t => t === 'null' ? value === null
    : t === 'object' ? value !== null && typeof value === 'object' && !Array.isArray(value)
    : t === 'array' ? Array.isArray(value)
    : t === 'integer' ? Number.isInteger(value)
    : t === 'number' ? typeof value === 'number' && Number.isFinite(value)
    : typeof value === t;
  if (!types.some(matches)) throw new Error(`${path}: invalid type`);
  if (schema.enum && !schema.enum.includes(value)) throw new Error(`${path}: invalid enum`);
  if (value === null) return;
  if (typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) throw new Error(`${path}: below minimum`);
    if (schema.maximum !== undefined && value > schema.maximum) throw new Error(`${path}: above maximum`);
  }
  if (typeof value === 'string' && schema.maxLength !== undefined && [...value].length > schema.maxLength) throw new Error(`${path}: too long`);
  if (Array.isArray(value)) {
    if (schema.maxItems !== undefined && value.length > schema.maxItems) throw new Error(`${path}: too many items`);
    value.forEach((v, i) => validate(v, schema.items, `${path}[${i}]`));
  } else if (typeof value === 'object') {
    for (const key of schema.required || []) if (!Object.hasOwn(value, key)) throw new Error(`${path}.${key}: required`);
    for (const [key, v] of Object.entries(value)) {
      if (!schema.properties?.[key]) throw new Error(`${path}.${key}: unexpected`);
      validate(v, schema.properties[key], `${path}.${key}`);
    }
  }
}
const input = $json || {};
try {
  if (input.error) throw new Error('ANTHROPIC_UPSTREAM_ERROR');
  if (input.model !== 'claude-opus-5-5') throw new Error('ANTHROPIC_MODEL_MISMATCH');
  if (input.stop_reason !== 'end_turn') throw new Error(`ANTHROPIC_STOP_${input.stop_reason || 'MISSING'}`);
  if (!Array.isArray(input.content)) throw new Error('ANTHROPIC_CONTENT_MISSING');
  const text = input.content.filter(b => b.type === 'text').map(b => b.text).join('');
  const output = JSON.parse(text);
  validate(output, SCHEMA);
  return [{json: {output, providerModel: input.model, usage: input.usage, stopReason: input.stop_reason}}];
} catch (error) {
  return [{json: {error: String(error.message || error), providerModel: input.model || null, usage: input.usage || null}}];
}
