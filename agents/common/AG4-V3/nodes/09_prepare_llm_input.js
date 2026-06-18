// 20H0 - Prepare LLM input (V3 - dual-branch full/reduced)
function stripHtml(s) {
  if (!s) return '';
  return String(s).replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
}

function toIso(x) {
  if (!x || x === 'unknown') return 'unknown';
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? 'unknown' : d.toISOString();
}

function toArray(v) {
  if (Array.isArray(v)) return v;
  if (typeof v === 'string') {
    try {
      const parsed = JSON.parse(v);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

// ---- Mode d'analyse (full = Forex+Actions / reduced = Actions seules) ----
// Source de verite : node Set "20CFG - Analysis Mode" (un seul champ a basculer
// dans l'UI n8n). Defaut prudent = reduced (Forex desactive au 2026-06-17).
let analysisMode = 'reduced';
try {
  const cfg = $('20CFG - Analysis Mode').first().json.analysisMode;
  if (typeof cfg === 'string' && cfg.trim()) analysisMode = cfg.trim().toLowerCase();
} catch {
  analysisMode = 'reduced';
}
if (analysisMode !== 'full' && analysisMode !== 'reduced') analysisMode = 'reduced';

// ---- Config branche reduite (Grok / xAI) ----
const GROK_MODEL = 'grok-4.3';
const GROK_REASONING_EFFORT = 'low'; // non-raisonnant : rapide + bon marche pour une extraction
const REDUCED_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    isActionable: { type: 'boolean' },
    market_regime: { type: 'string', enum: ['Risk-On', 'Risk-Off', 'Neutral', 'Sector Rotation'] },
    macro_theme: { type: 'string', enum: ['Inflation/Taux', 'Banques Centrales', 'Croissance/Recession', 'Geopolitique/Energie', 'Tech/AI', 'Resultats/Micro'] },
    sectors_bullish: { type: 'array', maxItems: 5, items: { type: 'string' } },
    sectors_bearish: { type: 'array', maxItems: 5, items: { type: 'string' } },
    impact_magnitude: { type: 'string', enum: ['Low', 'Medium', 'High'] },
    impact_score: { type: 'integer', minimum: 0, maximum: 10 },
    confidence: { type: 'number', minimum: 0, maximum: 1 },
    notes: { type: 'string' },
  },
  required: ['isActionable', 'market_regime', 'macro_theme', 'sectors_bullish', 'sectors_bearish', 'impact_magnitude', 'impact_score', 'confidence', 'notes'],
};

const REDUCED_SYSTEM = [
  'Tu es analyste marche ACTIONS. Tu analyses UNE news pour la gestion d un portefeuille d actions/ETF.',
  'Extrais uniquement : secteurs gagnants/perdants (dans l univers fourni), un score d impact CALIBRE, la magnitude, le theme macro et le regime.',
  'Reponds UNIQUEMENT en JSON valide selon le schema impose.',
  '',
  'CALIBRATION impact_score (0-10) - tres important :',
  '- La TRES GRANDE MAJORITE des news sont 1-4.',
  '- 8-10 : RESERVE aux chocs systemiques (decision/surprise de banque centrale, defaut souverain, crise geopolitique/energetique majeure, surprise macro > 2 sigma qui change la tendance).',
  '- 5-7 : evenement directionnel net pour un secteur (resultats majeurs, M&A significatif, guidance forte).',
  '- 1-3 : micro / anecdotique / interview / point marche de routine / actu produit.',
  '- 0 : aucun impact marche notable -> isActionable=false, notes=Noise.',
  '',
  'COHERENCE OBLIGATOIRE :',
  '- impact_magnitude=Low => impact_score entre 0 et 3 ; Medium => 4 a 6 ; High => 7 a 10.',
  '- Si confidence < 0.5, n attribue PAS un impact_score > 6.',
].join('\n');

function buildReducedUser(payloadStr) {
  return [
    'Analyse cette news:',
    payloadStr,
    '',
    'Regles de normalisation obligatoires:',
    '- Secteurs : utilise UNIQUEMENT les valeurs du champ universeSectors du JSON. N utilise jamais des industries. Si un secteur propose ne matche pas, ne le retourne pas.',
    '- Listes secteurs vides si la news ne concerne pas l univers actions. Max 5 elements par liste.',
    '- Si pas de lien macro/secteur clair -> isActionable=false, impact_score=0, impact_magnitude=Low, market_regime=Neutral, macro_theme=Resultats/Micro, notes=Noise.',
    '- Sinon, isActionable=true.',
  ].join('\n');
}

let sectorDictionary = [];
try {
  const raw = $items('20A2 - Build Sector Dictionary')[0]?.json?.sectorDictionary;
  sectorDictionary = toArray(raw).map((x) => String(x || '').trim()).filter(Boolean);
} catch {
  sectorDictionary = [];
}

const j = $json || {};
const candidateSectors = toArray(j.candidateSectors).map((x) => String(x || '').trim()).filter(Boolean).slice(0, 5);

const payload = {
  id: j.id || 'unknown',
  dedupeKey: j.dedupeKey,
  eventKey: j.eventKey,
  title: stripHtml(j.title || 'unknown'),
  snippet: stripHtml(j.snippet || ''),
  url: j.canonicalUrlNormalized || j.canonicalUrl || j.url || 'unknown',
  publishedAt: toIso(j.publishedAtNormalized || j.publishedAt || 'unknown'),
  source: j.source || 'unknown',
  type: 'macro',
  preImpactScore: j.preImpactScore ?? 0,
  preUrgency: j.preUrgency || 'low',
  candidateSectors,
  universeSectors: sectorDictionary,
};

const payloadStr = JSON.stringify(payload);

// Corps de requete pret a l emploi pour le node HTTP Grok (branche reduite).
const grokRequest = {
  model: GROK_MODEL,
  reasoning_effort: GROK_REASONING_EFFORT,
  temperature: 0,
  messages: [
    { role: 'system', content: REDUCED_SYSTEM },
    { role: 'user', content: buildReducedUser(payloadStr) },
  ],
  response_format: {
    type: 'json_schema',
    json_schema: {
      name: 'market_news_normalizer_reduced_v1',
      strict: true,
      schema: REDUCED_SCHEMA,
    },
  },
};

return {
  ...j,
  symbols: [],
  type: 'macro',
  universeSectors: sectorDictionary,
  candidateSectors,
  analysisMode,
  llmInput: payloadStr,
  grokRequest,
};
