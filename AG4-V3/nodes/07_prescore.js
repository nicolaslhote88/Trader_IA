// 20G1C - Pre-score heuristic impact (V2)
const HIGH = [
  'inflation', 'cpi', 'ppi', 'ecb', 'fed', 'taux', 'rate', 'yield',
  'recession', 'gdp', 'pmi', 'chomage', 'unemployment', 'war', 'tariff',
  'oil', 'opec', 'sanction', 'guidance', 'profit warning', 'default',
  'downgrade', 'bankruptcy', 'acquisition', 'merger', 'earnings'
];

const MID = ['forecast', 'outlook', 'results', 'macro', 'sector', 'dividend', 'buyback'];

function scoreText(text) {
  const t = String(text || '').toLowerCase();
  let s = 0;
  for (const k of HIGH) if (t.includes(k)) s += 2;
  for (const k of MID) if (t.includes(k)) s += 1;
  return s;
}

return $input.all().map((i) => {
  const j = i.json || {};
  const text = `${j.title || ''} ${j.snippet || ''}`;
  const kw = scoreText(text);
  const hasSectorHint = Array.isArray(j.candidateSectors) && j.candidateSectors.length > 0;

  let score = Math.min(10, kw + (hasSectorHint ? 2 : 1));
  score += Number(j.sourceTier === 1 ? 2 : (j.sourceTier === 2 ? 1 : 0));
  score = Math.min(10, score);

  const urgency = score >= 8 ? 'immediate' : score >= 6 ? 'today' : score >= 4 ? 'this_week' : 'low';

  return {
    json: {
      ...j,
      preImpactScore: score,
      preUrgency: urgency,
      preAnalyzeHint: score >= 4,
    },
    pairedItem: i.pairedItem,
  };
});
