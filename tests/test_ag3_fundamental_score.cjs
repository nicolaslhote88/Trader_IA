const fs = require('fs');
const assert = require('assert/strict');
const code = fs.readFileSync('agents/trading-actions/AG3 - Les fondamentaux/AG3-V2/nodes/02_score_fundamentals.js', 'utf8');
const score = (j) => new Function('$input', 'require', code)({all: () => [{json: {Symbol:'TEST', run_id:'TEST', ok:true, ...j}}]}, require)[0].json;
const missing = score({valuation:{trailingPE:null, forwardPE:null}, consensus:{targetMeanPrice:null, targetLowPrice:null, targetHighPrice:null}});
assert.equal(missing.triageRow.valuation_score, 50);
assert.equal(missing.triageRow.analyst_count, null);
assert.equal(missing.consensusRow.targetMeanPrice, null);
assert.match(missing.triageRow.valuation, /Base: ~n\/a/);
assert.equal(missing.metricRows.some(x => x.Metric === 'trailing_pe'), false);
for (const v of [1.5, 1.51, 2.94, -1.51, 0]) {
  const result = score({growth:{earningsGrowth:v}});
  assert.equal(result.metricRows.find(x => x.Metric === 'earnings_growth_pct').Value, v * 100);
}
const zero = score({growth:{revenueGrowth:0},financialHealth:{debtToEquity:0}});
assert.equal(zero.metricRows.find(x => x.Metric === 'revenue_growth_pct').Value, 0);
assert.equal(zero.metricRows.find(x => x.Metric === 'debt_to_equity').Unit, '%');
const empty = score({growth:{revenueGrowth:' '},price:{currentPrice:false}});
assert.equal(empty.triageRow.current_price, null);
assert.equal(empty.metricRows.some(x => x.Metric === 'revenue_growth_pct'), false);
assert.equal(score({growth:{earningsGrowth:2.94,earningsQuarterlyGrowth:2.979,revenueGrowth:.242}}).triageRow.growth_score,100);
console.log('AG3 score: 15 assertions passed');
