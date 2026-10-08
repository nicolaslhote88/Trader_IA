const fs=require('fs'),assert=require('node:assert/strict'),path=require('path');
const code=fs.readFileSync(path.join(__dirname,'../agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/historical_evidence_attach.code.js'),'utf8');
const asOf='2026-10-02T15:10:07.448Z',schema='AG3_HISTORICAL_EVIDENCE_V1';
const source={run:{timestampUtc:asOf},config:{max_pos_pct:25},opportunity_pack:{rows:[{symbol:'X',score:74,fundamentals:{schema:'AG3_EVIDENCE_V1',metrics:{net_margin_pct:10}}}]},portfolio_pack:{positions:[{symbol:'X',qty:4}]}};
const response={schema,as_of:asOf,advisory_only:true,predictive_probabilities_supplied:false,generated_at:asOf,
  legend:{schema},cards:{X:{schema,symbol:'X',as_of:asOf,status:'DATED_ACCOUNTS',advisory_only:true,predictive_probabilities_supplied:false,
    periods:[{period_end:'2025-12-31',available_at:'2026-02-01T00:00:00Z',reporting_currency:'USD',ratios_pct:{net_margin:10},amounts_million:{revenue:100},filings:['original']}],changes:null}}};
const clone=x=>JSON.parse(JSON.stringify(x));
const run=r=>new Function('$','$input',code)(name=>({first:()=>({json:source})}),{first:()=>({json:r})})[0].json;
const before=clone(source),out=run(response);
assert.deepEqual(source,before);
assert.deepEqual(out.config,source.config);
assert.deepEqual(out.portfolio_pack,source.portfolio_pack);
assert.deepEqual(out.opportunity_pack.rows[0].fundamentals.metrics,{net_margin_pct:10});
assert.equal(out.opportunity_pack.historical_accounts_delivery.covered,1);
assert.equal(out.opportunity_pack.historical_accounts_delivery.status,'AVAILABLE');
const mutations=[r=>({error:'timeout'}),r=>{r.cards.X.periods[0].available_at='2027-01-01T00:00:00Z';return r;},
 r=>{r.cards={};return r;},r=>{r.as_of='2026-10-03T00:00:00Z';return r;},
 r=>{r.predictive_probabilities_supplied=true;return r;}];
for(const mutate of mutations){
 const result=run(mutate(clone(response)));
 assert.equal(result.opportunity_pack.historical_accounts_delivery.status,'UNAVAILABLE_OR_INVALID');
 assert.deepEqual(result.opportunity_pack.rows[0].fundamentals.historical_accounts.periods,[]);
 assert.equal(result.opportunity_pack.rows[0].score,74);
 assert.deepEqual(result.config,source.config);
}
const injected=run({...response,config:{max_pos_pct:100},orders:[{symbol:'FAKE'}]});
assert.deepEqual(injected.config,source.config);assert.equal(injected.orders,undefined);
console.log(JSON.stringify({ok:true,cases:7,original_inputs_preserved:true,failure_explicit:true}));
