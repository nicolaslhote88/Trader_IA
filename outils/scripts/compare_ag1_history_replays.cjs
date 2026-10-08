// Pure replay of published consensus/safety code. No network or DB imports.
const fs=require('fs'),path=require('path'),assert=require('node:assert/strict');
const base=process.argv[2];
const read=p=>JSON.parse(fs.readFileSync(path.join(base,p),'utf8'));
const workflow=read('published.json');
const manifest=read('paired/manifest.json');
const frozen=Date.parse(manifest.as_of);
class ReplayDate extends Date {constructor(...a){super(...(a.length?a:[frozen]));} static now(){return frozen;}}
const names=['Information Extractor','Information Extractor1','Information Extractor2'];
const results={};
function run(name,json,items){
  const code=workflow.nodes.find(n=>n.name===name).parameters.jsCode;
  return new Function('$json','$input','Date',code)(json,{all:()=>items||[{json}]},ReplayDate)[0].json;
}
for(const label of ['baseline1','enriched1','enriched2','baseline2']){
  const variant=label.startsWith('baseline')?'baseline':'enriched';
  const context=read('paired/'+variant+'.json');
  const rd=read(label+'-result.json').data.resultData.runData;
  const proposals=names.map(n=>rd[n].at(-1).data.main[0][0].json);
  assert.ok(proposals.every(p=>p.extractorStatus.startsWith('OK_')));
  const consensus=run('AG1.V4 — Build Consensus',{},[{json:context},...proposals.map(json=>({json}))]);
  const safety=run('7 - Validate & Enforce Safety',consensus);
  const llmOutput=name=>rd[name]?.at(-1)?.data?.ai_languageModel?.[0]?.[0]?.json||{};
  const gpt=llmOutput('OpenAI Chat Model - GPT6sol'),deepseek=llmOutput('DeepSeek Chat Model');
  const claude=rd['Anthropic Messages - Opus5.5']?.at(-1)?.data?.main?.[0]?.[0]?.json||{};
  results[label]={proposals:proposals.map(p=>({model:p.modelId,status:p.extractorStatus,...p.output})),
    usage:{gpt_estimated:gpt.tokenUsageEstimate||null,deepseek_reported:deepseek.tokenUsage||null,claude_reported:claude.usage||null},
    consensus_actions:consensus.agentDecision.actions,consensus_decisions:consensus.consensusDecisions,
    hypothetical_orders:safety.orders,safety_decision:safety.decision,safety_warnings:safety.warnings,
    safety_metrics:safety.metrics};
}
function compare(a,b){
  const m=new Map(a.map(x=>[x.symbol,x])),n=new Map(b.map(x=>[x.symbol,x]));
  const symbols=[...new Set([...m.keys(),...n.keys()])].sort();
  const changes=[];
  const trading=new Set(['OPEN','INCREASE','DECREASE','CLOSE']);
  let tradeIntentChanges=0,tradeWeightChanges=0;
  for(const symbol of symbols){
    const x=m.get(symbol),y=n.get(symbol);
    const before=x?.action??'NO_PROPOSAL',after=y?.action??'NO_PROPOSAL';
    const weightBefore=x?.targetWeightPct??null,weightAfter=y?.targetWeightPct??null;
    const tx=trading.has(before)?before:'PASSIVE',ty=trading.has(after)?after:'PASSIVE';
    if(tx!==ty)tradeIntentChanges++;
    if(tx!==ty||((trading.has(before)||trading.has(after))&&weightBefore!==weightAfter))tradeWeightChanges++;
    if(before!==after||weightBefore!==weightAfter) changes.push({symbol,before,after,weight_before:weightBefore,weight_after:weightAfter});
  }
  return {symbols:symbols.length,action_changes:changes.filter(x=>x.before!==x.after).length,action_or_weight_changes:changes.length,
    trade_intent_changes:tradeIntentChanges,trade_intent_or_weight_changes:tradeWeightChanges,changes};
}
const comparisons={};
for(const [name,a,b] of [['baseline_variability','baseline1','baseline2'],['enriched_variability','enriched1','enriched2'],['paired_1','baseline1','enriched1'],['paired_2','baseline2','enriched2']]){
  comparisons[name]={models:results[a].proposals.map((p,i)=>({model:p.model,...compare(p.actions,results[b].proposals[i].actions)})),
    consensus:compare(results[a].consensus_actions,results[b].consensus_actions),
    hypothetical_order_counts:[results[a].hypothetical_orders.length,results[b].hypothetical_orders.length]};
}
const report={contract:'AG1_HISTORY_AB_RESULTS_V1',generated_at:new Date().toISOString(),manifest,
  design:'One frozen historical context; A1 B1 B2 A2; three published AG1 models; unchanged prompts; frozen consensus/safety clock.',
  limitations:['One context and two repetitions per arm cannot establish statistical significance.',
    'No realized or simulated return benefit established; subsequent prices unavailable for intended horizons.',
    'Historical LLM replay may contain training knowledge beyond the context date; not a point-in-time financial backtest.',
    'HOLD, WATCH and omitted proposals count as passive for trade_intent_changes; verbosity is not a trading change.',
    'Captured October 2 baseline predates the October 3 fundamental evidence card; no later snapshot injected.'],
  live_decision_enabled:false,orders_sent:0,comparisons,results};
fs.writeFileSync(path.join(base,'comparison.json'),JSON.stringify(report,null,2));
console.log(JSON.stringify({comparisons,orders_sent:0},null,2));
