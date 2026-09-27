const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '..');
const mac = path.join(root, 'agents/common/AG4-V3/nodes');
const spe = path.join(root, 'agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes');
const codes = process.argv[2] ? JSON.parse(fs.readFileSync(process.argv[2], 'utf8')) : {
 macro: fs.readFileSync(path.join(mac,'10b_parse_llm_output_reduced.js'),'utf8'),
 route: fs.readFileSync(path.join(mac,'08_route_new_seen.js'),'utf8'),
 normalize: fs.readFileSync(path.join(mac,'03_normalize_rss_items.js'),'utf8'),
 ...Object.fromEntries(['09_parse_llm_output.js','finnhub_parse.js','ibkr_parse.js'].map(f=>[f,fs.readFileSync(path.join(spe,f),'utf8')]))
};
let count = 0;
function macro(j) { return new Function('$json', codes.macro)(j)[0].json; }
const ai = {isActionable:true, impact_score:9, confidence:0.8, sectors_bullish:['Technology'], sectors_bearish:[], impact_magnitude:'High', notes:'test', market_regime:'Risk-On', macro_theme:'Tech/AI'};
const input = {dedupeKey:'a', output:ai, universeSectors:['Technology'], source:'Reuters', canonicalUrl:'https://example.org/news'};
assert.equal(macro(input).ImpactScore,9); count++;
assert.equal(macro({...input,output:{...ai,impact_magnitude:'Low'}}).ImpactScore,3); count++;
assert.equal(macro({...input,output:{...ai,isActionable:false}}).ImpactScore,0); count++;
for (const output of [null,{},'invalid',[],{...ai,confidence:'x'}]) {assert.throws(()=>macro({...input,output}),/INVALID_LLM/);count++;}
assert.throws(()=>macro({...input,error:{message:'Forbidden'}}),/INVALID_LLM/);count++;
const now=new Date().toISOString();
function route(n,old) {return new Function('$json','$items', codes.route)(n,()=>[{json:{historyIndex:{a:old}}}])[0].json;}
const old={title:'Same',publishedAt:now,analyzedAt:now,tagger_version:'reduced_deepseek_flash_v2',firstSeenAt:'2026-01-01',ImpactScore:3};
assert.equal(route({dedupeKey:'a',title:'Changed',preImpactScore:3},old)._action,'analyze');count++;
assert.equal(route({dedupeKey:'a',title:'Same',publishedAt:now,preImpactScore:8},old)._action,'skip');count++;
assert.equal(route({dedupeKey:'a',title:'Same',publishedAt:now,preImpactScore:3},{...old,tagger_version:'reduced_grok_v1'})._action,'analyze');count++;
const norm=new Function('$input','$',codes.normalize)({all:()=>[{json:{title:'Test',link:'https://www.3ds.com/news',pubDate:now}}]},()=>({item:{json:{source:'Official issuer',url:'https://www.3ds.com/rss'}}}));
assert.equal(norm[0].json.source,'Official issuer');count++;
for (const name of ['09_parse_llm_output.js','finnhub_parse.js','ibkr_parse.js']) {
 const run=(j)=>new Function('$input','$json',codes[name])({all:()=>[{json:j}]},j);
 assert.throws(()=>run({output:{},symbol:'TEST'}),/INVALID_LLM/);count++;
 const out=run({symbol:'TEST',source:'issuer_rss',output:{isRelevant:true,impactScore:3,confidence:60,summary:'Valid test',sentiment:'Bullish'}});
 assert.equal(out[0].json.source,'issuer_rss');count++;
}
console.log(JSON.stringify({ok:true,assertions:count}));
