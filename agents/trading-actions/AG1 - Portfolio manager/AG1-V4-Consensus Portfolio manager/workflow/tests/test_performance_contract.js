const fs = require("fs");
const path = require("path");
const assert = require("assert/strict");
const root = path.resolve(__dirname, "..");
async function run(file, json, items, http) {
  const code = fs.readFileSync(path.join(root, "nodes", file), "utf8");
  return (await new Function("$json", "$input", "$env", `return (async () => {${code}\n})()`)
    .call({helpers:{httpRequest: http || (() => {throw Error("Unexpected network access");})}}, json,
      {all:()=>items || [{json}]}, {}))[0].json;
}
function buy(symbol="NEW", qty=10) { return {symbol, action:"OPEN", assetClass:"EQUITY",targetQty:qty,
  currency:"EUR", fxRateToEUR:1, priceEUR:100, priceHint:100, sector:"Tech", entryPlan:{orderType:"LIMIT",limitPrice:100},
  liquidity:{status:"OK",contractResolved:true}, SpreadPct:.1}; }
function ctx(cash=2000) {return {portfolioSummary:{cashEUR:cash,totalPortfolioValueEUR:10000,positions:[{symbol:"OLD",quantity:10,lastPrice:100,marketValue:1000,sector:"Industry"}]},
  config:{min_order_value_eur:1000,max_order_value_pct:25,max_pos_pct:25,max_sector_pct:40,max_open_positions:10},
  agentDecision:{actions:[]}};}
async function main() {
  const input=ctx(0);input.agentDecision.actions=[{...buy("OLD"),action:"CLOSE"},buy()];
  let result=await run("post_agent/07_validate_enforce_safety_v5.code.js",input);
  assert.deepEqual(result.orders.map(o=>o.side),["SELL"],"Proposed sale must not finance BUY");
  const daily=ctx();daily.portfolioSummary.dailyReturnPct=-7;daily.agentDecision.actions=[buy(),{...buy("OLD"),action:"CLOSE"}];
  result=await run("post_agent/07_validate_enforce_safety_v5.code.js",daily);
  assert.deepEqual(result.orders.map(o=>o.side),["SELL"]);
  assert(result.warnings.some(w=>w.includes("DAILY_LOSS_BUY_BLOCKED")));
  const lot=ctx();lot.agentDecision.actions=[{...buy("JAPAN",4),quantityIncrement:100}];
  result=await run("post_agent/07_validate_enforce_safety_v5.code.js",lot);
  assert(result.warnings.some(w=>w.includes("BOARD_LOT_MISMATCH")));
  const batch=ctx(10000);batch.config.max_sector_pct=15;batch.agentDecision.actions=[buy("ONE"),buy("TWO")];
  result=await run("post_agent/07_validate_enforce_safety_v5.code.js",batch);
  assert.equal(result.orders.length,1,"Batch must reserve concentration without pending sales");
  const pack=await run("agent_input/ag1_00_assemble_input_packs.code.js",{},[{json:{portfolioBrief:{cash:0,totalValue:1000,positions:[{symbol:"OLD",quantity:10,avgPriceNative:100,lastPriceNative:110,perfLocalPct:10,holdingDays:15,openedAt:"2026-01-01",positionLifecycle:{lastBuyAt:"2026-01-01",source:"core.fills"}}]}}}]);
  assert.equal(pack.portfolio_pack.positions[0].heldDays,15);
  assert.equal(pack.portfolio_pack.positions[0].avgPriceEUR,null,"Unknown cost must remain unknown");
  const rows=Array.from({length:12},(_,i)=>({symbol:"X"+i,symbol_yahoo:"X"+i,entry:100,decision:"Entrer / Renforcer",gates:"OK",volume:1e6,sector:"Tech"}));
  const pre={portfolio_pack:{cashEUR:9000,totalValueEUR:10000,positions:[]},config:{min_order_value_eur:1000},opportunity_pack:{rows}};
  let calls=[];
  const http=async ({method,url})=>{
    assert.equal(method,"GET");assert(!url.includes("/orders"));calls.push(url);
    if(url.includes("/quote?")) return {quotes: rows.map(r=>({symbol:r.symbol,volume:1e6,regularMarketPrice:100,regularMarketTime:new Date().toISOString(),spreadPct:.1}))};
    if(url.includes("/contracts/equity/resolve"))return {results:rows.map((r,i)=>({symbol:r.symbol,conid:i+1,currency:"EUR",quantity_increment:i<10?100:1,min_quantity:i<10?100:1,quantity_rule_known:true})),errors:[]};
    if(url.includes("/account/ledger"))return {EUR:{exchangerate:1}};
    if(url.includes("/marketdata/snapshot")) return rows.map((r,i)=>({conid:i+1,"31":100,"84":99.95,"86":100.05}));
    throw Error("Unexpected endpoint "+url);
  };
  result=await run("pre_agent/ag1_v4_liquidity_preflight.code.js",pre,null,http);
  assert.deepEqual(result.opportunity_pack.rows.map(r=>r.symbol),["X10","X11"],"Reserve must replace infeasible top ten");
  assert.equal(result.execution_audit.length,12);
  const heldPre=structuredClone(pre);heldPre.portfolio_pack.cashEUR=0;
  heldPre.portfolio_pack.positions=[{symbol:"X10",quantity:10,marketValue:1000,sector:"Tech"}];
  heldPre.opportunity_pack.rows=[{...rows[10],decision:"Reduire / Sortir"}];
  result=await run("pre_agent/ag1_v4_liquidity_preflight.code.js",heldPre,null,http);
  assert.equal(result.opportunity_pack.rows[0].buy_feasibility.feasible,false);
  assert.equal(result.opportunity_pack.rows[0].sell_feasibility.feasible,true);
  const fxInput=structuredClone(pre);fxInput.opportunity_pack.rows=[{...rows[0],symbol:"KRW",symbol_yahoo:"KRW",entry:100}];
  let oldFx=false;
  const fxHttp=async ({url,method})=>{
    assert.equal(method,"GET");
    if(url.includes("EURKRW"))return {quotes:[{symbol:"EURKRW=X",regularMarketPrice:1000,regularMarketTime:new Date(Date.now()-(oldFx?9:0)*3600000-1000).toISOString()}]};
    if(url.includes("/quote?"))return {quotes:[{symbol:"KRW",regularMarketPrice:100,regularMarketTime:new Date().toISOString(),volume:1e6,spreadPct:.1}]};
    if(url.includes("/contracts/"))return {results:[{symbol:"KRW",conid:1,currency:"KRW",quantity_rule_known:true,quantity_increment:1,min_quantity:1}],errors:[]};
    if(url.includes("/account/ledger"))return {EUR:{exchangerate:1}};
    if(url.includes("/fx/snapshot"))return {quotes:[],errors:[]};
    if(url.includes("/snapshot"))return [{conid:1,"31":100,"84":99.95,"86":100.05,"6509":"DpB",_updated:Date.now()}];
    throw Error("Unexpected URL "+url);
  };
  result=await run("pre_agent/ag1_v4_liquidity_preflight.code.js",fxInput,null,fxHttp);
  assert.equal(result.opportunity_pack.rows[0].fx_rate_to_eur,.001);
  assert.equal(result.opportunity_pack.rows[0].market_state,"IBKR_DELAYED");
  assert(result.opportunity_pack.rows[0].liquidity.quoteAgeMinutes>=19.9);
  oldFx=true;
  result=await run("pre_agent/ag1_v4_liquidity_preflight.code.js",fxInput,null,fxHttp);
  assert.equal(result.opportunity_pack.rows.length,0,"Stale FX must not become a fresh conversion");
  console.log("performance_contract: lifecycle pack, cash, daily loss, lots, cumulative sector, reserve PASS");
}
main().catch(e=>{console.error(e);process.exit(1)});
