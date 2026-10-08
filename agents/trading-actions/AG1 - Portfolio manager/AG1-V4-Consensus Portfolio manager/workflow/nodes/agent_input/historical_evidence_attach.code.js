// Attach only the historical facts; original prices, gates and configuration remain authoritative.
const source = $('AG1.V4 — Liquidity Preflight').first().json;
const result = JSON.parse(JSON.stringify(source));
const response = $input.first().json || {};
const schema = 'AG3_HISTORICAL_EVIDENCE_V1';
const asOf = result.run?.timestampUtc;
const cutoff = Date.parse(asOf);
const rows = result.opportunity_pack?.rows || [];
const held = result.portfolio_pack?.positions || [];
const symbols = [...new Set([...rows,...held].map(r=>String(r.symbol||'').toUpperCase()).filter(Boolean))];
const validTime = value => typeof value === 'string' && /(?:Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value));
let status = 'AVAILABLE';
const validCard = (c,s) => c && c.schema===schema && c.symbol===s && validTime(c.as_of) && Date.parse(c.as_of)===cutoff &&
    c.advisory_only===true && c.predictive_probabilities_supplied===false &&
    ['DATED_ACCOUNTS','STALE_ANNUAL_ACCOUNTS','NO_DATED_ACCOUNTS'].includes(c.status) && Array.isArray(c.periods) && c.periods.length<=3 &&
    c.periods.every(p=>validTime(p.available_at) && Date.parse(p.available_at)<=cutoff &&
        /^\d{4}-\d{2}-\d{2}$/.test(p.period_end) && Date.parse(p.period_end)<=cutoff &&
        typeof p.reporting_currency==='string' && p.ratios_pct && p.amounts_million && Array.isArray(p.filings));
if (!validTime(asOf) || response.schema!==schema || Date.parse(response.as_of)!==cutoff ||
    response.advisory_only!==true || response.predictive_probabilities_supplied!==false || !response.cards ||
    !symbols.every(s=>validCard(response.cards[s],s))) status='UNAVAILABLE_OR_INVALID';
const cards={};
for(const s of symbols) cards[s]=status==='AVAILABLE'?response.cards[s]:{
    schema,symbol:s,as_of:asOf||null,status:'SOURCE_UNAVAILABLE',periods:[],changes:null,
    flags:['HISTORY_FETCH_FAILED_OR_INVALID'],advisory_only:true,predictive_probabilities_supplied:false};
const pack=result.opportunity_pack || (result.opportunity_pack={});
for(const row of rows){
    if(!row.fundamentals || typeof row.fundamentals!=='object')row.fundamentals={};
    row.fundamentals.historical_accounts=cards[String(row.symbol||'').toUpperCase()];
}
pack.held_historical_accounts=Object.fromEntries(held.filter(r=>r.symbol).map(r=>[r.symbol,cards[String(r.symbol).toUpperCase()]]));
pack.historical_accounts_legend=status==='AVAILABLE'?response.legend:{schema,use:'Historical source unavailable. Do not infer historical trends from missing data.'};
pack.historical_accounts_delivery={schema,status,as_of:asOf||null,advisory_only:true,
    covered:symbols.filter(s=>cards[s].status==='DATED_ACCOUNTS').length,total:symbols.length,
    generated_at:status==='AVAILABLE'?response.generated_at:null,
    performance_benefit:'NOT_DEMONSTRATED',predictive_probabilities_supplied:false};
return [{json:result}];
