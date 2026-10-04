"""Prepare paired AG1 inputs, writing only an explicitly isolated output folder."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

if len(Path(__file__).resolve().parents)>2:
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'services/ag3-predictive'))
from historical_evidence import load_cards, enrich_context


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def prepare(context):
    as_of=context['run']['timestampUtc']
    rows=context['opportunity_pack'].get('rows',[])
    held=context.get('portfolio_pack',{}).get('positions',[])
    symbols=sorted({r['symbol'] for r in rows+held if r.get('symbol')})
    cards=load_cards(symbols,as_of)
    enriched=enrich_context(context,cards)
    # Prove the intervention adds only historical evidence, not market inputs/gates.
    restored=json.loads(json.dumps(enriched))
    p=restored['opportunity_pack']
    p.pop('held_historical_accounts');p.pop('historical_accounts_legend')
    for before,after in zip(context['opportunity_pack']['rows'],p['rows']):
        after.get('fundamentals',{}).pop('historical_accounts',None)
        if 'fundamentals' not in before:
            after.pop('fundamentals',None)
    assert restored==context
    manifest={'contract':'AG1_HISTORY_AB_REPLAY_V1','live_decision_enabled':False,
        'run_id':context['run']['runId'],'as_of':as_of,'baseline_sha256':digest(context),
        'enriched_sha256':digest(enriched),'non_history_inputs_unchanged':True,
        'symbols':len(symbols),'dated_symbols':[s for s,c in cards.items() if c['status']=='DATED_ACCOUNTS'],
        'unavailable_symbols':[s for s,c in cards.items() if c['status']!='DATED_ACCOUNTS'],
        'added_characters':len(json.dumps(enriched))-len(json.dumps(context)),
        'historical_baseline_note':'Frozen captured AG1 run; does not retroactively include features deployed after that run.',
        'performance_validation':'NOT_ESTABLISHED; decision sensitivity replay, not a return backtest'}
    return {'baseline':context,'enriched':enriched,'cards':cards,'manifest':manifest}


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--input',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    values=prepare(json.loads(a.input.read_text(encoding='utf-8')))
    a.output.mkdir(parents=True,exist_ok=True)
    for name,value in values.items():
        (a.output/(name+'.json')).write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(values['manifest']))
