import json
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from store import ROOT, initialize, status, db, now
from historical_evidence import load_cards, timestamp, LEGEND, compact, SCHEMA

app=FastAPI(title='AG3 predictive research',version='1.1.0')


def history_enabled():
    return os.getenv('AG3_HISTORY_LIVE_ENABLED','0')=='1'


@app.on_event('startup')
def startup():
    initialize()


@app.get('/health')
def health():
    with db(True) as c:
        c.execute('SELECT 1').fetchone()
    return {'ok':True,'mode':'SHADOW','decision_enabled':False,
            'historical_evidence_live_enabled':history_enabled()}


@app.get('/status')
def get_status():
    return status()


@app.get('/evidence/{symbol}')
def evidence(symbol: str, as_of: str = ''):
    cutoff=as_of or now()
    try:
        if timestamp(cutoff)>timestamp(now()):
            raise ValueError('FUTURE_AS_OF')
    except (ValueError,TypeError):
        raise HTTPException(status_code=422,detail='Explicit past/present timestamp with timezone required')
    symbol=symbol.upper()
    return {'card':load_cards([symbol],cutoff)[symbol],'legend':LEGEND,'live_decision_enabled':history_enabled()}


class EvidenceRequest(BaseModel):
    symbols: list[str] = Field(max_length=100)
    as_of: str


@app.post('/ag1/historical-evidence')
def historical_batch(request: EvidenceRequest):
    try:
        cutoff=timestamp(request.as_of)
        if cutoff>timestamp(now()):
            raise ValueError('FUTURE_AS_OF')
    except (ValueError,TypeError):
        raise HTTPException(status_code=422,detail='Past/present timestamp with timezone required')
    if any(not s.strip() or len(s)>32 for s in request.symbols):
        raise HTTPException(status_code=422,detail='Invalid symbol')
    cards=load_cards(request.symbols,cutoff.isoformat())
    return {'schema':SCHEMA,'as_of':cutoff.isoformat(),'generated_at':now(),
            'cards':{s:compact(c) for s,c in cards.items()},'legend':LEGEND,
            'advisory_only':True,'predictive_probabilities_supplied':False}


@app.get('/research/{symbol}')
def research(symbol: str):
    symbol=symbol.upper()
    with db(True) as c:
        coverage=[dict(r) for r in c.execute('SELECT pipe,status,detail,updated_at FROM coverage WHERE symbol=?',(symbol,))]
        accounts=[dict(r) for r in c.execute('''SELECT source,period_end,metric,unit,value,date_quality,available_at
            FROM facts WHERE symbol=? AND metric IN ('revenue','net_income','assets','equity','operating_cashflow')
            ORDER BY period_end DESC,available_at DESC LIMIT 300''',(symbol,))]
    path=ROOT/'predictions.json'
    payload=json.loads(path.read_text()) if path.exists() else {}
    predictions=[r for r in payload.get('predictions',[]) if r['symbol']==symbol]
    return {'symbol':symbol,'generated_at':now(),'mode':'SHADOW','decision_enabled':False,
            'coverage':coverage,'accounts':accounts,'predictions':predictions,
            'predictions_generated_at':payload.get('generated_at')}
