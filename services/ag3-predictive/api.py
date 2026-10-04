import json
from fastapi import FastAPI, HTTPException
from store import ROOT, initialize, status, db, now
from historical_evidence import load_cards, timestamp, LEGEND

app=FastAPI(title='AG3 predictive research',version='1.0.0')


@app.on_event('startup')
def startup():
    initialize()


@app.get('/health')
def health():
    with db(True) as c:
        c.execute('SELECT 1').fetchone()
    return {'ok':True,'mode':'SHADOW','decision_enabled':False}


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
    return {'card':load_cards([symbol],cutoff)[symbol],'legend':LEGEND,'live_decision_enabled':False}


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
