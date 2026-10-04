"""Surgical published-workflow extension. Never changes model or order nodes."""
import argparse
import copy
import json
import uuid
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
PRE='AG1.V4 — Liquidity Preflight'
FETCH='AG1.HISTORY — Fetch Dated Accounts'
ATTACH='AG1.HISTORY — Attach Dated Accounts'
CODE=ROOT/'agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/historical_evidence_attach.code.js'


def build(old):
    assert old['id']=='AG1V4CONSENSUS'
    w=copy.deepcopy(old)
    assert not any(n['name'] in {FETCH,ATTACH} for n in w['nodes'])
    prior=w['connections'][PRE]
    assert {x['node'] for x in prior['main'][0]}=={'Agent #1 - Portfolio manager','Agent #1 - Portfolio manager1','Agent #1 - Portfolio manager2','AG1.V4 — Merge Model Proposals'}
    position=next(n['position'] for n in w['nodes'] if n['name']==PRE)
    w['nodes'].extend([
        {'id':'ag1-history-fetch-20261004','name':FETCH,'type':'n8n-nodes-base.httpRequest','typeVersion':4.3,
         'position':[position[0]+200,position[1]],'onError':'continueRegularOutput',
         'parameters':{'method':'POST','url':'http://ag3-predictive:8084/ag1/historical-evidence','sendBody':True,
            'specifyBody':'json','jsonBody':"={{ {symbols: [...new Set([...($json.opportunity_pack?.rows || []), ...($json.portfolio_pack?.positions || [])].map(r => String(r.symbol || '').toUpperCase()).filter(Boolean))], as_of: $json.run?.timestampUtc} }}",
            'options':{'timeout':8000,'response':{'response':{'responseFormat':'json'}}}}},
        {'id':'ag1-history-attach-20261004','name':ATTACH,'type':'n8n-nodes-base.code','typeVersion':2,
         'position':[position[0]+400,position[1]],'parameters':{'jsCode':CODE.read_text(encoding='utf-8')}}])
    w['connections'][PRE]={'main':[[{'node':FETCH,'type':'main','index':0}]]}
    w['connections'][FETCH]={'main':[[{'node':ATTACH,'type':'main','index':0}]]}
    w['connections'][ATTACH]=prior
    assert w['nodes'][:-2]==old['nodes']
    assert w['settings']==old['settings']
    w['versionId']=str(uuid.uuid4())
    return {k:w[k] for k in ['id','name','nodes','connections','settings','staticData','pinData','active','versionId'] if k in w}


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--published',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    w=build(json.loads(a.published.read_text(encoding='utf-8')))
    a.output.write_text(json.dumps(w,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'version':w['versionId'],'added_nodes':[FETCH,ATTACH],'existing_nodes_changed':0}))
