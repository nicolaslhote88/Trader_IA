"""Build isolated LLM-only replay graphs from the actually published AG1 graph."""
import argparse
import copy
import json
from pathlib import Path

KEEP={'Agent #1 - Portfolio manager','Agent #1 - Portfolio manager1','Agent #1 - Portfolio manager2',
      'OpenAI Chat Model - GPT6sol','DeepSeek Chat Model','Anthropic Messages - Opus5.5',
      'AG1.V4 — Structured Output GPT','AG1.V4 — Structured Output DeepSeek','AG1.V4 — Structured Output Claude',
      'Information Extractor','Information Extractor1','Information Extractor2'}


def build(published,context,variant):
    nodes=[copy.deepcopy(n) for n in published['nodes'] if n['name'] in KEEP]
    assert {n['name'] for n in nodes}==KEEP
    http=next(n for n in nodes if n['name']=='Anthropic Messages - Opus5.5')
    assert http['parameters']['url']=='https://api.anthropic.com/v1/messages'
    # Exact live transports, parsers, model options and prompts; only input differs.
    connections={k:{t:[[x for x in g if x['node'] in KEEP] for g in groups] for t,groups in v.items()}
                 for k,v in published['connections'].items() if k in KEEP}
    nodes.extend([
        {'id':'history-manual','name':'Manual Trigger','type':'n8n-nodes-base.manualTrigger','typeVersion':1,'position':[0,0],'parameters':{}},
        {'id':'history-input','name':'Replay Input','type':'n8n-nodes-base.code','typeVersion':2,'position':[200,0],
         'parameters':{'jsCode':'return [{json:'+json.dumps(context,ensure_ascii=False)+'}];'}}])
    connections['Manual Trigger']={'main':[[{'node':'Replay Input','type':'main','index':0}]]}
    connections['Replay Input']={'main':[[{'node':n,'type':'main','index':0} for n in
        ['Agent #1 - Portfolio manager','Agent #1 - Portfolio manager1','Agent #1 - Portfolio manager2']]]}
    assert all(n['name'] in KEEP|{'Manual Trigger','Replay Input'} for n in nodes)
    return {'id':'AG1HISTORY'+variant.upper(),'name':'Isolated AG1 historical evidence '+variant,
            'active':False,'nodes':nodes,'connections':connections,'settings':{'executionOrder':'v1'}}


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--published',type=Path,required=True)
    p.add_argument('--paired',type=Path,required=True)
    a=p.parse_args()
    w=json.loads(a.published.read_text(encoding='utf-8'))
    for variant in ['baseline','enriched']:
        context=json.loads((a.paired/(variant+'.json')).read_text(encoding='utf-8'))
        result=build(w,context,variant)
        (a.paired/(variant+'-workflow.json')).write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Two manual LLM-only graphs; no broker, trigger schedule, business DB or live workflow import.')
