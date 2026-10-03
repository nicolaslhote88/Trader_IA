"""Build a surgical update from published exports, preserving schedules/guards."""
import argparse
import copy
import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AG3 = ROOT / 'agents/trading-actions/AG3 - Les fondamentaux/AG3-V2'
AG1 = ROOT / 'agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow'
NAME = '20L — Fundamental Evidence'


def digest_source():
    shared = (ROOT / 'services/dashboard/fundamental_context.py').read_text(encoding='utf-8')
    return shared + '''

items = _items or []
for item in items:
    body = item.get("json", {})
    pack = body.get("opportunity_pack")
    if not isinstance(pack, dict):
        continue
    selected = pack.get("rows") or []
    symbols = [row.get("symbol") for row in selected if row.get("symbol")]
    cards = load_fundamental_context("/files/duckdb/ag3_v2.duckdb", symbols) if symbols else {}
    for row in selected:
        row["fundamentals"] = cards.get(str(row.get("symbol") or "").upper(), {"schema": FUNDAMENTAL_SCHEMA, "flags": ["MISSING_FUNDA"], "predictive_status": "NOT_VALIDATED"})
    pack["fundamental_legend"] = FUNDAMENTAL_LEGEND
return items
'''


def update(workflow):
    result = copy.deepcopy(workflow)
    if result['id'].startswith('AG3V2'):
        node = next(n for n in result['nodes'] if n['name'] == 'AG3V2.06 - Score Fundamentals')
        node['parameters']['jsCode'] = (AG3 / 'nodes/02_score_fundamentals.js').read_text(encoding='utf-8')
    else:
        assert result['id'] == 'AG1V4CONSENSUS'
        existing = next((n for n in result['nodes'] if n['name'] == NAME), None)
        if existing is None:
            source = next(n for n in result['nodes'] if n['name'] == 'Calcul Matrice & Briefing')
            existing = {'id': 'fundamental-evidence-20261003', 'name': NAME, 'type': 'n8n-nodes-base.code',
                        'typeVersion': 2, 'position': [source['position'][0] + 220, source['position'][1]],
                        'parameters': {'language': 'pythonNative', 'mode': 'runOnceForAllItems'}}
            result['nodes'].append(existing)
            prior = result['connections']['Calcul Matrice & Briefing']
            assert prior == {'main': [[{'node': 'Merge7', 'type': 'main', 'index': 1}]]}
            result['connections'][NAME] = prior
            result['connections']['Calcul Matrice & Briefing'] = {'main': [[{'node': NAME, 'type': 'main', 'index': 0}]]}
        existing['parameters']['pythonCode'] = digest_source()
    result['versionId'] = str(uuid.uuid4())
    result.pop('activeVersionId', None)
    return {k: result[k] for k in ['id','name','nodes','connections','settings','staticData','pinData','active','versionId'] if k in result}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--exports', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for wid in ['AG3V2HELDCORE20260622', 'AG3V2WATCHNIGHT20260622', 'AG1V4CONSENSUS']:
        old = json.loads((args.exports / (wid + '.json')).read_text(encoding='utf-8'))
        new = update(old)
        before = {n['name']: n for n in old['nodes']}
        modified = [n['name'] for n in new['nodes'] if before.get(n['name']) != n]
        assert modified == (['AG3V2.06 - Score Fundamentals'] if wid.startswith('AG3') else [NAME]), modified
        assert old['settings'] == new['settings']
        (args.output / (wid + '.json')).write_text(json.dumps(new, ensure_ascii=False, indent=2), encoding='utf-8')
        print(wid, modified)
    (AG1 / 'nodes/pre_agent/fundamental_evidence.code.py').write_text(digest_source(), encoding='utf-8')


if __name__ == '__main__':
    main()
