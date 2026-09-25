"""Scoped, idempotent migration; no API calls or live writes."""
import copy
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CLAUDE_AGENT = 'Agent #1 - Portfolio manager2'
CLAUDE_DECODE = 'AG1.V4 — Structured Output Claude'


def migrate(workflow):
    w = copy.deepcopy(workflow)
    by = {n['name']: n for n in w['nodes']}
    gpt = next(n for n in w['nodes'] if n['type'].endswith('.lmChatOpenAi') and n['name'].startswith('OpenAI Chat Model - GPT'))
    old_name = gpt['name']
    gpt['name'] = 'OpenAI Chat Model - GPT6sol'
    gpt['parameters']['model'] = {'__rl': True, 'value': 'gpt-6-sol', 'mode': 'id'}
    gpt['parameters']['responsesApiEnabled'] = True
    if old_name != gpt['name']:
        w['connections'][gpt['name']] = w['connections'].pop(old_name)
        for connections in w['connections'].values():
            for outputs in connections.values():
                for targets in outputs:
                    for target in targets:
                        if target['node'] == old_name:
                            target['node'] = gpt['name']

    agent = by[CLAUDE_AGENT]
    if agent['type'] != 'n8n-nodes-base.code':
        system = agent['parameters']['options']['systemMessage'].replace('Claude Opus 4.8', 'Claude Opus 5.5')
        text = agent['parameters']['text'].removeprefix('=')
        parts = re.split(r'\{\{(.*?)\}\}', text, flags=re.S)
        prompt_js = ' + '.join('String(' + p.strip() + ')' if i % 2 else json.dumps(p, ensure_ascii=False) for i, p in enumerate(parts))
        schema = json.loads(by[CLAUDE_DECODE]['parameters']['inputSchema'])
        def api_schema(value):
            if isinstance(value, list):
                return [api_schema(v) for v in value]
            if isinstance(value, dict):
                return {k: api_schema(v) for k, v in value.items() if k not in {'minimum', 'maximum', 'minLength', 'maxLength', 'minItems', 'maxItems'}}
            return value
        body = {
            'model': 'claude-opus-5-5', 'max_tokens': 16384,
            'thinking': {'type': 'adaptive'},
            'output_config': {'effort': 'medium', 'format': {'type': 'json_schema', 'schema': api_schema(schema)}},
            'system': system + '\nSCHEMA COMPLET A RESPECTER (bornes incluses):\n' + json.dumps(schema, ensure_ascii=False),
        }
        body_js = json.dumps(body, ensure_ascii=False)[:-1] + ', "messages": [{"role":"user", "content": ' + prompt_js + '}]}'
        http = by['Anthropic Chat Model']
        http['name'] = 'Anthropic Messages - Opus5.5'
        http['type'] = 'n8n-nodes-base.httpRequest'
        http['typeVersion'] = 4.2
        http['position'] = [agent['position'][0] + 200, agent['position'][1]]
        http['parameters'] = {
            'method': 'POST', 'url': 'https://api.anthropic.com/v1/messages',
            'authentication': 'predefinedCredentialType', 'nodeCredentialType': 'anthropicApi',
            'sendHeaders': True, 'headerParameters': {'parameters': [{'name': 'anthropic-version', 'value': '2023-06-01'}]},
            'sendBody': True, 'specifyBody': 'json', 'jsonBody': '={{ $json }}',
            'options': {'timeout': 1500000},
        }
        http.update(retryOnFail=True, maxTries=2, waitBetweenTries=2000, onError='continueRegularOutput')
        agent['type'] = 'n8n-nodes-base.code'
        agent['typeVersion'] = 2
        agent['parameters'] = {'jsCode': 'return [{json: ' + body_js + '}];'}
        agent.pop('credentials', None)
        decoder = by[CLAUDE_DECODE]
        decoder['type'] = 'n8n-nodes-base.code'
        decoder['typeVersion'] = 2
        decoder['parameters'] = {'jsCode': 'const SCHEMA = ' + json.dumps(schema, ensure_ascii=False) + ';\n' + (ROOT / 'nodes/agent_input/claude_messages_decode.code.js').read_text(encoding='utf-8')}
        decoder['position'] = [agent['position'][0] + 400, agent['position'][1]]
        decoder.pop('onError', None)
        w['connections'].pop('Anthropic Chat Model', None)
        w['connections'][CLAUDE_AGENT] = {'main': [[{'node': http['name'], 'type': 'main', 'index': 0}]]}
        w['connections'][http['name']] = {'main': [[{'node': CLAUDE_DECODE, 'type': 'main', 'index': 0}]]}
        w['connections'][CLAUDE_DECODE] = {'main': [[{'node': 'Information Extractor2', 'type': 'main', 'index': 0}]]}
    # Stable modelKey values deliberately preserve ledger continuity.
    replacements = [('gpt-5.6-sol', 'gpt-6-sol'), ('GPT-5.6 Sol', 'GPT-6 Sol'), ('claude-opus-4-8', 'claude-opus-5-5'), ('Claude Opus 4.8', 'Claude Opus 5.5')]
    for name in ['Information Extractor', 'Information Extractor2', 'AG1.V4 — Build Consensus']:
        code = by[name]['parameters']['jsCode']
        for old, new in replacements:
            code = code.replace(old, new)
        by[name]['parameters']['jsCode'] = code
    return w
