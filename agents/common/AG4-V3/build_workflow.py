#!/usr/bin/env python3
"""Rebuild from the verified graph; never resurrect the historical Grok/Forex generator."""
import json
from pathlib import Path
DIR=Path(__file__).resolve().parent
PATCHES={
 '20F - Normalize RSS Items': '03_normalize_rss_items.js',
 '20G2 - Route new vs seen': '08_route_new_seen.js',
 '20H0 - Prepare LLM Input': '09_prepare_llm_input.js',
 '20H2R - Parse DeepSeek Output': '10b_parse_llm_output_reduced.js',
 '20G1B - Build History Index': '13_read_history_index.py',
}
def build():
    wf=json.loads((DIR/'AG4-V3-workflow.json').read_text(encoding='utf-8'))
    assert any(n['type'].endswith('lmChatDeepSeek') for n in wf['nodes'])
    for node in wf['nodes']:
        if node['name'] in PATCHES:
            path=DIR/'nodes'/PATCHES[node['name']]
            key='pythonCode' if path.suffix=='.py' else 'jsCode'
            node['parameters'][key]=path.read_text(encoding='utf-8-sig')
    return wf
if __name__=='__main__':
    (DIR/'AG4-V3-workflow.json').write_text(json.dumps(build(),ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
