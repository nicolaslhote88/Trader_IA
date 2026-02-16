#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

DIR = Path(__file__).resolve().parent
NODES_DIR = DIR / 'nodes'


def load_code(filename: str) -> str:
    return (NODES_DIR / filename).read_text(encoding='utf-8')


def build() -> dict:
    nodes = [
        {
            'parameters': {
                'rule': {
                    'interval': [
                        {'field': 'cronExpression', 'expression': '*/30 7-20 * * 1-5'}
                    ]
                }
            },
            'type': 'n8n-nodes-base.scheduleTrigger',
            'typeVersion': 1.3,
            'position': [-1488, -192],
            'id': 'cecb88f6-8ab2-41f7-a911-77604cba6d3d',
            'name': 'Schedule Trigger',
        },
        {
            'parameters': {},
            'type': 'n8n-nodes-base.manualTrigger',
            'typeVersion': 1,
            'position': [-1488, -16],
            'id': 'c893012c-bf40-45b9-89d3-507207a9c8f4',
            'name': 'Manual Trigger',
        },
        {
            'parameters': {
                'documentId': {
                    '__rl': True,
                    'mode': 'list',
                    'value': '1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I',
                    'cachedResultName': 'TradingSim_GoogleSheet_Template',
                    'cachedResultUrl': 'https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit?usp=drivesdk',
                },
                'sheetName': {
                    '__rl': True,
                    'mode': 'list',
                    'value': 1628829420,
                    'cachedResultName': 'Source_RSS',
                    'cachedResultUrl': 'https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit#gid=1628829420',
                },
                'options': {},
            },
            'type': 'n8n-nodes-base.googleSheets',
            'typeVersion': 4.7,
            'position': [-1280, -96],
            'id': '4ba89389-1e3d-4730-b1da-9a9fbf9cc7b6',
            'name': '20A - Load RSS Sources',
            'credentials': {
                'googleSheetsOAuth2Api': {'id': 'aX5iAQEN9HK4UGjr', 'name': 'Google Sheets account'}
            },
        },
        {
            'parameters': {
                'documentId': {
                    '__rl': True,
                    'mode': 'list',
                    'value': '1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I',
                    'cachedResultName': 'TradingSim_GoogleSheet_Template',
                    'cachedResultUrl': 'https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit?usp=drivesdk',
                },
                'sheetName': {
                    '__rl': True,
                    'mode': 'list',
                    'value': 1078848687,
                    'cachedResultName': 'Universe',
                    'cachedResultUrl': 'https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit#gid=1078848687',
                },
                'options': {},
            },
            'type': 'n8n-nodes-base.googleSheets',
            'typeVersion': 4.7,
            'position': [-640, -256],
            'id': 'a24cf6f4-56c5-47d3-a63b-d151e2f0f001',
            'name': '20A1 - Load Universe',
            'credentials': {
                'googleSheetsOAuth2Api': {'id': 'aX5iAQEN9HK4UGjr', 'name': 'Google Sheets account'}
            },
        },
        {
            'parameters': {'jsCode': load_code('00_build_symbol_directory.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-416, -256],
            'id': 'aa3b664f-c1c6-4f42-b8be-4dc52f427077',
            'name': '20A2 - Build Symbol Directory',
        },
        {
            'parameters': {'jsCode': load_code('01_normalize_sources.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-1088, -96],
            'id': '1c30e7d0-8e02-4705-a111-e7f7a4376395',
            'name': '20B - Normalize RSS Sources',
        },
        {
            'parameters': {
                'conditions': {
                    'options': {'caseSensitive': True, 'leftValue': '', 'typeValidation': 'strict', 'version': 3},
                    'conditions': [
                        {
                            'id': 'enabled',
                            'leftValue': '={{$json.enabled}}',
                            'rightValue': '',
                            'operator': {'type': 'boolean', 'operation': 'true', 'singleValue': True},
                        }
                    ],
                    'combinator': 'and',
                },
                'options': {},
            },
            'type': 'n8n-nodes-base.if',
            'typeVersion': 2.3,
            'position': [-896, -96],
            'id': '8df4f5f5-36fa-4d53-83ae-a8787aff1050',
            'name': '20C - Filter enabled',
            'alwaysOutputData': True,
        },
        {
            'parameters': {'language': 'pythonNative', 'pythonCode': load_code('12_duckdb_init.py')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-720, -96],
            'id': '21d42ea6-9ea8-4f33-a6de-8d73adfdbdd9',
            'name': '20DB0 - DuckDB Init',
        },
        {
            'parameters': {'language': 'pythonNative', 'pythonCode': load_code('13_read_history_index.py')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-448, -192],
            'id': 'fc870215-a330-495c-bd7f-33987e6e5182',
            'name': '20G1B - Build History Index',
        },
        {
            'parameters': {'options': {'reset': False}},
            'type': 'n8n-nodes-base.splitInBatches',
            'typeVersion': 3,
            'position': [-560, -8],
            'id': '24468adc-03c7-42bf-99da-a0fee17a71a8',
            'name': '20D - Split Feeds',
            'onError': 'continueRegularOutput',
        },
        {
            'parameters': {'url': '={{$json.url}}', 'options': {}},
            'type': 'n8n-nodes-base.rssFeedRead',
            'typeVersion': 1.2,
            'position': [-352, 48],
            'id': '846f2fa1-e726-4c86-931c-2051f96912e3',
            'name': '20E - RSS Feed Read',
            'retryOnFail': True,
            'maxTries': 2,
            'onError': 'continueErrorOutput',
        },
        {
            'parameters': {'jsCode': load_code('02_build_error_log.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-144, 336],
            'id': 'ba81bc82-1ede-42eb-93e4-f74e6d7c4b1b',
            'name': '20E_ERR - Build Error Log',
        },
        {
            'parameters': {'language': 'pythonNative', 'pythonCode': load_code('15_write_errors_duckdb.py')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [64, 336],
            'id': '4672d63d-ceaf-49c8-b588-db1f70ae59cb',
            'name': '20E_ERR_DB - Write Errors DuckDB',
        },
        {
            'parameters': {'jsCode': load_code('03_normalize_rss_items.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-96, 48],
            'id': 'a3df8699-8179-4c02-9f6b-bf5b19527669',
            'name': '20F - Normalize RSS Items',
        },
        {
            'parameters': {'options': {'reset': '={{ $json._itemsLoopReset }}'}},
            'type': 'n8n-nodes-base.splitInBatches',
            'typeVersion': 3,
            'position': [144, 48],
            'id': '2ff1174a-2a77-4119-a67e-5f09acae90e6',
            'name': 'SplitInBatches ITEMS',
        },
        {
            'parameters': {'jsCode': load_code('05_add_keys.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [352, 48],
            'id': 'd0aad922-a90f-4af3-9743-a72bf2499121',
            'name': '20G0 - Add Keys',
        },
        {
            'parameters': {
                'conditions': {
                    'options': {'caseSensitive': True, 'leftValue': '', 'typeValidation': 'loose', 'version': 3},
                    'conditions': [
                        {
                            'id': 'title-ok',
                            'leftValue': '={{ $json.title }}',
                            'rightValue': 'unknown',
                            'operator': {'type': 'string', 'operation': 'notEquals'},
                        },
                        {
                            'id': 'valid-flag',
                            'leftValue': '={{ $json._isValid }}',
                            'rightValue': True,
                            'operator': {'type': 'boolean', 'operation': 'equals'},
                        },
                    ],
                    'combinator': 'and',
                },
                'looseTypeValidation': True,
                'options': {},
            },
            'type': 'n8n-nodes-base.if',
            'typeVersion': 2.3,
            'position': [560, 48],
            'id': '66b0ae2b-1d3d-488c-b9e5-c2e480776d37',
            'name': '20G_FILTER - Block Bad Items',
        },
        {
            'parameters': {'jsCode': load_code('04_tag_symbols.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [752, 48],
            'id': '2c32056e-567a-4676-a836-fcb4cf6994f0',
            'name': '20F1 - Tag Symbols',
        },
        {
            'parameters': {'jsCode': load_code('07_prescore.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [960, 48],
            'id': '3b15efdf-7732-4a09-a6be-fe79fddabdb8',
            'name': '20G1C - Pre-score',
        },
        {
            'parameters': {'jsCode': load_code('08_route_new_seen.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [1168, 48],
            'id': '707a3b8c-e2da-4da4-946f-60588bb9f2be',
            'name': '20G2 - Route new vs seen',
        },
        {
            'parameters': {
                'rules': {
                    'values': [
                        {
                            'conditions': {
                                'options': {'caseSensitive': True, 'leftValue': '', 'typeValidation': 'loose', 'version': 3},
                                'conditions': [
                                    {
                                        'id': 'analyze',
                                        'leftValue': '={{$json._action}}',
                                        'rightValue': 'analyze',
                                        'operator': {'type': 'string', 'operation': 'equals'},
                                    }
                                ],
                                'combinator': 'and',
                            }
                        },
                        {
                            'conditions': {
                                'options': {'caseSensitive': True, 'leftValue': '', 'typeValidation': 'loose', 'version': 3},
                                'conditions': [
                                    {
                                        'id': 'skip',
                                        'leftValue': '={{$json._action}}',
                                        'rightValue': 'skip',
                                        'operator': {'type': 'string', 'operation': 'equals'},
                                    }
                                ],
                                'combinator': 'and',
                            }
                        },
                    ]
                },
                'looseTypeValidation': True,
                'options': {},
            },
            'type': 'n8n-nodes-base.switch',
            'typeVersion': 3.4,
            'position': [1376, 48],
            'id': 'eeff816d-44d1-4679-91c0-4e8c02e96cdd',
            'name': '20G3 - Router analyze vs skip',
        },
        {
            'parameters': {'jsCode': load_code('09_prepare_llm_input.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [1568, -32],
            'id': '8d540e4e-0738-4405-a5e3-33a6f06f9d63',
            'name': '20H0 - Prepare LLM Input',
        },
        {
            'parameters': {
                'modelId': {'__rl': True, 'mode': 'list', 'value': 'gpt-5-mini', 'cachedResultName': 'GPT-5-MINI'},
                'responses': {
                    'values': [
                        {
                            'role': 'system',
                            'content': (
                                'Tu es le Chief Market Strategist. Tu analyses une news pour decision portefeuille. '
                                'Objectif: extraire regime de marche, theme macro, secteurs gagnants/perdants, resume actionnable. '
                                'Reponds uniquement en JSON valide selon le schema.'
                            ),
                        },
                        {
                            'content': '=Analyse cette news:\n{{ $json.llmInput }}'
                        },
                    ]
                },
                'builtInTools': {},
                'options': {
                    'textFormat': {
                        'textOptions': {
                            'type': 'json_schema',
                            'name': 'market_news_normalizer_v2',
                            'schema': (
                                '{'
                                '"type":"object",'
                                '"additionalProperties":false,'
                                '"properties":{'
                                '"market_regime":{"type":"string","enum":["Risk-On","Risk-Off","Neutral","Sector Rotation"]},'
                                '"macro_theme":{"type":"string","enum":["Inflation/Taux","Croissance/Recession","Geopolitique/Energie","Tech/AI","Resultats/Micro"]},'
                                '"sectors_bullish":{"type":"array","items":{"type":"string"}},'
                                '"sectors_bearish":{"type":"array","items":{"type":"string"}},'
                                '"strategic_summary":{"type":"string"},'
                                '"impact_score":{"type":"integer","minimum":0,"maximum":10},'
                                '"confidence":{"type":"number","minimum":0,"maximum":1},'
                                '"urgency":{"type":"string","enum":["immediate","today","this_week","low"]},'
                                '"notes":{"type":"string"}'
                                '},'
                                '"required":["market_regime","macro_theme","sectors_bullish","sectors_bearish","strategic_summary","impact_score","confidence","urgency","notes"]'
                                '}'
                            ),
                            'strict': True,
                        }
                    }
                },
            },
            'type': '@n8n/n8n-nodes-langchain.openAi',
            'typeVersion': 2.1,
            'position': [1792, -32],
            'id': 'f1245660-74e3-48cf-867a-44d06f800d05',
            'name': '20H1 - Analyze with OpenAI',
            'credentials': {'openAiApi': {'id': 'rILpYjTayqc4jXXZ', 'name': 'OpenAi account'}},
        },
        {
            'parameters': {'mode': 'combine', 'combineBy': 'combineByPosition', 'options': {}},
            'type': 'n8n-nodes-base.merge',
            'typeVersion': 3.2,
            'position': [2016, -32],
            'id': '1de98e3a-c7c6-4f9b-be88-f4ac530f5058',
            'name': '20H1B - Merge AI + Context',
        },
        {
            'parameters': {'jsCode': load_code('10_parse_llm_output.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [2240, -32],
            'id': 'fb13db3f-6b5e-49b9-ac4a-1bb40745de8f',
            'name': '20H2 - Parse AI Output',
        },
        {
            'parameters': {'jsCode': load_code('11_build_skip_row.js')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [1568, 160],
            'id': 'e68c8302-5a5e-4cd2-beee-04db64cb2b6a',
            'name': '20S1 - Build Skip Row',
        },
        {
            'parameters': {'mode': 'append', 'options': {}},
            'type': 'n8n-nodes-base.merge',
            'typeVersion': 3.2,
            'position': [2464, 64],
            'id': '1d7d61df-2326-443e-aec7-45ca7987a8b9',
            'name': '20Z - Merge analyzed + skipped',
        },
        {
            'parameters': {'language': 'pythonNative', 'pythonCode': load_code('14_write_news_duckdb.py')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [2688, 64],
            'id': '879d4bc5-2f7f-48ed-9f68-b9c8c761ef8e',
            'name': '20DBW - Upsert News DuckDB',
        },
        {
            'parameters': {'language': 'pythonNative', 'pythonCode': load_code('16_finalize_run.py')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-320, -112],
            'id': '36e3cbb6-3ec7-4606-8c06-743fabdf0844',
            'name': '20R1 - Finalize Run',
        },
        {
            'parameters': {'language': 'pythonNative', 'pythonCode': load_code('17_sync_sheets.py')},
            'type': 'n8n-nodes-base.code',
            'typeVersion': 2,
            'position': [-96, -112],
            'id': 'df4381f4-24a6-4f40-9bc4-d76f9c700d57',
            'name': '20R2 - Sync DuckDB to Rows',
        },
        {
            'parameters': {
                'operation': 'appendOrUpdate',
                'documentId': {
                    '__rl': True,
                    'mode': 'list',
                    'value': '1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I',
                    'cachedResultName': 'TradingSim_GoogleSheet_Template',
                    'cachedResultUrl': 'https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit?usp=drivesdk',
                },
                'sheetName': {
                    '__rl': True,
                    'mode': 'list',
                    'value': 1351236563,
                    'cachedResultName': 'News_History',
                    'cachedResultUrl': 'https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit#gid=1351236563',
                },
                'columns': {
                    'mappingMode': 'autoMapInputData',
                    'matchingColumns': ['dedupeKey'],
                    'attemptToConvertTypes': False,
                    'convertFieldsToString': False,
                },
                'options': {'handlingExtraData': 'ignoreIt'},
            },
            'type': 'n8n-nodes-base.googleSheets',
            'typeVersion': 4.7,
            'position': [128, -112],
            'id': '1bb3dca8-cca8-4b64-9972-58de2f461628',
            'name': '20G5 - Append News_History',
            'credentials': {
                'googleSheetsOAuth2Api': {'id': 'aX5iAQEN9HK4UGjr', 'name': 'Google Sheets account'}
            },
            'onError': 'continueRegularOutput',
        },
        {
            'parameters': {
                'content': 'AG4-V2 News Watcher: DuckDB on VPS is source of truth. Google Sheets is sync output for human control.',
                'height': 220,
                'width': 1420,
                'color': 5,
            },
            'type': 'n8n-nodes-base.stickyNote',
            'typeVersion': 1,
            'position': [-1460, -320],
            'id': 'f463e018-d888-4866-a579-7e73a768018a',
            'name': 'Note AG4-V2',
        },
    ]

    connections = {
        'Schedule Trigger': {'main': [[{'node': '20A - Load RSS Sources', 'type': 'main', 'index': 0}]]},
        'Manual Trigger': {'main': [[{'node': '20A - Load RSS Sources', 'type': 'main', 'index': 0}]]},
        '20A - Load RSS Sources': {'main': [[{'node': '20B - Normalize RSS Sources', 'type': 'main', 'index': 0}]]},
        '20B - Normalize RSS Sources': {'main': [[{'node': '20C - Filter enabled', 'type': 'main', 'index': 0}]]},
        '20C - Filter enabled': {
            'main': [
                [
                    {'node': '20DB0 - DuckDB Init', 'type': 'main', 'index': 0},
                    {'node': '20A1 - Load Universe', 'type': 'main', 'index': 0},
                ],
                [],
            ]
        },
        '20A1 - Load Universe': {'main': [[{'node': '20A2 - Build Symbol Directory', 'type': 'main', 'index': 0}]]},
        '20DB0 - DuckDB Init': {
            'main': [
                [
                    {'node': '20D - Split Feeds', 'type': 'main', 'index': 0},
                    {'node': '20G1B - Build History Index', 'type': 'main', 'index': 0},
                ]
            ]
        },
        '20D - Split Feeds': {
            'main': [
                [{'node': '20R1 - Finalize Run', 'type': 'main', 'index': 0}],
                [{'node': '20E - RSS Feed Read', 'type': 'main', 'index': 0}],
            ]
        },
        '20E - RSS Feed Read': {
            'main': [
                [{'node': '20F - Normalize RSS Items', 'type': 'main', 'index': 0}],
                [{'node': '20E_ERR - Build Error Log', 'type': 'main', 'index': 0}],
            ]
        },
        '20E_ERR - Build Error Log': {'main': [[{'node': '20E_ERR_DB - Write Errors DuckDB', 'type': 'main', 'index': 0}]]},
        '20E_ERR_DB - Write Errors DuckDB': {'main': [[{'node': '20D - Split Feeds', 'type': 'main', 'index': 0}]]},
        '20F - Normalize RSS Items': {'main': [[{'node': 'SplitInBatches ITEMS', 'type': 'main', 'index': 0}]]},
        'SplitInBatches ITEMS': {
            'main': [
                [{'node': '20D - Split Feeds', 'type': 'main', 'index': 0}],
                [{'node': '20G0 - Add Keys', 'type': 'main', 'index': 0}],
            ]
        },
        '20G0 - Add Keys': {'main': [[{'node': '20G_FILTER - Block Bad Items', 'type': 'main', 'index': 0}]]},
        '20G_FILTER - Block Bad Items': {
            'main': [
                [{'node': '20F1 - Tag Symbols', 'type': 'main', 'index': 0}],
                [{'node': 'SplitInBatches ITEMS', 'type': 'main', 'index': 0}],
            ]
        },
        '20F1 - Tag Symbols': {'main': [[{'node': '20G1C - Pre-score', 'type': 'main', 'index': 0}]]},
        '20G1C - Pre-score': {'main': [[{'node': '20G2 - Route new vs seen', 'type': 'main', 'index': 0}]]},
        '20G2 - Route new vs seen': {'main': [[{'node': '20G3 - Router analyze vs skip', 'type': 'main', 'index': 0}]]},
        '20G3 - Router analyze vs skip': {
            'main': [
                [{'node': '20H0 - Prepare LLM Input', 'type': 'main', 'index': 0}],
                [{'node': '20S1 - Build Skip Row', 'type': 'main', 'index': 0}],
            ]
        },
        '20H0 - Prepare LLM Input': {
            'main': [
                [
                    {'node': '20H1 - Analyze with OpenAI', 'type': 'main', 'index': 0},
                    {'node': '20H1B - Merge AI + Context', 'type': 'main', 'index': 0},
                ]
            ]
        },
        '20H1 - Analyze with OpenAI': {'main': [[{'node': '20H1B - Merge AI + Context', 'type': 'main', 'index': 1}]]},
        '20H1B - Merge AI + Context': {'main': [[{'node': '20H2 - Parse AI Output', 'type': 'main', 'index': 0}]]},
        '20H2 - Parse AI Output': {'main': [[{'node': '20Z - Merge analyzed + skipped', 'type': 'main', 'index': 0}]]},
        '20S1 - Build Skip Row': {'main': [[{'node': '20Z - Merge analyzed + skipped', 'type': 'main', 'index': 1}]]},
        '20Z - Merge analyzed + skipped': {'main': [[{'node': '20DBW - Upsert News DuckDB', 'type': 'main', 'index': 0}]]},
        '20DBW - Upsert News DuckDB': {'main': [[{'node': 'SplitInBatches ITEMS', 'type': 'main', 'index': 0}]]},
        '20R1 - Finalize Run': {'main': [[{'node': '20R2 - Sync DuckDB to Rows', 'type': 'main', 'index': 0}]]},
        '20R2 - Sync DuckDB to Rows': {'main': [[{'node': '20G5 - Append News_History', 'type': 'main', 'index': 0}]]},
    }

    return {
        'name': 'AG4-V2 - News Watcher',
        'nodes': nodes,
        'connections': connections,
        'pinData': {},
        'meta': {'instanceId': '093ba1266e85fcaaa6abca411d9f7941951737b3920bfaa6e2db89226df2c82d'},
    }


def main() -> None:
    wf = build()
    out = DIR / 'AG4-V2-workflow.json'
    out.write_text(json.dumps(wf, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f'Wrote {out}')


if __name__ == '__main__':
    main()
