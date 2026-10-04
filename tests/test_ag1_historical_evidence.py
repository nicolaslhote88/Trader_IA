import copy
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'services/ag3-predictive'))
sys.path.insert(0,str(ROOT/'outils/scripts'))
from historical_evidence import card, enrich_context, timestamp
from build_ag1_history_shadow import build, KEEP


def fact(metric,value,end='2024-12-31',available='2025-02-01T12:00:00+00:00',**kw):
    from normalization import TAGS
    f=dict(metric=metric,value=value,period_end=end,available_at=available,source='sec',
           date_quality='SEC_ACCEPTED_PLUS_10M',unit='USD',filing_id='original',raw_hash='a'*64,
           concept='us-gaap:'+TAGS[metric][0])
    return dict(f,**kw)


class HistoricalEvidenceTests(unittest.TestCase):
    def test_future_revision_and_undated_are_excluded(self):
        f=[fact('revenue',100),fact('net_income',10),
           fact('net_income',80,available='2026-01-01T00:00:00+00:00',filing_id='revision'),
           fact('assets',500,date_quality='UNKNOWN_PUBLICATION')]
        c=card('X',f,'2025-06-01T00:00:00+00:00')
        self.assertEqual(c['periods'][0]['ratios_pct']['net_margin'],10)
        self.assertNotIn('assets',c['periods'][0]['amounts_million'])
        self.assertEqual(c['periods'][0]['provenance']['net_income']['filing_id'],'original')

    def test_margin_changes_are_points_and_liabilities_not_debt(self):
        f=[]
        for end,revenue,income,ocf,capex in [('2024-12-31',200e6,30e6,50e6,20e6),('2023-12-31',100e6,10e6,20e6,10e6)]:
            f.extend(fact(k,v,end=end) for k,v in [('revenue',revenue),('net_income',income),('operating_cashflow',ocf),('capex',capex),('assets',400e6),('liabilities',250e6)])
        c=card('X',f,'2025-06-01T00:00:00+00:00',sector='Financial Services')
        self.assertEqual(c['changes']['ratios_pp']['net_margin'],5)
        self.assertEqual(c['changes']['fcf_proxy_delta_million'],20)
        self.assertEqual(c['periods'][0]['ratios_pct']['liabilities_assets'],62.5)
        self.assertIn('FINANCIAL_SECTOR_RATIOS_NOT_INDUSTRIAL_LEVERAGE',c['flags'])

    def test_no_currency_mix_or_fabricated_cashflow(self):
        c=card('X',[fact('revenue',100),fact('net_income',10,unit='EUR'),fact('operating_cashflow',20)],'2025-06-01T00:00:00Z')
        p=c['periods'][0]
        self.assertIsNone(p['ratios_pct']['net_margin'])
        self.assertIsNone(p['fcf_proxy_million'])
        self.assertIsNone(c['changes'])

    def test_timezone_required_and_stale_explicit(self):
        with self.assertRaises(ValueError): timestamp('2025-01-01')
        c=card('X',[fact('revenue',100)],'2026-10-04T00:00:00Z')
        self.assertEqual(c['status'],'STALE_ANNUAL_ACCOUNTS')

    def test_enrichment_preserves_existing_card_scores_and_input(self):
        context={'opportunity_pack':{'rows':[{'symbol':'X','score':70,'fundamentals':{'schema':'AG3_EVIDENCE_V1','metrics':{'net_margin_pct':3}}}]},'portfolio_pack':{'positions':[{'symbol':'Y'}]}}
        before=copy.deepcopy(context)
        cards={s:card(s,[],'2025-06-01T00:00:00Z') for s in ['X','Y']}
        enriched=enrich_context(context,cards)
        self.assertEqual(context,before)
        self.assertEqual(enriched['opportunity_pack']['rows'][0]['fundamentals']['metrics'],{'net_margin_pct':3})
        self.assertTrue(enriched['opportunity_pack']['held_historical_accounts']['Y']['advisory_only'])
        self.assertEqual(enrich_context(enriched,cards),enriched)

    def test_no_comparison_over_missing_year(self):
        f=[fact('revenue',100),fact('revenue',50,end='2022-12-31')]
        c=card('X',f,'2025-06-01T00:00:00Z')
        self.assertIsNone(c['changes'])

    def test_shadow_graph_preserves_models_and_excludes_execution(self):
        nodes=[{'name':n,'type':'test','parameters':{'url':'https://api.anthropic.com/v1/messages'} if n=='Anthropic Messages - Opus5.5' else {'options':{'sentinel':True}}} for n in KEEP]
        nodes.append({'name':'07b - IBKR Send Orders','type':'http','parameters':{}})
        published={'nodes':nodes,'connections':{'Information Extractor':{'main':[[{'node':'07b - IBKR Send Orders','index':0,'type':'main'}]]}}}
        result=build(published,{'opportunity_pack':{'rows':[]}},'test')
        self.assertFalse(result['active'])
        self.assertEqual([n for n in result['nodes'] if n['name'] in KEEP],nodes[:-1])
        self.assertNotIn('07b - IBKR Send Orders',str(result))
        self.assertNotIn('scheduleTrigger',str(result))

    def test_unexpected_http_destination_blocks_replay(self):
        nodes=[{'name':n,'parameters':{'url':'https://invalid.example'}} for n in KEEP]
        with self.assertRaises(AssertionError):build({'nodes':nodes,'connections':{}},{},'test')


if __name__=='__main__':
    unittest.main()
