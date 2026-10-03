import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'services/dashboard'))
from fundamental_context import fctx_card, FUNDAMENTAL_METHOD


class FundamentalContextTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 3, tzinfo=timezone.utc)
        self.row = dict(symbol='TEST', run_id='r1', score=80, status='OK', sector='Tech', fetched_at='2026-10-02T00:00:00Z',
                        updated_at='2026-10-02T00:00:00Z', strategy_version=FUNDAMENTAL_METHOD, target_price=110,
                        quality_score=80, growth_score=90, valuation_score=50, health_score=40, consensus_score=70,
                        analyst_count=5, source_url='https://finance.yahoo.com/quote/TEST')
        self.snapshot = dict(run_id='r1', growth_json={'earningsGrowth':2.94,'revenueGrowth':0}, profile_json={'currency':'USD'})

    def card(self, **kwargs):
        return fctx_card(kwargs.get('row',self.row), kwargs.get('snapshot',self.snapshot),
                         kwargs.get('consensus',{}), kwargs.get('history',[]), kwargs.get('peers',[]), self.now)

    def test_unknowns_and_units(self):
        card = self.card()
        self.assertEqual(card['metrics']['earnings_growth_pct'], 294)
        self.assertEqual(card['metrics']['revenue_growth_pct'], 0)
        self.assertIsNone(card['metrics']['forward_pe'])
        self.assertIsNone(card['accounting_period'])
        self.assertIsNone(card['published_at'])
        self.assertEqual(card['predictive_status'], 'NOT_VALIDATED')

    def test_wrong_run_is_not_mixed(self):
        card = self.card(snapshot=dict(self.snapshot,run_id='other'))
        self.assertIn('SNAPSHOT_UNAVAILABLE', card['flags'])
        self.assertIsNone(card['metrics']['earnings_growth_pct'])

    def test_stale_and_missing_source(self):
        card = self.card(row=dict(self.row,fetched_at='2026-06-01',status='ERR_SOURCE'))
        self.assertIn('STALE_OR_INVALID_DATE', card['flags'])
        self.assertIn('SOURCE_ERROR', card['flags'])

    def test_method_break_does_not_create_score_trend(self):
        history = [dict(updated_at='2026-09-02',target_price=100,score=70,strategy_version='old'),
                   dict(updated_at='2026-09-10',target_price=50,score=20,strategy_version=FUNDAMENTAL_METHOD)]
        card = self.card(history=history)
        self.assertEqual(card['changes']['30d']['target_revision_pct'],10)
        self.assertIsNone(card['changes']['30d']['score_delta'])
        self.assertIsNone(card['changes']['90d'])

    def test_targets_are_real_positive_and_same_run(self):
        self.assertEqual(self.card(consensus={'run_id':'other','target_mean_price':100})['analyst_targets'],{})
        card = self.card(consensus={'run_id':'r1','target_mean_price':100,'target_low_price':None,'target_high_price':0})
        self.assertEqual(card['analyst_targets'], {'low':None,'mean':100,'high':None})

    def test_sector_reference_requires_comparable_recent_sample(self):
        peers = [dict(self.row,symbol=str(i),score=60,data_coverage_pct=90) for i in range(8)]
        self.assertEqual(self.card(peers=peers)['sector_reference']['triage_percentile'],100)
        self.assertIsNone(self.card(peers=peers[:7])['sector_reference']['triage_percentile'])


if __name__ == '__main__':
    unittest.main()
