import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'services/ag3-predictive'))
sys.path.insert(0,str(ROOT/'outils/scripts'))
from api import app
from historical_evidence import card
from build_ag1_history_live import build, PRE, FETCH, ATTACH


class LiveHistoryTests(unittest.TestCase):
    def test_batch_is_read_only_and_excludes_prediction_payload(self):
        cards={'X':card('X',[],'2020-01-01T00:00:00Z')}
        with patch('api.load_cards',return_value=cards) as load:
            result=TestClient(app).post('/ag1/historical-evidence',json={'symbols':['X'],'as_of':'2020-01-01T00:00:00Z'})
        self.assertEqual(result.status_code,200)
        self.assertTrue(result.json()['advisory_only'])
        self.assertFalse(result.json()['predictive_probabilities_supplied'])
        self.assertNotIn('predictions',result.json())
        load.assert_called_once()

    def test_bad_dates_and_excessive_universe_do_not_read_data(self):
        with patch('api.load_cards') as load:
            for body in [{'symbols':['X'],'as_of':'2020-01-01'},
                         {'symbols':['X'],'as_of':'2099-01-01T00:00:00Z'},
                         {'symbols':['X']*101,'as_of':'2020-01-01T00:00:00Z'}]:
                self.assertEqual(TestClient(app).post('/ag1/historical-evidence',json=body).status_code,422)
            load.assert_not_called()

    def test_builder_preserves_all_existing_nodes_and_consensus_input(self):
        targets=['Agent #1 - Portfolio manager','Agent #1 - Portfolio manager1','Agent #1 - Portfolio manager2','AG1.V4 — Merge Model Proposals']
        old={'id':'AG1V4CONSENSUS','name':'test','nodes':[{'name':PRE,'position':[0,0],'parameters':{'untouched':True}}],
             'settings':{'timezone':'Europe/Paris'},'connections':{PRE:{'main':[[{'node':t,'type':'main','index':0} for t in targets]]}}}
        w=build(old)
        self.assertEqual(w['nodes'][:-2],old['nodes'])
        self.assertEqual(w['settings'],old['settings'])
        self.assertEqual(w['connections'][ATTACH],old['connections'][PRE])
        self.assertEqual(w['nodes'][-2]['parameters']['options']['timeout'],8000)
        self.assertEqual(w['nodes'][-2]['onError'],'continueRegularOutput')


if __name__=='__main__':unittest.main()
