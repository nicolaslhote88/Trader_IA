import sys
from pathlib import Path
import unittest
import tempfile
import importlib
from unittest.mock import patch
import pandas as pd
import numpy as np

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE/'services/ag3-predictive'))
sys.path.insert(0,str(BASE/'services/yfinance-api'))
from normalization import sec_facts, esef_facts
from model import accounting_at, temporal_split
from research_history import serialize, register
from fastapi import FastAPI
from fastapi.testclient import TestClient


class PointInTimeTests(unittest.TestCase):
    def test_sec_preserves_null_revision_and_excludes_ytd(self):
        values=[dict(val=100,start='2020-01-01',end='2020-12-31',accn='old',filed='2021-02-01',form='10-K'),
                dict(val=150,start='2020-01-01',end='2020-12-31',accn='amend',filed='2022-02-01',form='10-K/A'),
                dict(val=80,start='2020-01-01',end='2020-09-30',accn='ytd',filed='2020-11-01',form='10-Q'),
                dict(val=None,start='2020-01-01',end='2020-12-31',accn='null',filed='2021-02-01',form='10-K')]
        rows=sec_facts('TEST',{'facts':{'us-gaap':{'Revenues':{'units':{'USD':values}}}}},
                       {'old':'2021-02-01T22:00:00Z'},'hash')
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0][7],'2021-02-01T22:10:00+00:00')
        self.assertEqual(rows[1][8],'FILED_DATE_ONLY')

    def test_esef_never_uses_catalogue_date_as_publication(self):
        payload={'facts':{'x':{'value':'100','dimensions':{'concept':'ifrs-full:Revenue',
                 'period':'2020-01-01T00:00:00/2021-01-01T00:00:00','unit':'iso4217:EUR'}}}}
        rows=esef_facts('TEST','filing',payload,'hash')
        self.assertEqual(rows[0][6],'2020-12-31')
        self.assertIsNone(rows[0][7])
        self.assertEqual(rows[0][8],'UNKNOWN_PUBLICATION')
        payload['facts']['x']['dimensions']['axis:Segment']='segment:Europe'
        self.assertEqual(esef_facts('TEST','filing',payload,'hash'),[])

    def test_future_amendment_cannot_change_past_features(self):
        metrics={'revenue':('Revenues',100),'net_income':('NetIncomeLoss',10),'assets':('Assets',200),
                 'equity':('StockholdersEquity',100),'cash':('CashAndCashEquivalentsAtCarryingValue',20),
                 'liabilities':('Liabilities',100),'operating_cashflow':('NetCashProvidedByUsedInOperatingActivities',30)}
        rows=[dict(metric=m,concept='us-gaap:'+tag,value=v,period_end='2020-12-31',unit='USD',
                   available_at=pd.Timestamp('2021-02-01',tz='UTC'),date_quality='SEC_ACCEPTED_PLUS_10M')
              for m,(tag,v) in metrics.items()]
        original=accounting_at(pd.DataFrame(rows),pd.Timestamp('2021-05-01',tz='UTC'))
        rows.append(dict(rows[1],value=80,available_at=pd.Timestamp('2021-08-01',tz='UTC')))
        before=accounting_at(pd.DataFrame(rows),pd.Timestamp('2021-05-01',tz='UTC'))
        after=accounting_at(pd.DataFrame(rows),pd.Timestamp('2021-09-01',tz='UTC'))
        self.assertEqual(original['margin'],before['margin'])
        self.assertEqual(before['margin'],.1)
        self.assertEqual(after['margin'],.8)

    def test_splits_purge_forward_labels_across_all_symbols(self):
        days=pd.date_range('2010-01-01',periods=120,freq='MS',tz='UTC')
        d=pd.DataFrame([dict(day=t,label_end=t+pd.Timedelta(days=90),symbol=s) for t in days for s in ['A','B']])
        a,b,c=temporal_split(d)
        self.assertLess(a.label_end.max(),b.day.min())
        self.assertLess(b.label_end.max(),c.day.min())
        self.assertFalse(set(a.day)&set(b.day))


class YahooContractTests(unittest.TestCase):
    def test_adjustments_and_missing_values_are_preserved(self):
        frame=pd.DataFrame([{'Close':100,'Adj Close':90,'Dividends':2,'Stock Splits':0,'Volume':np.nan}],
                           index=pd.to_datetime(['2020-01-02']))
        r=serialize(frame)[0]
        self.assertEqual(r['close'],100)
        self.assertEqual(r['adj_close'],90)
        self.assertEqual(r['dividends'],2)
        self.assertIsNone(r['volume'])

    def test_invalid_requests_do_not_reach_yahoo(self):
        app=FastAPI()
        def forbidden(*a,**kw):
            raise AssertionError('network must not be called')
        register(app,None,forbidden,forbidden,forbidden,forbidden)
        client=TestClient(app)
        self.assertEqual(client.get('/research/history',params={'symbol':'../secret'}).status_code,422)
        self.assertEqual(client.get('/research/history',params={'symbol':'AAPL','start':'bad'}).status_code,422)

    def test_cache_bypasses_provider_and_returns_contract(self):
        app=FastAPI()
        def forbidden():
            raise AssertionError('rate gate must not be called on cache hit')
        register(app,None,lambda *a:'cache',lambda *a:{'ok':True,'contract':'YF_RESEARCH_HISTORY_V1','end_exclusive':'2020-01-02'},None,forbidden)
        result=TestClient(app).get('/research/history',params={'symbol':'AAPL','end':'2020-01-02'}).json()
        self.assertTrue(result['cache_hit'])


class ModelLifecycleTests(unittest.TestCase):
    def test_training_evaluation_outputs_remain_shadow(self):
        import model
        import json
        rng=np.random.default_rng(10)
        records=[]
        for day in pd.date_range('2010-01-01',periods=120,freq='MS',tz='UTC'):
            for symbol in range(12):
                row=dict(symbol=str(symbol),cik=str(symbol),day=day,label_end=day+pd.Timedelta(days=90),target=int(rng.integers(2)))
                row.update({f:float(rng.normal()) for f in model.FEATURES})
                records.append(row)
        data=pd.DataFrame(records)
        latest=data.tail(12).copy()
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(model,'ROOT',Path(folder)),patch.object(model,'build_dataset',return_value=(data,latest)):
                report=model.train()
            self.assertFalse(report['decision_enabled'])
            self.assertFalse(report['validated'])
            self.assertEqual(set(report['metrics']),{'frequency','constant_50pct','logistic','gradient_boosting'})
            self.assertTrue((Path(folder)/'model_artifacts.joblib').exists())
            result=json.loads((Path(folder)/'predictions.json').read_text())
            self.assertEqual(len(result['predictions']),12)
            self.assertFalse(result['decision_enabled'])

    def test_changed_adjustment_forces_full_refresh(self):
        import store
        import collectors
        from unittest.mock import Mock
        from datetime import date,timedelta
        day=(date.today()-timedelta(days=2)).isoformat()
        row={'date':day,'close':100,'adj_close':50,'volume':10,'dividends':0,'splits':2}
        payload={'ok':True,'contract':'YF_RESEARCH_HISTORY_V1','currency':'USD','fetched_at':store.now(),'bars':[row]}
        client=Mock()
        client.get.return_value=(payload,'test_digest')
        with tempfile.TemporaryDirectory() as folder,patch.object(store,'ROOT',Path(folder)):
            store.initialize()
            with store.db() as c:
                c.execute('INSERT INTO prices VALUES(?,?,?,?,?,?,?,?,?,?)',('TEST',day,100,100,10,0,0,'USD',store.now(),'old'))
            collectors.collect_prices(client,{'symbol':'TEST','yahoo':'TEST'})
            self.assertEqual(client.get.call_count,2)
            self.assertEqual(client.get.call_args.args[2]['start'],'2010-01-01')
            with store.db(True) as c:
                self.assertEqual(c.execute('SELECT adj_close FROM prices').fetchone()[0],50)

    def test_new_invalid_price_cannot_leave_an_old_value_in_training(self):
        import store
        import collectors
        from unittest.mock import Mock
        rows=[dict(date='2020-01-02',close=100,adj_close=None),
              dict(date='2020-01-03',close=101,adj_close=101)]
        payload={'ok':True,'contract':'YF_RESEARCH_HISTORY_V1','currency':'USD','fetched_at':store.now(),'bars':rows}
        client=Mock()
        client.get.return_value=(payload,'test_digest')
        with tempfile.TemporaryDirectory() as folder,patch.object(store,'ROOT',Path(folder)):
            store.initialize()
            with store.db() as c:
                c.execute('INSERT INTO prices VALUES(?,?,?,?,?,?,?,?,?,?)',('TEST','2020-01-02',100,100,10,0,0,'USD','2020-01-04','old'))
            collectors.collect_prices(client,{'symbol':'TEST','yahoo':'TEST'})
            with store.db(True) as c:
                self.assertEqual(c.execute('SELECT day FROM prices').fetchone()[0],'2020-01-03')
                self.assertEqual(c.execute('SELECT status FROM coverage').fetchone()[0],'PARTIAL')


if __name__=='__main__':
    unittest.main()
