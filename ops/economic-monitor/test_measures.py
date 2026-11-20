from datetime import datetime,timezone
from unittest import TestCase
from costs import tariff,quality,tax_estimate,costs
import sqlite3
import json
from performance import prices_eur,last,relative,curves
from allocation import attach
from usage import price

class Measures(TestCase):
    def test_tariff_minimum_and_market(self):
        self.assertEqual(tariff("SBF","EUR",10,490,1),3)
        self.assertEqual(tariff("NYSE","USD",2,500,.9),.9)
        self.assertIsNone(tariff("LSE","GBP",2,500,1.2))
        self.assertEqual(tax_estimate("BNP.PA","BUY","2026-11-19","FR",1000),4)
    def test_delayed_stale_and_future(self):
        t=datetime(2026,6,11,tzinfo=timezone.utc)
        q=dict(quoteTime=t.isoformat(),ibkrDataAvailability="D",spreadObserved=True,bid=10,ask=11)
        self.assertFalse(quality(q,t.timestamp()))
        q["ibkrDataAvailability"]="R"
        self.assertTrue(quality(q,t.timestamp()+1));self.assertFalse(quality(q,t.timestamp()+61));self.assertFalse(quality(q,t.timestamp()-1))
    def test_holiday_and_currency_gap(self):
        self.assertEqual(last({"2026-07-03":12},"2026-07-04",4),12)
        self.assertIsNone(prices_eur({"2026-07-10":100},"USD",{"2026-07-03":1.1},"2026-07-10"))
    def test_deposit_does_not_create_return_and_exposure_is_lagged(self):
        dates=["2026-06-11","2026-06-12","2026-06-15"]
        pa=dict(zip(dates,[(1000,0),(2000,0),(2100,.05)]))
        series=curves(pa,dict(zip(dates,[10,20,22])),{},dict(zip(dates,[[1000,1000],[0,2000],[0,2100]])),{}, {},1,dates[-1])
        self.assertEqual(series[1]["account"],0)
        self.assertEqual(series[1]["matched"],0);self.assertAlmostEqual(series[2]["matched"],.1)
        self.assertAlmostEqual(relative(0,series[2]["account"]),.05)
    def test_partial_execution_with_prospective_quote_before_flex(self):
        db=sqlite3.connect(":memory:")
        db.executescript("create table sends(eid,orderid,body);create table packs(eid,contract,body);create table quotes(contract,observed,stamp,bid,ask,availability)")
        sent="2026-11-25T15:00:00+00:00";at=datetime.fromisoformat(sent).timestamp()
        db.execute("insert into sends values(1,'X',?)",(json.dumps(dict(sent_at=sent,currency="EUR",estNotionalEUR=400)),))
        db.execute("insert into quotes values('12',?,?,?,?,?)",(at-2,at-3,99.,100.,"R"))
        row=dict(order_id="X",symbol="A",side="BUY",conid="12",expected=.4,fees=0,slip=None,ordered=4,at=sent,execution="demo",native_price=101.,filled=2.)
        o=costs([row,dict(row,execution="second",native_price=100.)],{"periods":[],"trades":[]},{"sources":[],"meta":{},"taxes":{},"fx":{}},db,{"fees":[]})
        self.assertAlmostEqual(o["orders"][0]["slippage_bps"],50)
        self.assertIsNone(o["orders"][0]["slippage_beyond_touch"]);self.assertEqual(o["future_measured"],1)
    def test_unreliable_output_not_priced(self):
        self.assertEqual(price(dict(requested="deepseek-v4-pro",at="2026-11-20T10:00:00+00:00",output_reliable=False,input=5,output=0)),(None,None))
    def test_no_allocation_without_scope(self):
        u={"calls":[]};s={"lines":[dict(provider="OpenAI",date="2026-11-18",model="org",usd="10",usage={})],"manifests":[],"ranges":{}}
        attach(u,s,{},{"2026-11-18":.9});self.assertEqual(u["provider_costs"][0]["Périmètre ouvert"],10)
        attach(u,s,{"exclusive":["OpenAI"]},{"2026-11-18":.9});self.assertEqual(u["daily_project_eur"]["2026-11-18"],9)
