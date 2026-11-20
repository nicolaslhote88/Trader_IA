import json
from pathlib import Path
import tempfile
import unittest
from decimal import Decimal as D
import xml.etree.ElementTree as ET

from statements import activate,connect,digest,events,ingest,key_account,load
from reconcile import report
from publish import publish

ACCOUNT="TEST-LOT"
HEAD=key_account(ACCOUNT)


def xml(start="20260501",end="20261118",gen="20261119;030000",rows=None,account=ACCOUNT):
    root=ET.Element("FlexQueryResponse",type="AF")
    stmt=ET.SubElement(ET.SubElement(root,"FlexStatements"),"FlexStatement",accountId=account,
                       fromDate=start,toDate=end,whenGenerated=gen)
    ET.SubElement(stmt,"AccountInformation",currency="EUR")
    groups={}
    for section,kind,values in rows or []:
        if section is None:
            ET.SubElement(stmt,kind,values)
        else:
            if section not in groups:groups[section]=ET.SubElement(stmt,section)
            if kind:ET.SubElement(groups[section],kind,values)
    return ET.tostring(root)


def trade(tid,**kwargs):
    v=dict(tradeID=tid,ibExecID="E"+tid,conid="101",currency="USD",tradeDate="20261118",settleDateTarget="20261120",
           quantity="1.125",buySell="BUY",ibCommission="-0.33",ibCommissionCurrency="EUR",levelOfDetail="EXECUTION",assetCategory="STK")
    return ("Trades","Trade",dict(v,**kwargs))


def cash(tid,**kwargs):
    return ("CashTransactions","CashTransaction",dict(dict(transactionID=tid,dateTime="20261118;150100",settleDate="20261120",
        amount="10",type="Dividends",currency="USD",fxRateToBase="0.9"),**kwargs))


def fixture():
    nav=(None,"ChangeInNAV",dict(startingValue="1000",endingValue="1009.67",mtm="1",dividends="9",commissions="-0.33",depositsWithdrawals="0"))
    balance=("CashReport","CashReportCurrency",dict(currency="USD",startingCash="0",endingCash="10",endingSettledCash="5"))
    pos=("OpenPositions","OpenPosition",dict(conid="101",currency="USD",position="1.125",levelOfDetail="SUMMARY",reportDate="20261118"))
    funds=("StmtFunds","StatementOfFundsLine",dict(transactionID="F1",currency="USD",amount="10",date="20261118",settleDate="20261120"))
    return [nav,balance,pos,funds,trade("1"),cash("C1")]


def reference():
    hist=dict(captured="2026-11-18T20:00:00+00:00",nav=[dict(date="2026-05-01")],capital="1000",fifo="0",latent="1",
              instant="1010.50",residual="9.50",sources={"broker":"test"})
    data=dict(captured="2026-11-20T09:00:00+00:00",nav={"2026-11-18":dict(value="1009.672",twr="0.00967",date="2026-11-18")},
              positions=[dict(date="2026-11-18",at="close",basis="close",rows=[dict(conid="101",qty="1.125")])],
              cash=[dict(date="2026-11-18",currency="USD",basis="close",at_utc="close",cashbalance="10",settledcash="5")],
              flows=[],fills=[dict(execution_id="E1",conid="101",qty="1.125",side="BUY",commission_amount="0.33",commission_ccy="EUR",
                                  commission_source="native",ts="2026-11-18 15:01:00")],
              trades=[dict(execution_id="E1",conid=101,size="1.125",side="B")])
    return hist,data


class Tests(unittest.TestCase):
    def setUp(self):
        self.db=connect(":memory:")
        self.addCleanup(self.db.close)

    def test_repeat_overlap_versions_and_rollback(self):
        a=xml(rows=fixture())
        aid,added=ingest(self.db,a,HEAD)
        self.assertTrue(added)
        self.assertEqual(ingest(self.db,a,HEAD),(aid,False))
        b=xml("20261101",gen="20261120;090000",rows=[trade("1"),cash("C1",amount="12"),cash("C2")])
        bid,_=ingest(self.db,b,HEAD)
        def active():
            return events(load(self.db)[1])[0]
        self.assertEqual(len(active()),4) # one trade, two cash movements and one funds reference
        self.assertEqual([x["v"]["amount"] for x in active() if x["v"].get("transactionID")=="C1"],["12"])
        activate(self.db,bid,False)
        self.assertEqual(ingest(self.db,b,HEAD),(bid,False))
        self.assertEqual(len(active()),3)
        self.assertEqual([x["v"]["amount"] for x in active() if x["v"].get("transactionID")=="C1"],["10"])
        activate(self.db,bid,True)
        self.assertEqual(len(active()),4)
        self.assertEqual(digest(self.db.execute("select raw from sources where id=?",(bid,)).fetchone()[0]),bid)

    def test_all_sections_and_rounding(self):
        ingest(self.db,xml(rows=fixture()),HEAD)
        frozen,ref=reference()
        result=report(self.db,frozen,ref)
        lines={x["rubrique"]:x for x in result["lines"]}
        for name in ("Équilibre du pont de NAV","Quantité · contrat 101","Exécution E1 · commission","Mouvements d’espèces / clôture · USD","Détail Flex / NAV · Dividendes","Résidu historique à ventiler"):
            self.assertEqual(lines[name]["etat"],"rapproché",name)
        self.assertEqual(lines["Clôture du relevé / PortfolioAnalyst"]["etat"],"expliqué")
        self.assertEqual(lines["Clôture du relevé / PortfolioAnalyst"]["ecart"],D("-0.002"))
        self.assertEqual(lines["Résidu historique à ventiler"]["ecart"],D(0))
        with tempfile.TemporaryDirectory() as t:
            st=publish(Path(t),frozen,ref,result,[],[("id","actif")],"test")
            self.assertEqual(st["periodes"],1)
            self.assertTrue((Path(t)/"rapprochement.html").exists())

    def test_partial_close_not_certified_and_no_rounding_quantity(self):
        ingest(self.db,xml(rows=fixture()),HEAD)
        h,ref=reference()
        ref["cash"][0]["basis"]="intraday"
        ref["positions"][0]["rows"][0]["qty"]="1.125000001"
        res=report(self.db,h,ref)
        lines={r["rubrique"]:r for r in res["lines"]}
        self.assertEqual(lines["USD · espèces à la date de transaction"]["etat"],"ouvert")
        self.assertEqual(lines["Quantité · contrat 101"]["ecart"],D("-0.000000001"))
        self.assertEqual(lines["Quantité · contrat 101"]["etat"],"ouvert")

    def test_cancel_correct_and_out_of_order(self):
        ingest(self.db,xml(rows=[trade("1",notes="C")]),HEAD)
        ingest(self.db,xml(gen="20261120;010000",rows=[trade("2",notes="Ca",origTradeID="1"),trade("3",notes="Co",origTradeID="1",quantity="2")]),HEAD)
        chosen,issues=events(load(self.db)[1])
        self.assertFalse(issues)
        self.assertEqual([x["v"]["tradeID"] for x in chosen],["3"])
        ingest(self.db,xml(gen="20261118;010000",rows=[trade("3",quantity="9")]),HEAD)
        self.assertEqual(events(load(self.db)[1])[0][0]["v"]["quantity"],"2")

    def test_absent_identifier_conflicts_and_missing_sections(self):
        ingest(self.db,xml(rows=[cash("",amount="4"),cash("X",amount="2"),cash("X",amount="3")]),HEAD)
        chosen,issues=events(load(self.db)[1])
        self.assertFalse(chosen)
        self.assertEqual(len(issues),2)
        h,r=reference()
        res=report(self.db,h,r)
        self.assertTrue(any(x["rubrique"]=="Résidu historique à ventiler" and x["etat"]=="ouvert" for x in res["lines"]))

    def test_inception_detail_with_both_fx_legs_and_flat_summary(self):
        h,ref=reference()
        h.update(capital="100",instant="115",residual="15",fifo="0",latent="0")
        def funds(ccy,code,amt,tid,trade=""):
            return ("StmtFunds","StatementOfFundsLine",dict(currency=ccy,activityCode=code,amount=amt,transactionID=tid,tradeID=trade,date="20261118"))
        rows=[
            (None,"ChangeInNAV",dict(currency="EUR",startingValue="0",endingValue="113",mtm="0",realized="0",changeInUnrealized="0",depositsWithdrawals="0")),
            ("EquitySummaryInBase","EquitySummaryByReportDateInBase",dict(currency="EUR",reportDate="20260430",total="0",cash="0",stock="0")),
            ("EquitySummaryInBase","EquitySummaryByReportDateInBase",dict(currency="EUR",reportDate="20261118",total="113",cash="59",stock="54")),
            ("CashReport","CashReportCurrency",dict(currency="EUR",startingCash="0",endingCash="50")),
            ("CashReport","CashReportCurrency",dict(currency="USD",startingCash="0",endingCash="10")),
            ("OpenPositions","OpenPosition",dict(conid="101",assetCategory="STK",levelOfDetail="SUMMARY",reportDate="20261118",currency="USD",fxRateToBase="0.9",position="1",fifoPnlUnrealized="10",positionValue="60")),
            ("ConversionRates","ConversionRate",dict(reportDate="20261118",fromCurrency="USD",toCurrency="EUR",rate="0.9")),
            ("CorporateActions",None,{}),("Transfers",None,{}),
            funds("EUR","DEP","100","dep"),funds("EUR","FOREX","-50","fx1","2"),funds("USD","FOREX","60","fx1","2"),funds("USD","BUY","-50","stock","1"),
            trade("1",quantity="1",netCash="-50",fifoPnlRealized="0"),trade("2",assetCategory="CASH",symbol="EUR.USD",quantity="-50",proceeds="60",buySell="SELL")
        ]
        key,_=ingest(self.db,xml(rows=rows),HEAD)
        r=report(self.db,h,ref)
        self.assertEqual(r["issues"],0)
        self.assertEqual(sum(x["montant"] for x in r["attribution"]),D("15"))
        self.assertEqual(next(x["montant"] for x in r["attribution"] if x["rubrique"].startswith("Conversions")),D("4"))
        activate(self.db,key,False)
        rows[-1][2]["proceeds"]="61"
        ingest(self.db,xml(rows=rows),HEAD)
        self.assertFalse(report(self.db,h,ref)["attribution"])

    def test_account_and_atomic_import(self):
        for raw in (xml(account="OTHER"),xml(rows=[trade("T",quantity="NaN")]),b"<FlexStatement/>",b"<!DOCTYPE x><FlexStatement/>",b"<bad"):
            with self.assertRaises(ValueError):ingest(self.db,raw,HEAD)
        self.assertEqual(self.db.execute("select count(*) from sources").fetchone()[0],0)

    def test_mtm_and_fifo_not_added_together(self):
        rows=fixture()
        rows[0][2]["realized"]="1"
        ingest(self.db,xml(rows=rows),HEAD)
        result=report(self.db,*reference())
        self.assertEqual(next(r["etat"] for r in result["lines"] if r["rubrique"]=="Équilibre du pont de NAV"),"ouvert")

    def test_native_fee_proof_and_no_cross_currency_subtraction(self):
        ingest(self.db,xml(rows=fixture()),HEAD)
        h,ref=reference()
        f=ref["fills"][0]
        f["commission_source"]="ibkr_commission_fallback_eur_no_ccy"
        def fee():
            return next(x for x in report(self.db,h,ref)["lines"] if x["rubrique"]=="Exécution E1 · commission")
        self.assertEqual(fee()["etat"],"expliqué")
        f["commission_ccy"]="USD"
        self.assertIsNone(fee()["ecart"])
        self.assertEqual(fee()["etat"],"ouvert")

    def test_withdraw_all_restores_unallocated_real_reference(self):
        sid,_=ingest(self.db,xml(rows=fixture()),HEAD)
        h,ref=reference()
        activate(self.db,sid,False)
        r=report(self.db,h,ref)
        self.assertEqual(r["imports"],0)
        self.assertEqual(r["periods"],0)
        residue=next(x for x in r["lines"] if x["rubrique"]=="Résidu historique à ventiler")
        self.assertEqual((residue["etat"],residue["montant"]),("ouvert",D("9.50")))

    def test_periods_never_add_overlapping_nav(self):
        a=xml(rows=fixture())
        ingest(self.db,a,HEAD)
        ingest(self.db,xml(start="20261101",rows=fixture()),HEAD)
        h,ref=reference()
        r=report(self.db,h,ref)
        self.assertEqual(r["periods"],2)
        self.assertEqual(len(r["events"]),3)
        rows=[x for x in r["lines"] if x["rubrique"]=="Résidu historique à ventiler"]
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["ecart"],D(0))

    def test_rollforward_with_split_transfer_and_fills(self):
        hist,ref=reference()
        old=("OpenPositions","OpenPosition",dict(conid="101",position="1",reportDate="20261031"))
        ingest(self.db,xml(start="20261001",end="20261031",rows=[old]),HEAD)
        ca=("CorporateActions","CorporateAction",dict(transactionID="A1",conid="101",quantity="1",dateTime="20261118",currency="USD"))
        tr=("Transfers","Transfer",dict(transactionID="T1",conid="101",quantity="-0.5",date="20261118"))
        new=("OpenPositions","OpenPosition",dict(conid="101",position="2.625",reportDate="20261118"))
        ingest(self.db,xml(start="20261101",rows=[ca,tr,new,trade("1")]),HEAD)
        result=report(self.db,hist,ref)
        check=next(x for x in result["lines"] if x["rubrique"]=="Stock reconstitué · contrat 101")
        self.assertEqual((check["etat"],check["ecart"]),("rapproché",D(0)))


if __name__=="__main__":
    unittest.main()
