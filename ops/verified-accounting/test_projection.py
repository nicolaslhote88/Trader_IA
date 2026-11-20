import unittest
from decimal import Decimal as D

from projection import fifo, normalize, total


class Accounting(unittest.TestCase):
    def test_multiple_buys_partial_sales_close_and_reopen(self):
        def trade(ident,side,qty,nominal,fee):
            return dict(execution=ident, conid="1",symbol="A",date="20260401",side=side,qty=D(qty),
                        nominal=D(nominal),fee=D(fee),old_nominal=D(nominal),old_fee=D(fee))
        book, closed = fifo([trade("a","BUY",3,30,3),trade("b","BUY",2,30,2),trade("c","SELL",4,80,4),
                             trade("d","SELL",1,20,1),trade("e","BUY",2,40,2)])
        self.assertEqual([(x["buy"], x["qty"],x["realized"]) for x in closed],[("a",3,24),("b",1,3),("b",1,3)])
        self.assertEqual([(l["buy"],l["qty"],l["unit"]) for l in book["1"]],[("e",2,21)])

    def test_missing_and_independent_fee_currency(self):
        v=dict(tradeDate="20260102",dateTime="20260102;120000", conid="2",buySell="BUY",currency="USD",
               quantity="2",tradePrice="3",proof="flex",line=1,ibCommissionCurrency="GBP",ibCommission="-1",
               ibExecID="demo",tradeID="1",symbol="test",fxRateToBase=None)
        r=normalize(v, {}, {"rates":{"20260102/GBP":"1.2"}}, None)
        self.assertEqual((r["native"],r["nominal"],r["fee"],r["source"]),(6,None,D("1.2"),"À rapprocher"))
        self.assertIsNone(total([None,D(0)]))

    def test_sale_without_inventory(self):
        r=dict(conid="1",execution="s",date="20260102",symbol="A",side="SELL",qty=D(1),nominal=D(4),fee=D(0),old_nominal=D(4),old_fee=D(0))
        with self.assertRaises(ValueError):
            fifo([r])


if __name__ == "__main__":
    unittest.main()
