import unittest
from test_ibkr_symbol_identity import load_node_namespace

class FillCurrencyTests(unittest.TestCase):
    def test_amsterdam_eur_and_usd_native(self):
        n=load_node_namespace();r={'price':'39.355','commission':'3','listing_exchange':'AEB'}
        self.assertEqual(n['fill_price_eur'](r,{'USD':.8661522}),39.355)
        self.assertEqual(n['commission_eur'](r,{'USD':.8661522}),3)
        r.update(listing_exchange='NASDAQ.NMS',price='100',commission='1')
        self.assertEqual(n['fill_price_eur'](r,{'USD':.86}),86)
        self.assertEqual(n['commission_eur'](r,{'USD':.86}),.86)
    def test_unknown_currency_and_missing_fx_fail_closed(self):
        n=load_node_namespace()
        with self.assertRaisesRegex(ValueError,'CURRENCY_UNRESOLVED'): n['fill_price_eur']({'price':100,'exchange':'SMART'}, {'USD':.86})
        with self.assertRaisesRegex(ValueError,'FX_RATE_UNAVAILABLE'): n['fill_price_eur']({'price':100,'currency':'JPY'}, {})
    def test_explicit_currency_and_commission_currency(self):
        n=load_node_namespace();r={'price':100,'currency':'EUR','listing_exchange':'NASDAQ','commission':1,'commission_currency':'USD'}
        self.assertEqual(n['fill_price_eur'](r,{'USD':.86}),100)
        self.assertEqual(n['commission_eur'](r,{'USD':.86}),.86)

if __name__=='__main__':unittest.main()
