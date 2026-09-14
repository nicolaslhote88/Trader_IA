import importlib.util,json,unittest
from pathlib import Path
import duckdb

SCRIPT=Path(__file__).resolve().parents[5]/'outils/scripts/fx_normalize_fills.py'
class NormalizerMetadataTests(unittest.TestCase):
    def test_reconciled_usd_preserves_eur_and_uses_exact_broker_native(self):
        spec=importlib.util.spec_from_file_location('fx_normalizer',SCRIPT)
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        c=duckdb.connect(':memory:');c.execute('create schema core')
        c.execute('create table core.orders(order_id varchar,symbol varchar)')
        c.execute("insert into core.orders values ('o','TSM')")
        c.execute('create table core.instruments(symbol varchar,currency varchar)')
        c.execute("insert into core.instruments values ('TSM','USD')")
        c.execute('create table core.fills(fill_id varchar,order_id varchar,ts_fill timestamp,price double,raw_fill_json json)')
        c.execute("insert into core.fills values ('f','o',now(),359.1654476525,?)",[json.dumps({'source':'ibkr_pf_reconcile','ibkrFill':{'price':'418.45'}})])
        self.assertEqual(m.normalize(c),1)
        price,native,rate,ccy=c.execute('select price,price_native,fx_rate_eur,currency from core.fills').fetchone()
        self.assertEqual(price,359.1654476525);self.assertEqual(native,418.45)
        self.assertAlmostEqual(rate,price/native);self.assertEqual(ccy,'USD')
        self.assertEqual(m.normalize(c),0);c.close()
if __name__=='__main__':unittest.main()
