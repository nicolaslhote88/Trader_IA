import sys,unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app_modules.visualizations import _normalize_transactions,_extract_trade_events,_build_position_sparkline
class ExecutionMarkerTests(unittest.TestCase):
    def test_prx_exact_time_price_and_multiple_fills(self):
        tx=_normalize_transactions(pd.DataFrame([
            dict(timestamp='2026-08-12T12:07:38Z',symbol='PRX.AS',side='BUY',price=39.355,quantity=26),
            dict(timestamp='2026-08-12T13:00:00Z',symbol='PRX.AS',side='BUY',price=39.4,quantity=1)]))
        hist=pd.DataFrame(dict(timestamp=pd.to_datetime(['2026-08-11','2026-08-13'],utc=True),close=[39.6,39.2]))
        ev=_extract_trade_events('PRX.AS',tx,start_ts=hist.timestamp.min(),end_ts=hist.timestamp.max())
        self.assertEqual(len(ev),2)
        self.assertEqual(ev.iloc[0].timestamp,pd.Timestamp('2026-08-12T12:07:38Z'))
        fig=_build_position_sparkline('PRX',hist,[39.355],ev,-9.86,False)
        self.assertEqual(fig.data[1].y[0],39.355)
        self.assertEqual(fig.data[1].x[0],ev.iloc[0].timestamp)
        self.assertEqual(fig.layout.shapes[-1].y0,39.355)
if __name__=='__main__':unittest.main()
