import unittest
from datetime import datetime, timezone

import pandas as pd

import main


class ClosedBarsTests(unittest.TestCase):
    def test_paris_daily_is_open_before_regular_close(self):
        now = datetime(2026, 7, 27, 13, 0, tzinfo=timezone.utc)
        self.assertFalse(
            main._bar_is_closed(
                pd.Timestamp("2026-07-27T00:00:00+02:00"),
                "1d", "Euronext Paris", "AIR.PA", "EQUITY", now,
            )
        )

    def test_paris_daily_is_closed_after_close_and_grace(self):
        now = datetime(2026, 7, 27, 15, 45, tzinfo=timezone.utc)
        self.assertTrue(
            main._bar_is_closed(
                pd.Timestamp("2026-07-27T00:00:00+02:00"),
                "1d", "Euronext Paris", "AIR.PA", "EQUITY", now,
            )
        )

    def test_new_york_dst_daily_is_not_available_mid_session(self):
        now = datetime(2026, 3, 20, 18, 0, tzinfo=timezone.utc)
        self.assertFalse(
            main._bar_is_closed(
                pd.Timestamp("2026-03-20T00:00:00-04:00"),
                "1d", "NYSE", "WMT", "EQUITY", now,
            )
        )

    def test_tokyo_and_hong_kong_daily_close_in_local_timezone(self):
        tokyo_bar = pd.Timestamp("2026-07-27T00:00:00+09:00")
        hk_bar = pd.Timestamp("2026-07-27T00:00:00+08:00")
        self.assertFalse(main._bar_is_closed(tokyo_bar, "1d", "TOKYO", "6861.T", "EQUITY", datetime(2026, 7, 27, 6, 35, tzinfo=timezone.utc)))
        self.assertTrue(main._bar_is_closed(tokyo_bar, "1d", "TOKYO", "6861.T", "EQUITY", datetime(2026, 7, 27, 6, 40, tzinfo=timezone.utc)))
        self.assertFalse(main._bar_is_closed(hk_bar, "1d", "HKEX", "0700.HK", "EQUITY", datetime(2026, 7, 27, 8, 5, tzinfo=timezone.utc)))
        self.assertTrue(main._bar_is_closed(hk_bar, "1d", "HKEX", "0700.HK", "EQUITY", datetime(2026, 7, 27, 8, 10, tzinfo=timezone.utc)))

    def test_crypto_daily_closes_at_next_utc_midnight_plus_grace(self):
        bar = pd.Timestamp("2026-07-27T00:00:00Z")
        self.assertFalse(main._bar_is_closed(bar, "1d", "", "BTC-USD", "CRYPTO", datetime(2026, 7, 28, 0, 5, tzinfo=timezone.utc)))
        self.assertTrue(main._bar_is_closed(bar, "1d", "", "BTC-USD", "CRYPTO", datetime(2026, 7, 28, 0, 10, tzinfo=timezone.utc)))

    def test_h1_requires_end_plus_grace(self):
        start = pd.Timestamp("2026-07-27T13:00:00Z")
        self.assertFalse(main._bar_is_closed(start, "1h", "NYSE", "WMT", "EQUITY", datetime(2026, 7, 27, 14, 5, tzinfo=timezone.utc)))
        self.assertTrue(main._bar_is_closed(start, "1h", "NYSE", "WMT", "EQUITY", datetime(2026, 7, 27, 14, 10, tzinfo=timezone.utc)))

    def test_filter_drops_open_and_invalid_rows(self):
        frame = pd.DataFrame(
            [
                {"Datetime": pd.Timestamp("2026-07-24T13:00:00Z"), "Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 10},
                {"Datetime": pd.Timestamp("2026-07-24T14:00:00Z"), "Open": 100, "High": 100, "Low": 99, "Close": 101, "Volume": 10},
                {"Datetime": pd.Timestamp("2026-07-27T13:00:00Z"), "Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 10},
            ]
        )
        bars, stats = main._df_to_validated_bars(
            frame, 20, "1h", "NYSE", "WMT", "EQUITY", True, True,
            datetime(2026, 7, 27, 13, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(1, len(bars))
        self.assertTrue(bars[0]["closed"])
        self.assertEqual(1, stats["droppedInvalid"])
        self.assertEqual(1, stats["droppedOpen"])


if __name__ == "__main__":
    unittest.main()
