import unittest
from datetime import datetime, timezone
import main


class SessionFreshnessTests(unittest.TestCase):
    def test_normalized_us_daily_date_is_not_previous_session(self):
        for exchange, symbol in [("NASDAQ", "TXN"), ("NYSE", "KO")]:
            for day, close_hour in [("2026-10-02", 20), ("2026-12-02", 21)]:
                timestamp = day + "T00:00:00Z"
                before = datetime.fromisoformat(day + "T14:37:00+00:00")
                after = datetime.fromisoformat(day + f"T{close_hour}:10:00+00:00")
                self.assertFalse(main._bar_is_closed(timestamp, "1d", exchange, symbol, "EQUITY", before))
                self.assertTrue(main._bar_is_closed(timestamp, "1d", exchange, symbol, "EQUITY", after))

    def test_monday_before_open_references_friday(self):
        now = datetime(2026, 10, 5, 7, 3, tzinfo=timezone.utc)
        r = main._ai_session_reference("1h", "NASDAQ", "TXN", "EQUITY", now)
        self.assertEqual("2026-10-02T19:30:00+00:00", r["latestExpectedBarTime"])

    def test_us_first_hour_requires_grace(self):
        before = main._ai_session_reference("1h", "NASDAQ", "TXN", "EQUITY", datetime(2026, 10, 2, 14, 37, tzinfo=timezone.utc))
        after = main._ai_session_reference("1h", "NASDAQ", "TXN", "EQUITY", datetime(2026, 10, 2, 14, 41, tzinfo=timezone.utc))
        self.assertEqual("2026-10-01T19:30:00+00:00", before["latestExpectedBarTime"])
        self.assertEqual("2026-10-02T13:30:00+00:00", after["latestExpectedBarTime"])

    def test_crypto_has_no_weekend_exemption(self):
        self.assertIsNone(main._ai_session_reference("1h", "", "BTC-USD", "CRYPTO", datetime.now(timezone.utc)))

    def test_missing_holiday_calendar_is_explicit_and_conservative(self):
        r = main._ai_session_reference("1h", "NYSE", "KO", "EQUITY", datetime(2026, 12, 25, 23, tzinfo=timezone.utc))
        self.assertEqual("CONSERVATIVE_NO_HOLIDAY_EXEMPTION", r["holidayPolicy"])
        self.assertTrue(r["latestExpectedBarTime"].startswith("2026-12-25"))
