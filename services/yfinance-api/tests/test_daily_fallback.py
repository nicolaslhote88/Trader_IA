import tempfile
import unittest
from datetime import datetime, time, timezone
from unittest.mock import patch
from daily_fallback import fill_daily_gaps


def bar(day, price=100):
    return dict(
        t=f"2026-10-{day:02d}T00:00:00Z",
        o=price,
        h=price + 2,
        l=price - 2,
        c=price + 1,
        v=10000,
        closed=True,
        quality="VALID",
    )


def ibkr(day, price=100):
    b = bar(day, price)
    b["t"] = datetime(2026, 10, day, 7, tzinfo=timezone.utc).timestamp() * 1000
    return b


class DailyFallbackTests(unittest.TestCase):
    def invoke(
        self, *, identity=None, prices=None, now=None, symbol="BNP.PA", error=None
    ):
        original = [bar(1), bar(2)]
        contract = identity or dict(
            contract_symbol="BNP", currency="EUR", conid=123, metadata_error=None
        )
        replies = [
            {"results": [contract], "errors": []},
            {
                "conid": 123,
                "data": prices if prices is not None else [ibkr(1), ibkr(2), ibkr(5)],
            },
        ]
        with tempfile.TemporaryDirectory() as folder, patch(
            "daily_fallback._get", side_effect=error or replies
        ) as request:
            args = dict(
                symbol=symbol,
                asset_class="EQUITY",
                market_timezone="Europe/Paris",
                market_close=time(17, 30),
                now=now or datetime(2026, 10, 6, 6, tzinfo=timezone.utc),
                data_dir=folder,
                max_bars=400,
                base_url="http://broker",
            )
            result, audit = fill_daily_gaps(original, **args)
            return original, result, audit, request.call_args_list

    def test_adds_observed_closed_bar_without_overwriting_yahoo(self):
        old, new, audit, calls = self.invoke()
        self.assertEqual(old, new[:2])
        self.assertEqual(3, len(new))
        self.assertEqual("FILLED", audit["status"])
        self.assertEqual("ibkr_cpapi_daily", new[-1]["source"])
        self.assertEqual(123, new[-1]["conid"])
        self.assertEqual("false", calls[-1].args[-1]["outside_rth"])

    def test_refuses_wrong_currency(self):
        old, new, audit, _ = self.invoke(
            identity=dict(contract_symbol="BNP", currency="USD", conid=123)
        )
        self.assertEqual(old, new)
        self.assertIn("IDENTITY_MISMATCH", audit["error"])

    def test_refuses_wrong_security(self):
        old, new, audit, _ = self.invoke(
            identity=dict(contract_symbol="OTHER", currency="EUR", conid=123)
        )
        self.assertEqual(old, new)
        self.assertIn("IDENTITY_MISMATCH", audit["error"])

    def test_refuses_unit_or_series_mismatch(self):
        old, new, audit, _ = self.invoke(
            prices=[ibkr(1, 10000), ibkr(2, 10000), ibkr(5, 10000)]
        )
        self.assertEqual(old, new)
        self.assertIn("PRICE_SERIES_MISMATCH", audit["error"])

    def test_refuses_insufficient_common_history(self):
        old, new, audit, _ = self.invoke(prices=[ibkr(2), ibkr(5)])
        self.assertEqual(old, new)
        self.assertIn("INSUFFICIENT_OVERLAP", audit["error"])

    def test_drops_invalid_broker_bar(self):
        broken = ibkr(5)
        broken["c"] = 1000
        old, new, audit, _ = self.invoke(prices=[ibkr(1), ibkr(2), broken])
        self.assertEqual(old, new)
        self.assertEqual(0, audit["added"])

    def test_cannot_admit_current_session_before_close_grace(self):
        old, new, audit, _ = self.invoke(
            now=datetime(2026, 10, 5, 15, 35, tzinfo=timezone.utc)
        )
        self.assertEqual(old, new)
        self.assertEqual(0, audit["added"])

    def test_disallows_ambiguous_unsuffixed_symbol(self):
        old, new, audit, calls = self.invoke(symbol="ABB")
        self.assertEqual(old, new)
        self.assertEqual([], calls)
        self.assertEqual("OUT_OF_SCOPE", audit["status"])

    def test_outage_keeps_existing_bars_and_explicit_failure(self):
        old, new, audit, _ = self.invoke(error=TimeoutError("unavailable"))
        self.assertEqual(old, new)
        self.assertEqual("UNAVAILABLE", audit["status"])


if __name__ == "__main__":
    unittest.main()
