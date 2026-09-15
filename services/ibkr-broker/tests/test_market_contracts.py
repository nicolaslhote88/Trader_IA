import sys
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from market_contracts import equity_quantity_rules, quantity_error, normalize_news_time


def test_us_ui_increment_is_not_a_board_lot():
    rule = equity_quantity_rules("EXAMPLE", {"currency": "USD"}, {"sizeIncrement": 100})
    assert rule["quantity_increment"] == 1
    assert quantity_error(4, rule) is None


def test_japan_common_lot_and_unknown_fund():
    rule = equity_quantity_rules("8035.T", {"currency": "JPY", "industry": "Semiconductors", "category": "Equipment"}, {})
    assert quantity_error(4, rule) == "BOARD_LOT_MISMATCH"
    assert quantity_error(100, rule) is None
    unknown = equity_quantity_rules("XXXX.T", {"currency": "JPY", "category": "ETF"}, {})
    assert quantity_error(1, unknown) == "QUANTITY_RULE_UNKNOWN"


def test_news_future_is_observation_with_raw_provenance():
    now = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
    raw = int(datetime(2026, 1, 1, 12, tzinfo=timezone.utc).timestamp() * 1000)
    row = normalize_news_time(raw, now)
    assert row["published_at"] == now.isoformat()
    assert row["provider_time_raw"] == raw
    assert row["published_at_source"] == "observed_at_fallback"
    assert normalize_news_time(raw - 3*3600000, now)["published_at_source"] == "provider_epoch"
