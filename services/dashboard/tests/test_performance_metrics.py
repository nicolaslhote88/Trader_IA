import sys
from pathlib import Path
import pandas as pd
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from performance_metrics import *


def bars(values):
    return pd.DataFrame({"timestamp": pd.to_datetime(["2026-01-05", "2026-01-06"], utc=True), "close": values})


def test_constant_us_price_changes_in_eur_and_is_not_available_early():
    contract = benchmark_contract("^GSPC")
    early = benchmark_eur(bars([100,100]), contract, bars([1,1.25]), cutoff="2026-01-06T20:00Z")
    assert len(early) == 1
    late = benchmark_eur(bars([100,100]), contract, bars([1,1.25]), cutoff="2026-01-07T00:00Z")
    assert late.close.tolist() == [100,80]
    assert benchmark_eur(bars([100,100]), contract, None).empty


def test_no_future_or_unbounded_fill():
    b = benchmark_eur(bars([100,110]), benchmark_contract("^FCHI"), cutoff="2026-01-07T00:00Z")
    n = pd.DataFrame({"timestamp": pd.to_datetime(["2026-01-05T10:00Z", "2026-01-06T10:00Z", "2026-01-20T10:00Z"]), "value": [100,100,100]})
    matched = benchmark_at_nav_times(b,n)
    assert len(matched) == 1 and matched.close.iloc[0] == 100


def test_deposit_and_withdrawal_are_not_returns_but_dividend_is():
    n = pd.DataFrame({"timestamp": pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"],utc=True), "value": [100,200,150,165]})
    flows = pd.DataFrame({"timestamp": n.timestamp.iloc[1:3], "amount": [100,-50]})
    assert flow_neutral_index(n,flows).value.tolist() == pytest.approx([100,100,100,110])


def test_custom_contract_keeps_currency_and_variant():
    assert benchmark_contract("CUSTOM", {"currency": "USD", "variant": "NET_RETURN"})["variant"] == "NET_RETURN"
    assert benchmark_eur(bars([1,2]), {"ticker": "UNKNOWN"}).empty


def test_duckdb_nanoseconds_and_provider_microseconds_are_compatible():
    benchmark=bars([100,110])
    benchmark["timestamp"]=benchmark["timestamp"].astype("datetime64[us, UTC]")
    nav=pd.DataFrame({"timestamp":pd.to_datetime(["2026-01-06T10:00:00Z"]).astype("datetime64[ns, UTC]"),"value":[100]})
    assert benchmark_at_nav_times(benchmark,nav).close.iloc[0]==110
