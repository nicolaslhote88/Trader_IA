from __future__ import annotations

import math
import asyncio
import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fred_client
from fred_client import FREDClient, _annualize_qoq_pct, _gdp_rates
from world_bank_client import WorldBankClient


def test_gdp_level_is_converted_to_annualized_growth_not_used_as_a_rate():
    observations = [
        {"date": "2026-04-01", "value": 102.0},
        {"date": "2026-01-01", "value": 101.0},
        {"date": "2025-10-01", "value": 100.0},
    ]
    latest, previous = _gdp_rates(observations, "level_qoq_annualized")
    assert latest == pytest_approx(((102.0 / 101.0) ** 4 - 1.0) * 100.0)
    assert previous == pytest_approx(((101.0 / 100.0) ** 4 - 1.0) * 100.0)
    assert abs(latest) < 10


def test_qoq_rate_is_annualized_consistently():
    assert _annualize_qoq_pct(1.0) == pytest_approx(4.060401)


def test_world_bank_ppp_and_reer_directions_are_currency_supportive_when_undervalued():
    ppp, _ = WorldBankClient._transform("ppp_fair_value_usd", {"value": 2.0}, None)
    assert ppp == 0.5
    latest = {"value": 90.0, "_history": [{"value": 90.0}, {"value": 100.0}, {"value": 102.0}]}
    reer_gap, _ = WorldBankClient._transform("reer_gap_pct", latest, None)
    assert reer_gap > 0
    assert math.isfinite(reer_gap)


def test_fred_retries_transient_server_errors(monkeypatch):
    request = httpx.Request("GET", "https://example.test")
    responses = [
        httpx.Response(502, request=request, text="temporary"),
        httpx.Response(200, request=request, json={"observations": [{"date": "2026-08-01", "value": "1.25"}]}),
    ]

    class FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return responses.pop(0)

    async def no_sleep(*args, **kwargs):
        return None

    monkeypatch.setattr(fred_client.httpx, "AsyncClient", lambda **kwargs: FakeAsyncClient())
    monkeypatch.setattr(fred_client.asyncio, "sleep", no_sleep)
    client = FREDClient("test-key")
    client.max_retries = 2
    rows = asyncio.run(client.get_series("TEST"))
    assert rows == [{"date": "2026-08-01", "value": 1.25}]
    assert responses == []


def pytest_approx(value: float):
    # Évite d'importer pytest dans le code de production tout en gardant des
    # assertions numériques lisibles dans ce petit test.
    import pytest
    return pytest.approx(value, rel=1e-9, abs=1e-9)
