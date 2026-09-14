from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from global_context_tab import build_market_context_summary, render_global_context_tab


def _fixture_data() -> dict:
    return {
        "snapshot": pd.DataFrame([{
            "snapshot_id": "GC_20260810T110500Z_0aa86c43",
            "as_of": "2026-08-10T11:05:00Z",
            "status": "OK",
            "coverage_ratio": 0.908,
            "confidence": 0.683,
            "freshness_status": "aging",
            "ag1_pack_json": "{}",
        }]),
        "components": pd.DataFrame([
            {"component": "AG5", "status": "OK", "coverage_ratio": 0.97, "confidence": 0.58, "freshness_status": "aging", "age_hours": 5.7},
            {"component": "AG6", "status": "OK", "coverage_ratio": 0.97, "confidence": 0.58, "freshness_status": "aging", "age_hours": 5.4},
            {"component": "AG7", "status": "OK", "coverage_ratio": 0.75, "confidence": 0.87, "freshness_status": "fresh", "age_hours": 5.1},
            {"component": "AG8", "status": "OK", "coverage_ratio": 0.93, "confidence": 0.70, "freshness_status": "fresh", "age_hours": 4.8},
        ]),
        "ag5": pd.DataFrame([
            {"entity_id": "USD", "macro_score": -0.05, "coverage_ratio": 1.0, "confidence": 0.6, "freshness_status": "aging"},
            {"entity_id": "EUR", "macro_score": 0.06, "coverage_ratio": 0.84, "confidence": 0.51, "freshness_status": "fresh"},
            {"entity_id": "NOK", "macro_score": 0.41, "coverage_ratio": 1.0, "confidence": 0.6, "freshness_status": "aging"},
        ]),
        "ag6": pd.DataFrame([
            {"currency": "USD", "valuation_score": -0.04, "coverage_ratio": 1.0, "confidence": 0.59, "freshness_status": "aging"},
            {"currency": "EUR", "valuation_score": 0.00, "coverage_ratio": 1.0, "confidence": 0.59, "freshness_status": "fresh"},
            {"currency": "CHF", "valuation_score": -0.18, "coverage_ratio": 1.0, "confidence": 0.59, "freshness_status": "aging"},
        ]),
        "ag7": pd.DataFrame([
            {"entity_id": "USD", "positioning_score": -0.56, "z_score": 1.12, "crowded_flag": False, "crowded_direction": "neutral", "confidence": 0.8, "freshness_status": "fresh"},
            {"entity_id": "EUR", "positioning_score": 1.0, "z_score": -2.1, "crowded_flag": True, "crowded_direction": "short", "confidence": 0.9, "freshness_status": "fresh"},
        ]),
        "ag8": pd.DataFrame([
            {"currency": "USD", "policy_regime": "restrictive", "duration_pressure": 0.62, "coverage_ratio": 1.0, "confidence": 0.69, "freshness_status": "fresh"},
            {"currency": "EUR", "policy_regime": "tightening", "duration_pressure": 0.25, "coverage_ratio": 1.0, "confidence": 0.77, "freshness_status": "fresh"},
        ]),
        "ag1_brief": pd.DataFrame([{
            "run_id": "RUN_20260810_163006_20933",
            "ts_start": "2026-08-10T16:30:06+02:00",
            "model": "ag1_v4_consensus",
            "global_context_status": "OK",
            "global_context_age": 0.427,
            "global_context_pack_json": """{
              "status":"OK","use_policy":"CAUTION",
              "relevant_currencies":["EUR","USD"],
              "quality":{"source_freshness":"aging","coverage_ratio":0.908,"confidence":0.683},
              "component_summary":{
                "macro":{"relevant_rows":2,"usable_rows":2,"freshness":"aging","confidence":0.55,"details":"INCLUDED"},
                "fx_valuation":{"relevant_rows":2,"usable_rows":2,"freshness":"aging","confidence":0.60,"details":"INCLUDED"},
                "positioning":{"relevant_rows":2,"usable_rows":2,"freshness":"fresh","confidence":0.80,"details":"INCLUDED"},
                "rates_liquidity":{"relevant_rows":2,"usable_rows":2,"freshness":"fresh","confidence":0.69,"details":"INCLUDED"}
              }
            }""",
        }]),
        "errors": {},
    }


class _FakeStreamlit:
    def __init__(self):
        self.markdowns = []
        self.warnings = []

    def title(self, value):
        self.page_title = value

    def caption(self, value):
        self.last_caption = value

    def subheader(self, value):
        self.last_subheader = value

    def markdown(self, value, **_kwargs):
        self.markdowns.append(value)

    def warning(self, value):
        self.warnings.append(value)


def test_summary_preserves_the_four_agent_views_and_snapshot_quality():
    summary = build_market_context_summary(_fixture_data())

    assert summary["snapshot"]["headline"] == "Contexte disponible, fraîcheur à surveiller"
    assert summary["snapshot"]["coverage"] == 0.908
    assert list(summary["agents"]) == ["AG5", "AG6", "AG7", "AG8"]
    assert summary["agents"]["AG5"]["signals"][0]["currency"] == "NOK"
    assert summary["agents"]["AG7"]["signals"][0]["currency"] == "EUR"
    assert summary["agents"]["AG7"]["signals"][0]["detail"] == "Crowding short"
    assert summary["ag1_brief"]["policy"] == "CAUTION"
    assert summary["ag1_brief"]["relevant_currencies"] == ["EUR", "USD"]
    assert [row["currency"] for row in summary["matrix"]] == ["USD", "EUR", "CHF", "NOK"]
    assert [row["currency"] for row in summary["matrix"] if row["ag1_relevant"]] == ["USD", "EUR"]


def test_render_is_one_view_without_tabs_or_dataframes():
    fake_st = _FakeStreamlit()
    with patch("global_context_tab.load_global_context_data", return_value=_fixture_data()):
        render_global_context_tab(fake_st, global_path="global", world_path="world", macro_path="macro", ag1_path="ag1")

    page = "\n".join(fake_st.markdowns)
    assert fake_st.page_title == "Contexte de marché"
    assert "gctx-agent-grid" in page
    assert all(agent in page for agent in ("AG5", "AG6", "AG7", "AG8"))
    assert "gctx-matrix" in page
    assert "gctx-guide-grid" in page
    assert "gctx-ag1-panel" in page
    assert "Influence possible sur AG1" in page
    assert "Brief AG1" in page
    assert "Non couvert" in page
    assert not hasattr(fake_st, "tabs")
    assert not hasattr(fake_st, "dataframe")
