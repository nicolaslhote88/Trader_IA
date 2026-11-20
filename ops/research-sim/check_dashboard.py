"""Run in the deployed dashboard container against its normal Streamlit entry point."""

import json
import sys
from pathlib import Path
from streamlit.testing.v1 import AppTest

app = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(app.parent))
from app_modules.research_sim import VIEWS, ROOT

report = json.loads((ROOT / "report.json").read_bytes())
test = AppTest.from_file(str(app), default_timeout=150).run()


def loaded():
    assert not test.exception, [x.message for x in test.exception]
    assert not test.error


loaded()
for view in VIEWS:
    test.radio(key="portfolio_read_view").set_value(view).run()
    loaded()
    assert test.dataframe, view
    if view == VIEWS[0]:
        dates = next(x for x in test.selectbox if x.label == "Photographie datée")
        dates.select_index(len(dates.options) - 1).run()
        loaded()
        assert {x.value for x in test.metric} >= {"566", "364", "260", "8"}
        next(x for x in test.selectbox if x.label == "Population").set_value("Détenus").run()
        assert len(test.dataframe[0].value) == 8
    if view == VIEWS[1]:
        assert " / " in test.metric[0].value
    if view == VIEWS[2] and not report["reviews"]:
        assert len(test.dataframe[0].value) == 4
    print("Vue vérifiée", view, flush=True)
