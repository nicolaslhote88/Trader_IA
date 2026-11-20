from pathlib import Path
import sys
from streamlit.testing.v1 import AppTest

app = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(app.parent))
from app_modules.strategy_trials import MENU, VIEWS

test = AppTest.from_file(str(app), default_timeout=150).run()
assert not test.exception
test.radio(key="portfolio_read_view").set_value(MENU).run()
for tab in VIEWS:
    test.radio(key="experiment_view").set_value(tab).run()
    assert not test.exception, [x.message for x in test.exception]
    assert not test.error and test.dataframe
    if tab == VIEWS[0]:
        assert {"75", "64"} <= {x.value for x in test.metric}
    if tab == VIEWS[1]:
        sel = next(x for x in test.selectbox if x.label == "Pack à examiner")
        sel.select_index(1).run()
        assert not test.exception, [x.message for x in test.exception]
    if tab == VIEWS[2]:
        x = next(x for x in test.selectbox if x.label == "Session archivée")
        x.select_index(len(x.options) - 1).run()
        assert not test.exception and test.dataframe
    print("Vue vérifiée", tab, flush=True)
