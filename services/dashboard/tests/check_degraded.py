"""Recette des publications absentes et de la cascade sur les fichiers réellement montés."""
from pathlib import Path
import json
import sys
from unittest.mock import patch
from streamlit.testing.v1 import AppTest

app = Path(sys.argv[1]).resolve()
sys.path.insert(0,str(app.parent))
from app_modules import pilotage_data as source
from app_modules.account_view import waterfall
from app_modules.pilotage_pages import PAGES

account, econ=source.published("account"),source.published("economic")
result=waterfall(account,econ.get("performance",{}))
assert result and abs(result[1])<=1, "Écart avec le résultat économique publié"
print("Cascade vérifiée, écart en euros", round(result[1],6),flush=True)
empty={"snapshot":(None,None),"pf_times":[],"news":None,"technical":None,"fundamental":None}
with patch.object(source, "published", return_value={}),patch.object(source,"evidence",return_value=empty):
    ui=AppTest.from_file(str(app),default_timeout=80).run()
    for page in sorted(PAGES):
        ui.button(key="nav_"+page).click().run()
        assert not ui.exception and not ui.error,(page,[v.message for v in ui.exception])
    assert any(b.key=="broker_alert" for b in ui.button)
print("Huit écrans avec publications absentes et compte non confirmé",flush=True)
