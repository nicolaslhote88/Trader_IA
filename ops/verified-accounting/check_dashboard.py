"""Exercise both deployed dashboard routes with the production container's Streamlit runtime."""
import json
import sys
from pathlib import Path
from streamlit.testing.v1 import AppTest

app = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(app.parent))
from app_modules.verified_account import REPORT, euros, probe
from unittest.mock import patch
import hashlib
from types import SimpleNamespace
account = "DEMO"
key = hashlib.sha256(account.encode()).hexdigest()
sample = {"authenticated":True, "dry_run":False, "account_alignment":{"selected_account":account,"configured_account_id":account,"aligned":True,"gateway_is_paper":False}}
def mock_response(value):
    return SimpleNamespace(json=lambda:value, raise_for_status=lambda:None)
with patch("app_modules.verified_account.requests.get", side_effect=[mock_response(v) for v in [
        sample,{"BASE":{"currency":"BASE","netliquidationvalue":99},"EUR":{"exchangerate":1}},[{"conid":12,"position":3}],sample]]):
    assert probe("http://fixture-aligned",key)[2:] == (99,{"12":3.0})
with patch("app_modules.verified_account.requests.get", return_value=mock_response({"authenticated":False})):
    assert probe("http://fixture-disconnected",key)[2:] == (None,None)


data = json.loads(REPORT.read_bytes())
at = AppTest.from_file(str(app), default_timeout=120).run()
assert not at.exception, [e.message for e in at.exception]
assert not at.error,[e.value for e in at.error]
assert at.title[0].value == "Cockpit"
at.button(key="nav_Mon compte").click().run()
assert any(x.label=="Détails et preuves" for x in at.expander)
assert not at.error and not at.exception
metrics = {m.label:m.value for m in at.metric}
assert metrics["Valeur du compte"] == euros(data["close"]["total"])
assert metrics["Résultat FIFO des ventes"] != "indisponible"
assert any("à la clôture" in h.value for h in at.subheader)
at.checkbox(key="account_old_rate").check().run()
assert not at.exception, [e.message for e in at.exception]
count = sum(float(t["old_rate"] or 0)==0.8764 for t in data["trades"])
assert any(len(t.value)==count and "Nominal corrigé (€)" in t.value.columns for t in at.dataframe)
assert any("Dernier consensus AG1" in str(x.value) for x in at.caption)
print("Portefeuille, NAV, FIFO, activité et filtre des taux vérifiés", len(data["positions"]),count)
print([x.value for x in at.warning if "direct" in x.value][:1])
at.button(key="nav_Chaîne").click().run()
assert not at.exception, [e.message for e in at.exception]
assert any(h.value == "Rapprochement comptable IBKR" for h in at.subheader)
print("Pilotage global et rapprochement vérifiés")
