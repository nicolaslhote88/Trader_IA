import duckdb
from datetime import datetime

DB = "/files/duckdb/ag4_spe_v2.duckdb"
con = duckdb.connect(DB, read_only=True)
last = con.execute("SELECT max(started_at) FROM run_log WHERE status='SUCCESS'").fetchone()[0]
z = con.execute("SELECT count(*) FROM run_log WHERE status='RUNNING' AND started_at < now()-INTERVAL '1 hour'").fetchone()[0]
tot = con.execute("SELECT count(*) FROM news_history").fetchone()[0]
bad = con.execute("SELECT count(*) FROM news_history WHERE published_at IS NOT NULL AND (published_at < now()-INTERVAL '730 days' OR published_at > now()+INTERVAL '7 days')").fetchone()[0]
con.close()

now = datetime.utcnow()
issues = []
if last is None:
    issues.append("Aucun run SUCCESS en base.")
else:
    ah = (now - last).total_seconds() / 3600.0
    if ah > 8:
        issues.append("Dernier run SUCCESS il y a %.0fh (>8h) - pipeline bloque ?" % ah)
if z > 0:
    issues.append("%d run(s) zombies RUNNING>1h." % z)
ratio = (100.0 * bad / tot) if tot else 0.0
if ratio > 5:
    issues.append("%.1f%% de published_at hors plage (regression dates)." % ratio)

if not issues:
    return []
text = "<b>AG4_Spe-V2 - alerte sante</b>\n" + "\n".join("- " + i for i in issues)
return [{"json": {"text": text}}]
