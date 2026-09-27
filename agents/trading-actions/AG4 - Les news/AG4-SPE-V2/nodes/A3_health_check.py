import duckdb
import time

issues = []
def connect_ro(path):
    for attempt in range(8):
        try:
            return duckdb.connect(path, read_only=True)
        except Exception as e:
            if "lock" not in str(e).lower() or attempt == 7:
                raise
            time.sleep(2)

for db, label in [("ag4_spe_v2", "Boursorama"), ("ag4_v3", "Macro DeepSeek")]:
    con = None
    try:
        con = connect_ro("/files/duckdb/" + db + ".duckdb")
        age = con.execute("SELECT epoch(now()-max(started_at))/3600 FROM run_log WHERE status IN ('SUCCESS','PARTIAL')").fetchone()[0]
        if age is None or age > 8:
            issues.append(label + ": aucun run termine depuis 8h.")
        max_age = '2 hours' if db == 'ag4_v3' else '1 hour'
        zombies = con.execute("SELECT count(*) FROM run_log WHERE status='RUNNING' AND started_at < now()-CAST(? AS INTERVAL)", [max_age]).fetchone()[0]
        if zombies:
            issues.append(label + ": %d run(s) inacheves >%s." % (zombies, max_age))
        bad = con.execute("SELECT count(*) FROM news_history WHERE published_at > now()+INTERVAL '5 minutes'").fetchone()[0]
        if bad:
            issues.append(label + ": %d dates futures." % bad)
        if db == "ag4_v3":
            last_status = con.execute("SELECT status FROM run_log WHERE status != 'RUNNING' ORDER BY started_at DESC LIMIT 1").fetchone()
            if last_status and last_status[0] in ['PARTIAL', 'FAILED', 'NO_DATA']:
                issues.append("Macro : dernier run " + last_status[0] + ", verifier les erreurs de flux/analyse.")
            total, unknown = con.execute("SELECT count(*),count(*) FILTER(WHERE source='unknown') FROM news_history WHERE first_seen_at > now()-INTERVAL '8 hours'").fetchone()
            if total and unknown/total > .1:
                issues.append("Macro : sources unknown >10%.")
        else:
            pending, oldest = con.execute("""SELECT count(*),max(epoch(now()-s.fetched_at)/3600)
              FROM news_finnhub_staging s WHERE s.published_at >= now()-INTERVAL '14 days'
              AND NOT EXISTS (SELECT 1 FROM news_history h WHERE h.news_id=s.news_id
                OR (h.source=s.source AND h.news_article_id=s.news_article_id AND h.symbol=s.symbol))""").fetchone()
            if pending and oldest > 1.25:
                issues.append("Flux gratuits : %d articles en attente, plus ancien %.1fh." % (pending, oldest))
            for source in ['finnhub','issuer_rss']:
                r = con.execute("SELECT epoch(now()-finished_at)/3600,status,errors FROM news_collection_runs WHERE source=? ORDER BY finished_at DESC LIMIT 1", [source]).fetchone()
                if not r or r[0] > 4 or r[1] != 'SUCCESS':
                    issues.append(source + ": collecte absente/perimee ou partielle.")
            ibkr = con.execute("SELECT epoch(now()-max(fetched_at))/3600 FROM news_history WHERE source='ibkr'").fetchone()[0]
            if ibkr is None or ibkr > 24:
                issues.append("IBKR : aucun article recent depuis 24h (absence de news ou collecte a verifier).")
    except Exception as e:
        issues.append(label + ": controle indisponible (" + type(e).__name__ + ").")
    finally:
        if con is not None:
            con.close()
if not issues:
    return []
text = "<b>AG4 - sante de tous les flux news</b>\n" + "\n".join("- " + i for i in issues)
return [{"json": {"text": text}}]
