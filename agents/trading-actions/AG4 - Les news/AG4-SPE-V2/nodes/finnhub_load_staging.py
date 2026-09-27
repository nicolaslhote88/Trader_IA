import duckdb
import json
import time
from datetime import datetime, timezone

DB = "/files/duckdb/ag4_spe_v2.duckdb"
RUN_ID = "AG4FREE_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

def connect_ro():
    for attempt in range(8):
        try:
            return duckdb.connect(DB, read_only=True)
        except Exception as e:
            if "lock" not in str(e).lower() or attempt == 7:
                raise
            time.sleep(2)

con = connect_ro()
try:
    rows = con.execute("""
      SELECT s.news_id, s.symbol, s.company_name, s.provider, s.news_article_id,
             s.url, s.title, CAST(s.published_at AS VARCHAR), s.snippet,
             CAST(s.first_seen_at AS VARCHAR), s.source, s.published_at_raw
      FROM news_finnhub_staging s
      WHERE COALESCE(s.status,'PENDING')='PENDING'
        AND s.published_at >= now()-INTERVAL '14 days' AND s.published_at <= now()
        AND NOT EXISTS (SELECT 1 FROM news_history h WHERE h.news_id=s.news_id
          OR (h.source=s.source AND h.news_article_id=s.news_article_id AND h.symbol=s.symbol))
      ORDER BY CASE WHEN s.source='issuer_rss' THEN 0 ELSE 1 END,
               s.published_at DESC, s.symbol, s.news_id LIMIT 400
    """).fetchall()
finally:
    con.close()
out = []
for r in rows:
    news_id, sym, name, provider, art, url, title, pub, snippet, fsa, source, raw = r
    title = str(title or "").strip()
    snippet = str(snippet or "").strip() or title
    if not title or not url:
        continue
    contract = {"provider_published_at": pub, "timestamp_quality": "PROVIDER_REPORTED", "raw": raw}
    out.append({"json": {
        "run_id": RUN_ID, "db_path": DB, "newsId": news_id, "symbol": sym,
        "company_name": name or sym, "companyName": name or sym, "isin": None,
        "source": source, "provider": provider, "newsArticleId": art, "reason": source,
        "url": url, "articleUrl": url, "articleCanonicalUrl": url, "canonicalUrl": url,
        "title": title, "articleTitle": title, "publishedAt": pub, "snippet": snippet, "text": snippet,
        "providerTimeContract": contract, "firstSeenAt": fsa,
        "_reason": source, "_runAI": True, "_articlesLoopReset": False,
        "llmInput": json.dumps({"source": source, "title": title, "date": pub,
                                "provider": provider, "snippet": snippet, "content": snippet}),
    }})
return out
