"""Utilities for free collectors. Production writes require DuckDB 1.4.3."""
import hashlib
import json
import time
from datetime import datetime, timezone

STAGING_DDL = """CREATE TABLE IF NOT EXISTS news_finnhub_staging (
 news_id VARCHAR PRIMARY KEY, symbol VARCHAR, company_name VARCHAR, source VARCHAR,
 url VARCHAR, canonical_url VARCHAR, title VARCHAR, published_at TIMESTAMP, published_at_raw VARCHAR,
 snippet VARCHAR, category VARCHAR, sentiment VARCHAR, provider VARCHAR, news_article_id VARCHAR,
 status VARCHAR, first_seen_at TIMESTAMP, fetched_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"""
HEALTH_DDL = """CREATE TABLE IF NOT EXISTS news_collection_runs (
 run_id VARCHAR PRIMARY KEY, source VARCHAR, finished_at TIMESTAMP, status VARCHAR,
 symbols INTEGER, received INTEGER, inserted INTEGER, errors VARCHAR)"""
COLUMNS = ['news_id','symbol','company_name','source','url','canonical_url','title','published_at',
 'published_at_raw','snippet','category','sentiment','provider','news_article_id','status','first_seen_at','fetched_at']

def news_key(source, article_id, symbol):
    return hashlib.sha1(f'{source}|{article_id}|{symbol}'.encode()).hexdigest()

def connect(path, read_only=False):
    import duckdb
    if not read_only and duckdb.__version__ != '1.4.3':
        raise RuntimeError('Writer requires DuckDB 1.4.3')
    for attempt in range(60):
        try:
            return duckdb.connect(str(path), read_only=read_only)
        except Exception as exc:
            if 'lock' not in str(exc).lower() or attempt == 59:
                raise
            time.sleep(2)

def store_batch(path, source, rows, symbols, errors, gaps=None):
    now = datetime.now(timezone.utc).isoformat()
    con = connect(path)
    inserted = 0
    try:
        con.execute('BEGIN')
        con.execute(STAGING_DDL)
        con.execute(HEALTH_DDL)
        # Bounded staging, keeping failures eligible for retry for 14 days.
        con.execute("DELETE FROM news_finnhub_staging WHERE published_at < now()-INTERVAL '14 days' OR news_id IN (SELECT news_id FROM news_history)")
        for row in rows:
            # Compatible with old hashes; retain every issuer link for shared stories.
            exists = con.execute("""SELECT 1 FROM news_history WHERE source=? AND news_article_id=? AND symbol=?
              UNION ALL SELECT 1 FROM news_finnhub_staging WHERE source=? AND news_article_id=? AND symbol=? LIMIT 1""",
              [row['source'],row['news_article_id'],row['symbol']]*2).fetchone()
            if exists:
                continue
            con.execute('INSERT INTO news_finnhub_staging ('+','.join(COLUMNS)+') VALUES ('+','.join('?' for _ in COLUMNS)+') ON CONFLICT DO NOTHING', [row.get(c) for c in COLUMNS])
            inserted += 1
        status = 'PARTIAL' if errors else 'SUCCESS'
        con.execute('INSERT INTO news_collection_runs VALUES (?,?,?,?,?,?,?,?)',
                    [source+'_'+now,source,now,status,symbols,len(rows),inserted,json.dumps({"errors":errors,"unsupported_symbols":gaps or []})])
        con.execute("DELETE FROM news_collection_runs WHERE finished_at < now()-INTERVAL '60 days'")
        con.execute('COMMIT')
    except Exception:
        con.execute('ROLLBACK')
        raise
    finally:
        con.close()
    return {'source':source,'status':status,'symbols':symbols,'received':len(rows),'inserted':inserted,'errors':errors,'unsupported_symbols':gaps or []}
