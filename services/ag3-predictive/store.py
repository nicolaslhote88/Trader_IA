"""Dedicated SQLite research store. Live DuckDB files are never written."""
import contextlib
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from datetime import datetime, timezone

ROOT = Path(os.getenv('AG3_DATA_DIR', '/data'))


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, allow_nan=False, default=str), encoding='utf-8')
    os.replace(temp, path)


def archive(body):
    digest = hashlib.sha256(body).hexdigest()
    path = ROOT / 'raw' / digest[:2] / (digest + '.gz')
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with gzip.open(path, 'wb') as f:
            f.write(body)
    return digest


@contextlib.contextmanager
def db(readonly=False):
    ROOT.mkdir(parents=True, exist_ok=True)
    path = ROOT / 'research.sqlite'
    c = sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=30) if readonly else sqlite3.connect(path, timeout=30)
    c.row_factory = sqlite3.Row
    try:
        if not readonly:
            c.execute('PRAGMA journal_mode=WAL')
        yield c
        if not readonly:
            c.commit()
    except Exception:
        if not readonly:
            c.rollback()
        raise
    finally:
        c.close()


def initialize():
    with db() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS instruments(symbol TEXT PRIMARY KEY, yahoo TEXT, name TEXT,
            country TEXT, currency TEXT, isin TEXT, sector TEXT, asset_class TEXT, held INTEGER,
            quarantined INTEGER, cik TEXT, lei TEXT, identity_source TEXT, updated_at TEXT);
        CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY, source TEXT, request_key TEXT,
            observed_at TEXT, status INTEGER, digest TEXT);
        CREATE INDEX IF NOT EXISTS request_cache ON requests(request_key, observed_at);
        CREATE TABLE IF NOT EXISTS coverage(symbol TEXT, pipe TEXT, status TEXT, detail TEXT,
            updated_at TEXT, PRIMARY KEY(symbol,pipe));
        CREATE TABLE IF NOT EXISTS filings(source TEXT, filing_id TEXT, symbol TEXT, period_end TEXT,
            available_at TEXT, date_quality TEXT, raw_hash TEXT, PRIMARY KEY(source,filing_id,symbol));
        CREATE TABLE IF NOT EXISTS facts(source TEXT, symbol TEXT, filing_id TEXT, metric TEXT,
            concept TEXT, period_start TEXT, period_end TEXT, available_at TEXT, date_quality TEXT,
            unit TEXT, value REAL, raw_hash TEXT,
            PRIMARY KEY(source,symbol,filing_id,concept,period_start,period_end,unit));
        CREATE INDEX IF NOT EXISTS facts_asof ON facts(symbol, available_at, period_end);
        CREATE TABLE IF NOT EXISTS prices(symbol TEXT, day TEXT, close REAL, adj_close REAL,
            volume REAL, dividend REAL, split REAL, currency TEXT, observed_at TEXT, raw_hash TEXT,
            PRIMARY KEY(symbol,day));
        CREATE TABLE IF NOT EXISTS macro(series TEXT, day TEXT, vintage TEXT, value REAL,
            raw_hash TEXT, PRIMARY KEY(series,day,vintage));
        CREATE TABLE IF NOT EXISTS fx(currency TEXT, day TEXT, units_per_eur REAL, raw_hash TEXT,
            PRIMARY KEY(currency,day));
        CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT,
            status TEXT, report_json TEXT);
        ''')


def coverage(symbol, pipe, status, detail=''):
    with db() as c:
        c.execute('INSERT OR REPLACE INTO coverage VALUES(?,?,?,?,?)',
                  (symbol, pipe, status, str(detail)[:1000], now()))


def status():
    with db(True) as c:
        counts = {t: c.execute('SELECT count(*) FROM ' + t).fetchone()[0]
                  for t in ['instruments','filings','facts','prices','macro','fx']}
        summary = [dict(r) for r in c.execute('SELECT pipe,status,count(*) n FROM coverage GROUP BY pipe,status')]
        gaps = [dict(r) for r in c.execute("SELECT * FROM coverage WHERE status NOT IN ('OK','COLLECTED_UNDATED') ORDER BY pipe,symbol")]
        runs = [dict(r) for r in c.execute('SELECT run_id,started_at,ended_at,status FROM runs ORDER BY started_at DESC LIMIT 5')]
    model = ROOT / 'model_report.json'
    return {'contract':'AG3_RESEARCH_STATUS_V1', 'mode':'SHADOW', 'decision_enabled':False,
            'generated_at':now(), 'counts':counts, 'coverage':summary, 'gaps':gaps, 'runs':runs,
            'model':json.loads(model.read_text()) if model.exists() else {'status':'NOT_TRAINED'}}
