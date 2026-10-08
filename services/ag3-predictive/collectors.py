import csv
import io
import json
import os
from pathlib import Path
import re
import unicodedata
from urllib.parse import urljoin
from datetime import date, timedelta, datetime
from http_client import FetchError
from normalization import sec_facts, esef_facts, numeric
from store import db, now, coverage, ROOT


def save_facts(rows):
    with db() as c:
        c.executemany('INSERT OR REPLACE INTO facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', rows)


def import_universe():
    import duckdb
    path=os.environ.get('UNIVERSE_DB','/source/ag2_v3.duckdb')
    with duckdb.connect(path,read_only=True) as c:
        c.execute('SET threads=1')
        rows=c.execute('''SELECT u.symbol,coalesce(nullif(u.symbol_yahoo,''),u.symbol),u.name,
            u.country,u.currency,u.isin,u.sector,u.asset_class,
            EXISTS(SELECT 1 FROM universe_segments s WHERE s.symbol=u.symbol AND s.segment='HELD')
            FROM universe u WHERE EXISTS(SELECT 1 FROM universe_segments s WHERE s.symbol=u.symbol)
            ORDER BY 9 DESC,u.symbol''').fetchall()
        columns=[r[0] for r in c.execute('DESCRIBE universe_quarantine').fetchall()]
        active='is_active' if 'is_active' in columns else 'active'
        quarantined={r[0] for r in c.execute('SELECT symbol FROM universe_quarantine WHERE '+active+'=true').fetchall()}
    imported_at=now()
    with db() as c:
        # Preserve verified IDs; current universe is a survivorship-limited snapshot.
        c.executemany('''INSERT INTO instruments(symbol,yahoo,name,country,currency,isin,sector,asset_class,held,quarantined,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET yahoo=excluded.yahoo,name=excluded.name,
            country=excluded.country,currency=excluded.currency,isin=excluded.isin,sector=excluded.sector,
            asset_class=excluded.asset_class,held=excluded.held,quarantined=excluded.quarantined,updated_at=excluded.updated_at''',
            [tuple(r)+(int(r[0] in quarantined),imported_at) for r in rows])
    return len(rows)


def collect_prices(client, instrument):
    symbol=instrument['symbol']
    with db(True) as c:
        latest=c.execute('SELECT max(day),max(observed_at) FROM prices WHERE symbol=?',(symbol,)).fetchone()
        prior=c.execute("SELECT status FROM coverage WHERE symbol=? AND pipe='prices'",(symbol,)).fetchone()
    if (latest[1] and latest[1][:10]==date.today().isoformat() and prior and prior[0]=='OK'
            and os.getenv('AG3_PRICE_FORCE_REFRESH')!='1'):
        return
    # Monthly full refresh updates all historical adjustment factors. On other
    # days retain a 14-day overlap and avoid archiving 16 years again per day.
    start='2010-01-01'
    if latest[0] and latest[1] and latest[1][:7]==date.today().isoformat()[:7]:
        start=(date.fromisoformat(latest[0])-timedelta(days=14)).isoformat()
    payload,digest=client.get('yahoo',os.getenv('YAHOO_URL','http://yfinance-api:8080')+'/research/history',
        {'symbol':instrument.get('yahoo',symbol),'start':start,'end':date.today().isoformat()},ttl=72000)
    if not payload.get('ok') or payload.get('contract')!='YF_RESEARCH_HISTORY_V1':
        raise FetchError('INVALID_YAHOO_CONTRACT')
    currency=payload.get('currency')
    if not currency:
        raise FetchError('YAHOO_CURRENCY_MISSING')
    if start!='2010-01-01':
        with db(True) as c:
            existing={r[0]:r[1] for r in c.execute('SELECT day,adj_close FROM prices WHERE symbol=? AND day>=?',(symbol,start))}
        changed=any(b['date'] in existing and numeric(b.get('adj_close')) is not None and
                    abs(b['adj_close']-existing[b['date']])>max(1e-6,abs(existing[b['date']])*1e-7)
                    for b in payload['bars'])
        if changed:
            payload,digest=client.get('yahoo',os.getenv('YAHOO_URL','http://yfinance-api:8080')+'/research/history',
                {'symbol':instrument.get('yahoo',symbol),'start':'2010-01-01','end':date.today().isoformat()},ttl=0)
            if not payload.get('ok') or payload.get('currency')!=currency:
                raise FetchError('ADJUSTMENT_REFRESH_FAILED')
    rows=[]
    rejected=0
    invalid_days=[]
    for b in payload['bars']:
        close, adj=numeric(b.get('close')),numeric(b.get('adj_close'))
        if close is None or adj is None or close<=0 or adj<=0:
            rejected+=1
            invalid_days.append((symbol,b['date']))
            continue
        rows.append((symbol,b['date'],close,adj,numeric(b.get('volume')),numeric(b.get('dividends')),
                     numeric(b.get('splits')),currency,payload['fetched_at'],digest))
    if not rows:
        raise FetchError('NO_VALID_PRICES')
    with db() as c:
        c.executemany('DELETE FROM prices WHERE symbol=? AND day=?',invalid_days)
        c.executemany('INSERT OR REPLACE INTO prices VALUES(?,?,?,?,?,?,?,?,?,?)',rows)
    coverage(symbol,'prices','OK' if not rejected else 'PARTIAL',f'{len(rows)} rows; {rejected} rejected; {currency}')


def sec_index(client):
    # Seed CIKs are candidates, never accepted without a ticker check below.
    seeds=json.loads(Path('identity_seeds.json').read_text())
    try:
        payload,_=client.get('sec','https://www.sec.gov/files/company_tickers.json',ttl=7*86400)
        seeds.update({r['ticker']:str(r['cik_str']) for r in payload.values()})
        coverage('*','sec_index','OK',f'{len(seeds)} candidates')
    except FetchError as e:
        coverage('*','sec_index','PARTIAL',str(e)+'; validated per-company seeds available')
    return seeds


def collect_sec(client, instrument, seeds):
    symbol=instrument['symbol']
    candidate=instrument.get('cik') or seeds.get(instrument.get('yahoo',symbol))
    if not candidate:
        coverage(symbol,'sec','NO_MAPPING')
        return
    cik=str(candidate).zfill(10)
    sub,sub_hash=client.get('sec',f'https://data.sec.gov/submissions/CIK{cik}.json')
    normalize=lambda t:t.replace('.','-').upper()
    if not sub.get('tickers'):
        coverage(symbol,'sec','IDENTITY_UNVERIFIED','SEC profile has no ticker: '+sub.get('name',''))
        return
    if normalize(instrument.get('yahoo',symbol)) not in {normalize(t) for t in sub.get('tickers',[])}:
        coverage(symbol,'sec','IDENTITY_MISMATCH',sub.get('name',''))
        return
    with db() as c:
        c.execute('UPDATE instruments SET cik=?,identity_source=? WHERE symbol=?',(cik,'SEC_SUBMISSIONS_TICKER',symbol))
    records=[sub.get('filings',{}).get('recent',{})]
    for old in sub.get('filings',{}).get('files',[]):
        if old.get('filingTo','') < '2009-01-01':
            continue
        name=old.get('name','')
        if re.fullmatch(r'CIK[0-9]+-submissions-[0-9]+\.json',name):
            records.append(client.get('sec','https://data.sec.gov/submissions/'+name,ttl=30*86400)[0])
    accepted={}
    for block in records:
        accepted.update(dict(zip(block.get('accessionNumber',[]),block.get('acceptanceDateTime',[]))))
    payload,digest=client.get('sec',f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json')
    rows=sec_facts(symbol,payload,accepted,digest)
    save_facts(rows)
    filings={(r[0],r[2],symbol,r[6],r[7],r[8],digest) for r in rows}
    with db() as c:
        c.executemany('INSERT OR REPLACE INTO filings VALUES(?,?,?,?,?,?,?)',sorted(filings))
    coverage(symbol,'sec','OK' if rows else 'NO_USABLE_FACTS',f'{len(rows)} annual/consolidated facts')


def clean_name(name):
    name=unicodedata.normalize('NFKD',name or '').encode('ascii','ignore').decode().upper()
    words=re.findall('[A-Z0-9]+',name)
    return ' '.join(w for w in words if w not in {'SA','SE','NV','PLC','INC','CORPORATION','CORP','LIMITED','LTD','THE'})


def esef_index(client):
    from store import atomic_json
    from collections import defaultdict
    index=defaultdict(list)
    url='https://filings.xbrl.org/api/entities?page%5Bsize%5D=200'
    pages=0
    while url and pages<100:
        payload,_=client.get('esef',url,ttl=7*86400)
        for row in payload.get('data',[]):
            a=row['attributes']
            key=clean_name(a.get('name'))
            if key and re.fullmatch('[A-Z0-9]{20}',a.get('identifier','')):
                index[key].append(a['identifier'])
        url=payload.get('links',{}).get('next')
        if url:
            url=urljoin('https://filings.xbrl.org',url)
        pages+=1
    if url:
        raise FetchError('ESEF_INDEX_PAGINATION_INCOMPLETE')
    atomic_json(ROOT/'esef_identity_index.json',{k:sorted(set(v)) for k,v in index.items()})
    coverage('*','esef_index','OK',f'{len(index)} normalized legal names, {pages} pages')


def collect_esef(client,instrument):
    symbol=instrument['symbol']
    lei=instrument.get('lei')
    index_path=ROOT/'esef_identity_index.json'
    if not lei and index_path.exists():
        index=json.loads(index_path.read_text())
        matches=index.get(clean_name(instrument['name']),[])
        if len(matches)==1:
            lei=matches[0]
    if not lei:
        isin=instrument.get('isin') or ''
        if isin:
            j,_=client.get('gleif','https://api.gleif.org/api/v1/lei-records',{'filter[isin]':isin},ttl=30*86400)
            matches=[r for r in j.get('data',[]) if clean_name(r['attributes']['entity']['legalName']['name'])==clean_name(instrument['name'])]
            if len(matches)==1:
                lei=matches[0]['id']
        if not lei:
            j,_=client.get('esef','https://filings.xbrl.org/api/entities',{'filter[name]':instrument['name'].upper()},ttl=7*86400)
            matches=[r for r in j.get('data',[]) if clean_name(r['attributes']['name'])==clean_name(instrument['name'])]
            if len(matches)==1:
                lei=matches[0]['attributes']['identifier']
        if not lei:
            coverage(symbol,'esef','NO_VERIFIED_LEI')
            return
        if not re.fullmatch('[A-Z0-9]{20}',lei):
            raise FetchError('INVALID_LEI')
        with db() as c:
            c.execute('UPDATE instruments SET lei=? WHERE symbol=?',(lei,symbol))
    with db() as c:
        c.execute('UPDATE instruments SET lei=? WHERE symbol=?',(lei,symbol))
    url=f'https://filings.xbrl.org/api/entities/{lei}/filings?page%5Bsize%5D=200'
    entries=[]
    for _ in range(50):
        try:
            j,_=client.get('esef',url)
        except FetchError as exc:
            if str(exc).endswith('HTTP_404'):
                coverage(symbol,'esef','NO_CATALOG_FILINGS',lei)
                return
            raise
        entries.extend(j.get('data',[]))
        url=j.get('links',{}).get('next')
        if not url:
            break
        url=urljoin('https://filings.xbrl.org',url)
    overrides=ROOT/'verified_publications.json'
    publication_dates=json.loads(overrides.read_text()) if overrides.exists() else {}
    total=0
    for filing in entries:
        a=filing['attributes']
        fid=a['fxo_id']
        if not a.get('json_url') or a.get('error_count',0):
            continue
        payload,digest=client.get('esef',urljoin('https://filings.xbrl.org',a['json_url']),ttl=365*86400)
        publication=publication_dates.get(fid)
        if publication:
            try:
                verified_time=datetime.fromisoformat(publication.get('available_at','').replace('Z','+00:00'))
                valid=(verified_time.tzinfo is not None and publication.get('raw_hash')==digest
                       and str(publication.get('source_url','')).startswith('https://'))
            except ValueError:
                valid=False
            if not valid:
                publication=None
        rows=esef_facts(symbol,fid,payload,digest,publication)
        save_facts(rows)
        total+=len(rows)
        with db() as c:
            c.execute('INSERT OR REPLACE INTO filings VALUES(?,?,?,?,?,?,?)',
                ('esef',fid,symbol,a['period_end'],publication.get('available_at') if publication else None,
                 'VERIFIED_PUBLICATION' if publication else 'UNKNOWN_PUBLICATION',digest))
    coverage(symbol,'esef','COLLECTED_UNDATED' if total else 'NO_USABLE_FACTS',f'{len(entries)} reports; {total} numeric annual facts; publication dates require verification')


def collect_ecb(client):
    payload,digest=client.get('ecb','https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A',
        {'startPeriod':'2010-01-01','format':'csvdata'},as_json=False)
    rows=[('USD',r['TIME_PERIOD'],numeric(r['OBS_VALUE']),digest) for r in csv.DictReader(io.StringIO(payload.decode()))]
    rows=[r for r in rows if r[2] is not None and r[2]>0]
    with db() as c:
        c.executemany('INSERT OR REPLACE INTO fx VALUES(?,?,?,?)',rows)
    coverage('*','ecb','OK' if rows else 'EMPTY',f'{len(rows)} EUR/USD observations')


def collect_fred(client):
    key=os.getenv('FRED_API_KEY')
    if not key:
        coverage('*','alfred','MISSING_KEY')
        return
    for series in ['FEDFUNDS','CPIAUCSL','UNRATE']:
        offset=0
        total=0
        while True:
            payload,digest=client.get('alfred','https://api.stlouisfed.org/fred/series/observations',
                {'api_key':key,'series_id':series,'file_type':'json','observation_start':'2010-01-01',
                 'realtime_start':'2010-01-01','realtime_end':date.today().isoformat(),'output_type':1,
                 'limit':100000,'offset':offset})
            observations=payload.get('observations',[])
            rows=[(series,r['date'],r['realtime_start'],numeric(r['value']),digest) for r in observations]
            with db() as c:
                c.executemany('INSERT OR REPLACE INTO macro VALUES(?,?,?,?,?)',rows)
            total+=len(rows)
            offset+=len(observations)
            if offset>=payload.get('count',0) or not observations:
                break
        coverage(series,'alfred','OK' if total else 'EMPTY',f'{total} observations/vintages')
