import gzip
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import requests
from store import ROOT, archive, db, now

ALLOWED = {'data.sec.gov','www.sec.gov','filings.xbrl.org','api.gleif.org',
           'data-api.ecb.europa.eu','api.stlouisfed.org','api.edinet-fsa.go.jp',
           'opendart.fss.or.kr','yfinance-api','yf-ag3-shadow','127.0.0.1','localhost'}
SECRET_FIELDS = {'api_key','apikey','Subscription-Key','crtfc_key'}


class FetchError(RuntimeError):
    pass


class Client:
    def __init__(self):
        self.session = requests.Session()
        self.last = {}

    def get(self, source, url, params=None, ttl=86400, as_json=True):
        if urlparse(url).hostname not in ALLOWED:
            raise FetchError('UNAPPROVED_SOURCE_HOST')
        req = requests.Request('GET', url, params=params).prepare()
        parsed = urlparse(req.url)
        safe_query = urlencode([(k,v) for k,v in parse_qsl(parsed.query) if k not in SECRET_FIELDS])
        safe_url = urlunparse(parsed._replace(query=safe_query))
        key = hashlib.sha256((source + safe_url).encode()).hexdigest()
        with db(True) as c:
            row = c.execute('SELECT observed_at,digest FROM requests WHERE request_key=? AND status=200 ORDER BY id DESC LIMIT 1',(key,)).fetchone()
        if row and (datetime.now(timezone.utc)-datetime.fromisoformat(row['observed_at'])).total_seconds() < ttl:
            path = ROOT/'raw'/row['digest'][:2]/(row['digest']+'.gz')
            if path.exists():
                body = gzip.decompress(path.read_bytes())
                return (json.loads(body) if as_json else body), row['digest']
        host = parsed.hostname
        headers = {'User-Agent':os.getenv('SEC_USER_AGENT','TraderIA/1.0 (personal financial research)')}
        for attempt in range(3):
            time.sleep(max(0, 1.05-(time.monotonic()-self.last.get(host,0))))
            self.last[host] = time.monotonic()
            try:
                r = self.session.get(req.url, headers=headers, timeout=(10,90), allow_redirects=False)
            except requests.RequestException:
                if attempt==2:
                    raise FetchError(source+':NETWORK_FAILURE') from None
                time.sleep(2**attempt)
                continue
            digest = archive(r.content)
            with db() as c:
                c.execute('INSERT INTO requests(source,request_key,observed_at,status,digest) VALUES(?,?,?,?,?)',
                          (source,key,now(),r.status_code,digest))
            if r.status_code==200:
                try:
                    return (r.json() if as_json else r.content), digest
                except ValueError:
                    raise FetchError(source+':INVALID_JSON') from None
            if r.status_code in (429,500,502,503,504) and attempt<2:
                time.sleep(min(30, max(2**attempt, float(r.headers.get('Retry-After','3')) if r.headers.get('Retry-After','3').isdigit() else 3)))
                continue
            raise FetchError(source+':HTTP_'+str(r.status_code))
