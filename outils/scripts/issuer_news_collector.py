#!/usr/bin/env python3
"""Official issuer RSS -> existing staging -> DeepSeek Flash. No subscription or API key."""
import argparse
import html
import json
import re
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from free_news_common import news_key, store_batch

FEEDS = [
 ('DSY.PA','Dassault Systemes','https://www.3ds.com/contents/feed/content/article_press_release'),
 ('9983.T','Fast Retailing','https://www.fastretailing.com/eng/ir/news/index.xml'),
]

def plain(value):
    return re.sub(r'\s+',' ',html.unescape(re.sub(r'<[^>]+>',' ',value or ''))).strip()

def parse_feed(data, symbol, name, feed, now):
    rows=[]
    root=ET.fromstring(data)
    for item in root.findall('.//item'):
        title=plain(item.findtext('title'))
        url=urllib.parse.urljoin(feed,item.findtext('link') or '')
        raw=item.findtext('pubDate') or item.findtext('{http://purl.org/dc/elements/1.1/}date')
        try:
            pub=parsedate_to_datetime(raw) if ',' in (raw or '') else datetime.fromisoformat(raw.replace('Z','+00:00'))
            if pub.tzinfo is None:
                continue
            pub=pub.astimezone(timezone.utc)
        except (ValueError, TypeError, AttributeError):
            continue
        if not title or not url.startswith('https://') or not now-timedelta(days=14) <= pub <= now:
            continue
        snippet=plain(item.findtext('description') or item.findtext('{http://purl.org/rss/1.0/modules/content/}encoded'))[:6000]
        # No inference from inaccessible PDF bodies. Tell the model exactly what is available.
        snippet=snippet or 'Titre du communique officiel uniquement. Ne pas supposer son contenu ni chiffrer un impact non documente.'
        article_id=url+'|'+pub.isoformat()+'|'+title
        rows.append({'news_id':news_key('issuer_rss',article_id,symbol),'symbol':symbol,'company_name':name,
         'source':'issuer_rss','url':url,'canonical_url':url,'title':title,'published_at':pub.isoformat(),
         'published_at_raw':raw,'snippet':snippet,'category':'Issuer release','provider':name+' IR',
         'news_article_id':article_id,'status':'PENDING','first_seen_at':now.isoformat(),'fetched_at':now.isoformat()})
    return sorted(rows,key=lambda r:r['published_at'],reverse=True)[:6]

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--ag4',default='/local-files/duckdb/ag4_spe_v2.duckdb');ap.add_argument('--dry-run',action='store_true');args=ap.parse_args()
    rows=[];errors=[];now=datetime.now(timezone.utc)
    for symbol,name,url in FEEDS:
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'TraderIA-NewsMonitor/1.0','Accept':'application/rss+xml, application/xml, text/xml'})
            with urllib.request.urlopen(req,timeout=25) as response:
                data=response.read(2_000_001)
            if len(data)>2_000_000:
                raise ValueError('Feed too large')
            rows.extend(parse_feed(data,symbol,name,url,now))
        except Exception as exc:
            errors.append({'symbol':symbol,'error':type(exc).__name__,'http':getattr(exc,'code',None)})
    result={'source':'issuer_rss','symbols':len(FEEDS),'received':len(rows),'errors':errors,'dry_run':args.dry_run}
    if not args.dry_run:
        result=store_batch(args.ag4,'issuer_rss',rows,len(FEEDS),errors)
    print(json.dumps(result))
    return 1 if errors else 0

if __name__=='__main__':
    raise SystemExit(main())
