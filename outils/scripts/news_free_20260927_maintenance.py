#!/usr/bin/env python3
"""Disable only the two FXStreet feeds returning HTTP 403. Backup DB required before use."""
import argparse
import json
from free_news_common import connect
IDS=['site-bourse-fxstreet-news','site-bourse-fxstreet-analysis']
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--db',required=True);args=ap.parse_args()
    con=connect(args.db)
    try:
        before=con.execute('SELECT source_id,url,enabled FROM cfg.ag4_rss_sources WHERE source_id IN (?,?) ORDER BY source_id',IDS).fetchall()
        assert len(before)==2 and all(r[1].startswith('https://www.fxstreet.com/rss/') for r in before)
        con.execute('BEGIN')
        con.execute('UPDATE cfg.ag4_rss_sources SET enabled=false,updated_at=now() WHERE source_id IN (?,?)',IDS)
        after=con.execute('SELECT source_id,enabled FROM cfg.ag4_rss_sources WHERE source_id IN (?,?) ORDER BY source_id',IDS).fetchall()
        assert all(r[1] is False for r in after)
        con.execute('COMMIT')
        print(json.dumps({'before':before,'after':after}))
    finally:con.close()
if __name__=='__main__':main()
