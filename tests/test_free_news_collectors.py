import unittest
import tempfile
from pathlib import Path
from datetime import datetime, timezone
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'outils/scripts'))
from free_news_common import news_key, store_batch, connect
from issuer_news_collector import parse_feed

class FreeNewsTests(unittest.TestCase):
    def test_multi_issuer_key(self):
        self.assertNotEqual(news_key('finnhub','42','AAA'),news_key('finnhub','42','BBB'))
    def test_dates_relative_url_and_missing_content(self):
        xml=b'''<rss><channel>
          <item><title>Release</title><link>/release.pdf</link><pubDate>Fri, 25 Sep 2026 13:00:00 +0900</pubDate></item>
          <item><title>Future</title><link>/future</link><pubDate>Fri, 25 Sep 2027 13:00:00 +0900</pubDate></item>
          <item><title>Unknown date</title><link>/unknown</link></item>
          <item><title>Old</title><link>/old</link><pubDate>Fri, 01 Jan 2021 13:00:00 +0900</pubDate></item>
        </channel></rss>'''
        rows=parse_feed(xml,'TEST','Test','https://issuer.example/rss',datetime(2026,9,27,tzinfo=timezone.utc))
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['url'],'https://issuer.example/release.pdf')
        self.assertIn('Titre du communique',rows[0]['snippet'])
        self.assertTrue(rows[0]['published_at'].endswith('+00:00'))
    def test_staging_idempotence_legacy_key_and_issuer_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'news.duckdb'
            c=connect(path)
            c.execute('CREATE TABLE news_history(news_id VARCHAR,source VARCHAR,news_article_id VARCHAR,symbol VARCHAR)')
            c.execute("INSERT INTO news_history VALUES ('old_hash','finnhub','42','AAA')")
            c.close()
            now=datetime.now(timezone.utc).isoformat()
            rows=[{'news_id':news_key('finnhub','42',sym),'source':'finnhub','news_article_id':'42','symbol':sym,'published_at':now,'fetched_at':now} for sym in ['AAA','BBB']]
            self.assertEqual(store_batch(path,'finnhub',rows,2,[])['inserted'],1)
            self.assertEqual(store_batch(path,'finnhub',rows,2,[])['inserted'],0)
            c=connect(path,read_only=True)
            self.assertEqual(c.execute('SELECT symbol FROM news_finnhub_staging').fetchall(),[('BBB',)])
            c.close()
if __name__=='__main__':unittest.main()
