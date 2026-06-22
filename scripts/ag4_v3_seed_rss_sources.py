#!/usr/bin/env python3
"""Seed AG4-V3 RSS source configuration into DuckDB.

This replaces the former external spreadsheet configuration with
cfg.ag4_rss_sources inside ag4_v3.duckdb.
"""
import argparse
from datetime import datetime
from datetime import timezone

import duckdb

DEFAULT_DB = "/files/duckdb/ag4_v3.duckdb"

RSS_SOURCES = [
    ("institution-amf-france-offres-operations", "Institution", "AMF France", "Offres & Opérations", "https://www.amf-france.org/fr/flux-rss/display/29", 5, 1, True),
    ("macro-eco-banque-centrale-bce-communiques-de-presse", "Macro-Eco", "Banque Centrale (BCE)", "Communiqués de Presse", "https://www.ecb.europa.eu/rss/press.html", 5, 1, True),
    ("institution-amf-france-sanctions", "Institution", "AMF France", "Sanctions", "https://www.amf-france.org/fr/flux-rss/display/24", 3, 2, True),
    ("site-bourse-investing-com-actualites-generales", "Site Bourse", "Investing.com", "Actualités Générales", "https://fr.investing.com/rss/news.rss", 3, 2, True),
    ("site-bourse-investing-com-forex", "Site Bourse", "Investing.com", "Forex", "https://fr.investing.com/rss/news_1.rss", 3, 2, True),
    ("media-eco-bfm-business-economie", "Média Eco", "BFM Business", "Économie", "https://www.bfmtv.com/rss/economie/", 3, 2, True),
    ("site-bourse-easybourse-media-articles", "Site Bourse", "EasyBourse", "Média / Articles", "https://www.easybourse.com/feeds/media/", 2, 2, True),
    ("banque-centrale-bank-of-england-news", "Banque Centrale", "Bank of England", "News", "https://www.bankofengland.co.uk/rss/news", 5, 1, True),
    ("banque-centrale-bank-of-england-publications", "Banque Centrale", "Bank of England", "Publications", "https://www.bankofengland.co.uk/rss/publications", 3, 2, True),
    ("banque-centrale-bank-of-japan-what-s-new", "Banque Centrale", "Bank of Japan", "What's New", "https://www.boj.or.jp/en/rss/whatsnew.xml", 4, 1, True),
    ("presse-fin-capital-entreprises-marches", "Presse Fin.", "Capital", "Entreprises & Marchés", "https://feed.prismamediadigital.com/v1/cap/rss?sources=capital,polemik,xerfi,capital-avec-agence-france-presse,capital-avec-aof,capital-avec-reuters&categories=entreprises-marches", 4, 1, True),
    ("presse-fin-le-monde-economie", "Presse Fin.", "Le Monde", "Économie", "https://www.lemonde.fr/economie/rss_full.xml", 4, 1, True),
    ("presse-fin-le-monde-economie-mondiale", "Presse Fin.", "Le Monde", "Économie Mondiale", "https://www.lemonde.fr/economie-mondiale/rss_full.xml", 3, 2, True),
    ("site-bourse-fxstreet-news", "Site Bourse", "FXStreet", "News", "https://www.fxstreet.com/rss/news", 4, 1, True),
    ("site-bourse-fxstreet-analysis", "Site Bourse", "FXStreet", "Analysis", "https://www.fxstreet.com/rss/analysis", 3, 2, True),
    ("secteur-tech-journal-du-net-index-general", "Secteur Tech", "Journal du Net", "Index général", "https://www.journaldunet.com/rss/index.xml", 3, 2, True),
    ("secteur-tech-numerama-articles", "Secteur Tech", "Numerama", "Articles", "https://www.numerama.com/feed/", 2, 2, True),
    ("secteur-tech-clubic-news", "Secteur Tech", "Clubic", "News", "https://www.clubic.com/feed/news.rss", 2, 2, True),
    ("banque-centrale-federal-reserve-press-all", "Banque Centrale", "Federal Reserve", "Press All", "https://www.federalreserve.gov/feeds/press_all.xml", 5, 1, True),
    ("banque-centrale-federal-reserve-press-monetary-policy", "Banque Centrale", "Federal Reserve", "Press Monetary Policy", "https://www.federalreserve.gov/feeds/press_monetary.xml", 5, 1, True),
    ("banque-centrale-federal-reserve-speeches", "Banque Centrale", "Federal Reserve", "Speeches", "https://www.federalreserve.gov/feeds/speeches.xml", 4, 1, True),
    ("banque-centrale-swiss-national-bank-news", "Banque Centrale", "Swiss National Bank", "News", "https://www.snb.ch/public/en/rss/news", 3, 2, True),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    con = duckdb.connect(args.db)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS cfg")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS cfg.ag4_rss_sources (
              source_id VARCHAR PRIMARY KEY,
              family VARCHAR NOT NULL,
              source VARCHAR NOT NULL,
              feed_name VARCHAR NOT NULL,
              url VARCHAR NOT NULL,
              interest INTEGER NOT NULL,
              source_tier INTEGER NOT NULL,
              enabled BOOLEAN NOT NULL DEFAULT TRUE,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        con.executemany(
            """
            INSERT OR REPLACE INTO cfg.ag4_rss_sources (
              source_id, family, source, feed_name, url, interest, source_tier,
              enabled, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(
              (SELECT created_at FROM cfg.ag4_rss_sources WHERE source_id = ?),
              ?
            ), ?)
            """,
            [row + (row[0], now, now) for row in RSS_SOURCES],
        )
        con.execute("CHECKPOINT")
        count = con.execute("SELECT count(*) FROM cfg.ag4_rss_sources").fetchone()[0]
        enabled = con.execute("SELECT count(*) FROM cfg.ag4_rss_sources WHERE enabled").fetchone()[0]
        print(f"seeded cfg.ag4_rss_sources: {count} rows, {enabled} enabled")
    finally:
        con.close()


if __name__ == "__main__":
    main()
