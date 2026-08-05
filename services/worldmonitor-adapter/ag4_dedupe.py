"""Lecture seule des empreintes AG4 pour la lineage, sans écrire sa base."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import duckdb

from normalizer import event_fingerprint


def load_ag4_fingerprints(window_days: int = 14) -> set[str]:
    path = os.environ.get("AG4_SPE_DUCKDB_PATH", "/files/duckdb/ag4_spe_v2.duckdb")
    if not os.path.isfile(path):
        return set()
    with duckdb.connect(path, read_only=True) as con:
        columns = {row[1] for row in con.execute("PRAGMA table_info('news_history')").fetchall()}
        title_col = next((name for name in ("title", "headline") if name in columns), None)
        time_col = next((name for name in ("published_at", "publishedAt", "published_date", "created_at") if name in columns), None)
        url_col = next((name for name in ("url", "article_url", "link") if name in columns), None)
        if not title_col or not time_col:
            return set()
        url_expr = url_col if url_col else "NULL"
        cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
        rows = con.execute(
            f"SELECT {title_col}, {time_col}, {url_expr} FROM news_history WHERE TRY_CAST({time_col} AS TIMESTAMP) >= ?",
            [cutoff],
        ).fetchall()
    return {event_fingerprint(title, event_time, [], url) for title, event_time, url in rows}
