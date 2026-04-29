import os
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag4_v3_path") or "/files/duckdb/ag4_v3.duckdb"
lookback = int(ctx.get("lookback_hours") or 24)
items = []

if os.path.exists(path):
    try:
        with duckdb.connect(path, read_only=True) as con:
            cols = {r[1].lower(): r[1] for r in con.execute("PRAGMA table_info('main.news_history')").fetchall()}
            def col(*names):
                for n in names:
                    if n.lower() in cols:
                        return cols[n.lower()]
                return None

            published = col("published_at", "publishedAt", "date", "created_at") or "created_at"
            title = col("title", "Title") or "title"
            source = col("source", "Source") or "source"
            snippet = col("snippet", "strategic_summary", "Strategy", "summary") or title
            dedupe = col("dedupe_key", "dedupeKey", "event_key", "id") or title
            impact_class = col("impact_asset_class") or "impact_asset_class"
            pairs = col("impact_fx_pairs") or "impact_fx_pairs"
            mag = col("impact_magnitude") or "impact_magnitude"
            bull = col("currencies_bullish") or "currencies_bullish"
            bear = col("currencies_bearish") or "currencies_bearish"
            hint = col("fx_directional_hint") or "fx_directional_hint"
            query = f"""
                SELECT
                  CAST({dedupe} AS VARCHAR) AS dedupe_key,
                  CAST({published} AS VARCHAR) AS published_at,
                  CAST({title} AS VARCHAR) AS title,
                  CAST({source} AS VARCHAR) AS source,
                  CAST({snippet} AS VARCHAR) AS snippet,
                  CAST({mag} AS VARCHAR) AS impact_magnitude,
                  CAST({pairs} AS VARCHAR) AS impact_fx_pairs,
                  CAST({bull} AS VARCHAR) AS currencies_bullish,
                  CAST({bear} AS VARCHAR) AS currencies_bearish,
                  CAST({hint} AS VARCHAR) AS fx_directional_hint
                FROM main.news_history
                WHERE CAST({published} AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '{lookback} hours'
                  AND (
                    lower(COALESCE(CAST({impact_class} AS VARCHAR), '')) LIKE '%fx%'
                    OR lower(COALESCE(CAST({impact_class} AS VARCHAR), '')) LIKE '%mixed%'
                  )
                ORDER BY CAST({published} AS TIMESTAMP) DESC
                LIMIT 200
            """
            for row in con.execute(query).fetchdf().to_dict("records"):
                row["origin"] = "global_base"
                items.append(row)
    except Exception as exc:
        ctx["global_error"] = str(exc)

return [{"json": {**ctx, "global_news": items, "news_global_pulled": len(items)}}]
