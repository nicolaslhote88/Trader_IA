import os
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag4_forex_path") or "/files/duckdb/ag4_forex_v1.duckdb"
lookback = int(ctx.get("lookback_hours") or 24)
news = []
macro = {}
pairs_rows = []

if os.path.exists(path):
    try:
        with duckdb.connect(path, read_only=True) as con:
            tables = {r[0].lower() for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
            if "fx_news_history" in tables:
                df = con.execute(
                    f"""
                    SELECT
                      CAST(dedupe_key AS VARCHAR) AS dedupe_key,
                      CAST(published_at AS VARCHAR) AS published_at,
                      CAST(title AS VARCHAR) AS title,
                      CAST(source AS VARCHAR) AS source,
                      CAST(snippet AS VARCHAR) AS snippet,
                      CAST(impact_magnitude AS VARCHAR) AS impact_magnitude,
                      CAST(impact_fx_pairs AS VARCHAR) AS impact_fx_pairs,
                      CAST(currencies_bullish AS VARCHAR) AS currencies_bullish,
                      CAST(currencies_bearish AS VARCHAR) AS currencies_bearish,
                      CAST(fx_directional_hint AS VARCHAR) AS fx_directional_hint
                    FROM main.fx_news_history
                    WHERE published_at >= CURRENT_TIMESTAMP - INTERVAL '{lookback} hours'
                    ORDER BY published_at DESC
                    LIMIT 200
                    """
                ).fetchdf()
                news = df.to_dict("records")
                for r in news:
                    r["origin"] = "fx_channel"
            if "fx_macro" in tables:
                df = con.execute("SELECT * FROM main.fx_macro ORDER BY as_of DESC LIMIT 1").fetchdf()
                if not df.empty:
                    macro = df.iloc[0].to_dict()
            if "fx_pairs" in tables:
                df = con.execute(
                    """
                    SELECT *
                    FROM (
                      SELECT *, ROW_NUMBER() OVER (PARTITION BY pair ORDER BY as_of DESC) AS rn
                      FROM main.fx_pairs
                    )
                    WHERE rn = 1
                    """
                ).fetchdf()
                pairs_rows = df.to_dict("records")
    except Exception as exc:
        ctx["fx_channel_error"] = str(exc)

return [{"json": {**ctx, "fx_channel_news": news, "fx_macro": macro, "fx_pairs": pairs_rows, "news_fx_channel_pulled": len(news)}}]
