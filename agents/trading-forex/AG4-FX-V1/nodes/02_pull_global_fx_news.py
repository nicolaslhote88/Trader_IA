import os
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag4_v3_path") or "/files/duckdb/ag4_v3.duckdb"
forex_path = ctx.get("ag4_forex_path") or "/files/duckdb/ag4_forex_v1.duckdb"
lookback = max(1, min(168, int(ctx.get("lookback_hours") or 96)))
items = []


def split_csv(value):
    return [x.strip() for x in str(value or "").replace(";", ",").split(",") if x.strip()]


def has_fx_signal(row):
    pairs = split_csv(row.get("impact_fx_pairs"))
    bull = split_csv(row.get("currencies_bullish"))
    bear = split_csv(row.get("currencies_bearish"))
    return bool(pairs or bull or bear)


def append_rows(rows, origin="global_base"):
    for row in rows:
        row = dict(row)
        row["origin"] = origin
        if has_fx_signal(row):
            items.append(row)


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
            title = col("title", "Title")
            source = col("source", "Source")
            snippet = col("snippet", "strategic_summary", "Strategy", "summary")
            dedupe = col("dedupe_key", "dedupeKey", "event_key", "id")
            impact_class = col("impact_asset_class")
            pairs = col("impact_fx_pairs")
            mag = col("impact_magnitude")
            bull = col("currencies_bullish")
            bear = col("currencies_bearish")
            hint = col("fx_directional_hint")
            regime = col("regime", "Regime", "market_regime")
            confidence = col("confidence")
            impact_score = col("impact_score", "ImpactScore")
            urgency = col("urgency")

            def expr(name, default=""):
                if name:
                    return f"CAST({name} AS VARCHAR)"
                return f"'{default}'"

            fx_filter_parts = []
            if impact_class:
                fx_filter_parts.append(f"lower(COALESCE(CAST({impact_class} AS VARCHAR), '')) LIKE '%fx%'")
                fx_filter_parts.append(f"lower(COALESCE(CAST({impact_class} AS VARCHAR), '')) LIKE '%mixed%'")
            if pairs:
                fx_filter_parts.append(f"COALESCE(CAST({pairs} AS VARCHAR), '') <> ''")
            if bull:
                fx_filter_parts.append(f"COALESCE(CAST({bull} AS VARCHAR), '') <> ''")
            if bear:
                fx_filter_parts.append(f"COALESCE(CAST({bear} AS VARCHAR), '') <> ''")
            fx_filter = " OR ".join(fx_filter_parts) or "TRUE"

            query = f"""
                SELECT
                  {expr(dedupe)} AS dedupe_key,
                  CAST({published} AS VARCHAR) AS published_at,
                  {expr(title, "unknown")} AS title,
                  {expr(source, "global_base")} AS source,
                  {expr(snippet)} AS snippet,
                  {expr(mag, "Low")} AS impact_magnitude,
                  {expr(pairs)} AS impact_fx_pairs,
                  {expr(bull)} AS currencies_bullish,
                  {expr(bear)} AS currencies_bearish,
                  {expr(hint)} AS fx_directional_hint,
                  {expr(regime, "Neutral")} AS regime,
                  {expr(confidence)} AS confidence,
                  {expr(impact_score)} AS impact_score,
                  {expr(urgency)} AS urgency
                FROM main.news_history
                WHERE CAST({published} AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '{lookback} hours'
                  AND ({fx_filter})
                ORDER BY CAST({published} AS TIMESTAMP) DESC
                LIMIT 200
            """
            append_rows(con.execute(query).fetchdf().to_dict("records"))
    except Exception as exc:
        ctx["global_error"] = str(exc)

if not items and os.path.exists(forex_path):
    try:
        with duckdb.connect(forex_path, read_only=True) as con:
            tables = {r[0].lower() for r in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()}
            if "fx_news_history" in tables:
                rows = con.execute(
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
                      CAST(fx_directional_hint AS VARCHAR) AS fx_directional_hint,
                      CAST(regime AS VARCHAR) AS regime,
                      CAST(confidence AS VARCHAR) AS confidence,
                      CAST(impact_score AS VARCHAR) AS impact_score,
                      CAST(urgency AS VARCHAR) AS urgency
                    FROM main.fx_news_history
                    WHERE origin = 'global_base'
                      AND published_at >= CURRENT_TIMESTAMP - INTERVAL '{lookback} hours'
                    ORDER BY published_at DESC
                    LIMIT 200
                    """
                ).fetchdf().to_dict("records")
                append_rows(rows)
    except Exception as exc:
        ctx["global_fallback_error"] = str(exc)

return [{"json": {**ctx, "global_news": items, "news_global_pulled": len(items)}}]
