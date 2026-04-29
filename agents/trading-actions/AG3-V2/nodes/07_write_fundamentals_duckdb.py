import duckdb, time, gc, json, hashlib
from contextlib import contextmanager

DEFAULT_DB_PATH = "/files/duckdb/ag3_v2.duckdb"


@contextmanager
def db_con(path=DEFAULT_DB_PATH, retries=5, delay=0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def to_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def to_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def to_int(v):
    try:
        if v is None or v == "":
            return None
        return int(round(float(v)))
    except Exception:
        return None


def to_json(v):
    try:
        if v is None:
            return None
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return None


items = _items or []
if not items:
    return []

first = items[0].get("json", {}) or {}
db_path = str(first.get("db_path") or DEFAULT_DB_PATH)

with db_con(db_path) as con:
    for it in items:
        j = it.get("json", {}) or {}
        run_id = str(j.get("run_id", "") or "").strip()
        symbol = str(j.get("Symbol") or j.get("symbol") or "").strip().upper()
        if not run_id or not symbol:
            continue

        triage = j.get("triageRow", {}) or {}
        consensus = j.get("consensusRow", {}) or {}
        metric_rows = j.get("metricRows", []) or []

        status = str((triage.get("Status") if triage else j.get("ok")) or "").strip()
        if status in ("True", "true", "1"):
            status = "OK"
        if not status:
            status = "OK" if j.get("ok") is True else "ERR_SOURCE"

        error_txt = to_text(triage.get("Error") if triage else j.get("error"))
        as_of = to_text((triage.get("AsOfDate") if triage else j.get("asOfDate")) or j.get("as_of_date"))
        fetched = to_text((triage.get("fetchedAt") if triage else j.get("fetchedAt")) or j.get("nowIso"))

        snapshot_seed = f"{run_id}|{symbol}|{as_of or ''}"
        snapshot_id = hashlib.sha1(snapshot_seed.encode("utf-8")).hexdigest()

        con.execute(
            """
            INSERT OR REPLACE INTO fundamentals_snapshot (
              snapshot_id, run_id, symbol, name, sector, industry, country, boursorama_ref,
              as_of_date, fetched_at, status, error, source, source_url, data_coverage_pct,
              profile_json, price_json, valuation_json, profitability_json, growth_json,
              financial_health_json, consensus_json, dividends_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                snapshot_id,
                run_id,
                symbol,
                to_text(triage.get("Name") if triage else j.get("Name")),
                to_text(triage.get("Sector") if triage else j.get("Sector")),
                to_text(triage.get("Industry") if triage else (j.get("profile") or {}).get("industry")),
                to_text(triage.get("Country") if triage else (j.get("profile") or {}).get("country")),
                to_text(triage.get("BoursoramaRef") if triage else j.get("BoursoramaRef")),
                as_of,
                fetched,
                status,
                error_txt,
                to_text(triage.get("Source") if triage else j.get("source")) or "yfinance_api",
                to_text(triage.get("SourceUrl") if triage else j.get("sourceUrl")),
                to_float(triage.get("data_coverage_pct") if triage else (j.get("meta") or {}).get("dataCoveragePctApprox")),
                to_json(j.get("profile")),
                to_json(j.get("price")),
                to_json(j.get("valuation")),
                to_json(j.get("profitability")),
                to_json(j.get("growth")),
                to_json(j.get("financialHealth")),
                to_json(j.get("consensus")),
                to_json(j.get("dividends")),
            ],
        )

        if triage and triage.get("RecordId"):
            con.execute(
                """
                INSERT OR REPLACE INTO fundamentals_triage_history (
                  record_id, run_id, updated_at, fetched_at, as_of_date, status, error,
                  symbol, name, sector, industry, country, boursorama_ref, source, source_url,
                  score, funda_conf, risk_score, quality_score, growth_score, valuation_score,
                  health_score, consensus_score, horizon, current_price, target_price, upside_pct,
                  recommendation, analyst_count, valuation, why, risks, next_steps,
                  data_coverage_pct, strategy_version, config_version, updated_row_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    to_text(triage.get("RecordId")),
                    run_id,
                    to_text(triage.get("UpdatedAt")),
                    to_text(triage.get("fetchedAt")),
                    to_text(triage.get("AsOfDate")),
                    to_text(triage.get("Status")),
                    to_text(triage.get("Error")),
                    to_text(triage.get("Symbol")) or symbol,
                    to_text(triage.get("Name")),
                    to_text(triage.get("Sector")),
                    to_text(triage.get("Industry")),
                    to_text(triage.get("Country")),
                    to_text(triage.get("BoursoramaRef")),
                    to_text(triage.get("Source")),
                    to_text(triage.get("SourceUrl")),
                    to_int(triage.get("Score")),
                    to_int(triage.get("funda_conf")),
                    to_int(triage.get("risk_score")),
                    to_int(triage.get("quality_score")),
                    to_int(triage.get("growth_score")),
                    to_int(triage.get("valuation_score")),
                    to_int(triage.get("health_score")),
                    to_int(triage.get("consensus_score")),
                    to_text(triage.get("horizon")),
                    to_float(triage.get("current_price")),
                    to_float(triage.get("target_price")),
                    to_float(triage.get("upside_pct")),
                    to_text(triage.get("recommendation")),
                    to_int(triage.get("analyst_count")),
                    to_text(triage.get("valuation")),
                    to_text(triage.get("why")),
                    to_text(triage.get("risks")),
                    to_text(triage.get("nextSteps")),
                    to_float(triage.get("data_coverage_pct")),
                    to_text(triage.get("strategy_version")),
                    to_text(triage.get("config_version")),
                ],
            )

        if consensus and consensus.get("RecordId"):
            con.execute(
                """
                INSERT OR REPLACE INTO analyst_consensus_history (
                  record_id, run_id, updated_at, as_of_date, symbol, name, sector, recommendation,
                  recommendation_mean, analyst_count, current_price, target_mean_price, target_high_price,
                  target_low_price, upside_pct, dispersion_pct, confidence_proxy, risk_proxy,
                  source, source_url, status, error, horizon, updated_row_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    to_text(consensus.get("RecordId")),
                    run_id,
                    to_text(consensus.get("UpdatedAt")),
                    to_text(consensus.get("AsOfDate")),
                    to_text(consensus.get("Symbol")) or symbol,
                    to_text(consensus.get("Name")),
                    to_text(consensus.get("Sector")),
                    to_text(consensus.get("recommendation")),
                    to_float(consensus.get("recommendationMean")),
                    to_int(consensus.get("analystCount")),
                    to_float(consensus.get("currentPrice")),
                    to_float(consensus.get("targetMeanPrice")),
                    to_float(consensus.get("targetHighPrice")),
                    to_float(consensus.get("targetLowPrice")),
                    to_float(consensus.get("upsidePct")),
                    to_float(consensus.get("dispersionPct")),
                    to_int(consensus.get("confidenceProxy")),
                    to_int(consensus.get("riskProxy")),
                    to_text(consensus.get("Source")),
                    to_text(consensus.get("SourceUrl")),
                    to_text(consensus.get("Status")),
                    to_text(consensus.get("Error")),
                    to_text(consensus.get("horizon")),
                ],
            )

        for m in metric_rows:
            rid = to_text(m.get("RecordId"))
            if not rid:
                continue
            raw_val = m.get("Value")
            con.execute(
                """
                INSERT OR REPLACE INTO fundamental_metrics_history (
                  record_id, run_id, extracted_at, as_of_date, symbol, boursorama_ref, data_type,
                  section, metric, period, value_num, value_text, unit, source_url, sig_hash,
                  title, author, excerpt, raw_text, signal, score, currency, title_or_label, notes, updated_row_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    rid,
                    run_id,
                    to_text(m.get("ExtractedAt")),
                    to_text(m.get("AsOfDate")),
                    to_text(m.get("Symbol")) or symbol,
                    to_text(m.get("BoursoramaRef")),
                    to_text(m.get("DataType")),
                    to_text(m.get("Section")),
                    to_text(m.get("Metric")),
                    to_text(m.get("Period")),
                    to_float(raw_val),
                    to_text(raw_val),
                    to_text(m.get("Unit")),
                    to_text(m.get("SourceUrl")),
                    to_text(m.get("SigHash")),
                    to_text(m.get("Title")),
                    to_text(m.get("Author")),
                    to_text(m.get("Excerpt")),
                    to_text(m.get("RawText")),
                    to_text(m.get("Signal")),
                    to_float(m.get("Score")),
                    to_text(m.get("Currency")),
                    to_text(m.get("TitleOrLabel")),
                    to_text(m.get("Notes")),
                ],
            )

return items
