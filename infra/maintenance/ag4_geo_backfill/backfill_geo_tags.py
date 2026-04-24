#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb


TARGET_TAGGER_VERSION = "geo_v1"
BATCH_SIZE = 200

ALLOWED_REGIONS = {"Global", "US", "EU", "France", "UK", "APAC", "Emerging", "Other"}
ALLOWED_CLASSES = {"Equity", "FX", "Commodity", "Bond", "Crypto", "Mixed", "None"}
ALLOWED_MAG = {"Low", "Medium", "High"}
ALLOWED_PAIRS = {
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD",
    "AUDJPY", "AUDNZD", "AUDCAD",
    "NZDJPY", "NZDCAD",
    "CADJPY", "CHFJPY", "CADCHF",
    "CHFCAD", "JPYNZD",
}


SYSTEM_PROMPT = """Tu es le tagger geo/asset-class de AG4.
Tu ne modifies pas l'analyse existante. Tu ajoutes seulement:
- impact_region: CSV parmi {Global, US, EU, France, UK, APAC, Emerging, Other}
- impact_asset_class: CSV parmi {Equity, FX, Commodity, Bond, Crypto, Mixed, None}
- impact_magnitude: une valeur parmi {Low, Medium, High}
- impact_fx_pairs: CSV de paires FX au format XXXYYY, vide sauf si impact_asset_class contient FX ou Mixed.

Regles:
- Fed = Global sauf politique US pure.
- Mixed si 3+ classes majeures sont impactees.
- None si pas d'impact marche notable.
- High implique un evenement potentiellement trend-changer.
- Si impact_asset_class contient FX ou Mixed, impact_fx_pairs doit etre non vide.
- Paires autorisees: EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD,
  EURGBP, EURJPY, EURCHF, EURAUD, EURCAD, EURNZD, GBPJPY, GBPCHF, GBPAUD, GBPCAD,
  AUDJPY, AUDNZD, AUDCAD, NZDJPY, NZDCAD, CADJPY, CHFJPY, CADCHF, CHFCAD, JPYNZD.
- Reponds uniquement en JSON valide, sans markdown.
"""


def parse_args() -> argparse.Namespace:
    default_since = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
    parser = argparse.ArgumentParser(description="Backfill AG4 geo tags into ag4_v3 and ag4_forex_v1.")
    parser.add_argument("--ag4-db", default=os.getenv("AG4_DB_PATH", "/files/duckdb/ag4_v3.duckdb"))
    parser.add_argument("--forex-db", default=os.getenv("AG4_FOREX_DB_PATH", "/files/duckdb/ag4_forex_v1.duckdb"))
    parser.add_argument("--model", default=os.getenv("AG4_GEO_BACKFILL_MODEL", "gpt-5-mini"))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("AG4_GEO_BACKFILL_BATCH", BATCH_SIZE)))
    parser.add_argument("--since", default=os.getenv("AG4_GEO_BACKFILL_SINCE", default_since))
    parser.add_argument("--limit", type=int, default=int(os.getenv("AG4_GEO_BACKFILL_LIMIT", "0")))
    parser.add_argument("--yes", action="store_true", help="Skip interactive cost confirmation.")
    return parser.parse_args()


def sanitize_csv(raw: Any, allowed: set[str], default: str = "Other") -> tuple[str, list[str]]:
    if raw is None:
        return default, []
    if isinstance(raw, list):
        parts = [str(x).strip() for x in raw if str(x).strip()]
    else:
        parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    if not parts:
        return default, []
    kept: list[str] = []
    violations: list[str] = []
    for part in parts:
        if part in allowed and part not in kept:
            kept.append(part)
        elif part not in allowed:
            violations.append(part)
    return (", ".join(kept) if kept else default), violations


def sanitize_payload(payload: dict[str, Any], currencies: str) -> tuple[dict[str, str], list[str]]:
    violations: list[str] = []
    impact_region, bad = sanitize_csv(payload.get("impact_region"), ALLOWED_REGIONS, "Other")
    violations += [f"impact_region:{x}" for x in bad]
    impact_asset_class, bad = sanitize_csv(payload.get("impact_asset_class"), ALLOWED_CLASSES, "None")
    violations += [f"impact_asset_class:{x}" for x in bad]

    impact_magnitude = str(payload.get("impact_magnitude") or "").strip()
    if impact_magnitude not in ALLOWED_MAG:
        if impact_magnitude:
            violations.append(f"impact_magnitude:{impact_magnitude}")
        impact_magnitude = "Low"

    impact_fx_pairs, bad = sanitize_csv(payload.get("impact_fx_pairs"), ALLOWED_PAIRS, "")
    violations += [f"impact_fx_pairs:{x}" for x in bad]

    classes = {p.strip() for p in impact_asset_class.split(",") if p.strip()}
    needs_pairs = "FX" in classes or "Mixed" in classes
    if not needs_pairs:
        if impact_fx_pairs:
            violations.append("impact_fx_pairs:present_without_fx_or_mixed")
        impact_fx_pairs = ""
    elif not impact_fx_pairs:
        impact_fx_pairs = derive_pairs(currencies)
        violations.append("impact_fx_pairs:missing_derived")

    return {
        "impact_region": impact_region,
        "impact_asset_class": impact_asset_class,
        "impact_magnitude": impact_magnitude,
        "impact_fx_pairs": impact_fx_pairs,
    }, violations


def derive_pairs(currencies: str) -> str:
    ccy = {p.strip().upper() for p in currencies.split(",") if p.strip()}
    mapping = {
        "USD": ["EURUSD", "USDJPY", "USDCHF"],
        "EUR": ["EURUSD", "EURGBP", "EURJPY"],
        "GBP": ["GBPUSD", "EURGBP", "GBPJPY"],
        "JPY": ["USDJPY", "EURJPY", "GBPJPY"],
        "CHF": ["USDCHF", "EURCHF", "CHFJPY"],
        "AUD": ["AUDUSD", "AUDJPY", "EURAUD"],
        "CAD": ["USDCAD", "CADJPY", "EURCAD"],
        "NZD": ["NZDUSD", "NZDJPY", "EURNZD"],
    }
    out: list[str] = []
    for code in ccy:
        for pair in mapping.get(code, []):
            if pair not in out:
                out.append(pair)
    return ", ".join(out[:5] or ["EURUSD"])


def init_forex_schema(con: duckdb.DuckDBPyConnection) -> None:
    migration = Path(__file__).resolve().parents[2] / "migrations" / "ag4_forex_v1" / "20260425_init.sql"
    if migration.exists():
        execute_script(con, migration.read_text(encoding="utf-8"))
        return
    raise FileNotFoundError(f"Missing migration: {migration}")


def ensure_ag4_schema(con: duckdb.DuckDBPyConnection) -> None:
    migration = Path(__file__).resolve().parents[2] / "migrations" / "ag4_v3" / "20260425_add_geo_tagging.sql"
    execute_script(con, migration.read_text(encoding="utf-8"))


def execute_script(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    for stmt in statements:
        con.execute(stmt)


def row_prompt(row: dict[str, Any]) -> str:
    payload = {
        "title": row.get("title") or "",
        "source": row.get("source") or "",
        "published_at": str(row.get("published_at") or ""),
        "snippet": row.get("snippet") or "",
        "type": row.get("type") or "",
        "impact_score": row.get("impact_score"),
        "confidence": row.get("confidence"),
        "urgency": row.get("urgency"),
        "theme": row.get("theme") or "",
        "regime": row.get("regime") or "",
        "sectors_bullish": row.get("sectors_bullish") or "",
        "sectors_bearish": row.get("sectors_bearish") or "",
        "currencies_bullish": row.get("currencies_bullish") or "",
        "currencies_bearish": row.get("currencies_bearish") or "",
        "notes": row.get("notes") or "",
    }
    return json.dumps(payload, ensure_ascii=False, default=str)


def call_openai(client: Any, model: str, prompt: str) -> tuple[dict[str, Any], dict[str, int]]:
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "ag4_geo_tags",
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "impact_region": {"type": "string"},
                        "impact_asset_class": {"type": "string"},
                        "impact_magnitude": {"type": "string", "enum": ["Low", "Medium", "High"]},
                        "impact_fx_pairs": {"type": "string"},
                    },
                    "required": ["impact_region", "impact_asset_class", "impact_magnitude", "impact_fx_pairs"],
                },
            }
        },
    )
    text = getattr(response, "output_text", "") or "{}"
    usage = getattr(response, "usage", None)
    usage_dict = {
        "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
    }
    return json.loads(text), usage_dict


def estimate_cost_eur(rows: int) -> float:
    input_tokens = rows * int(os.getenv("AG4_GEO_EST_INPUT_TOKENS", "400"))
    output_tokens = rows * int(os.getenv("AG4_GEO_EST_OUTPUT_TOKENS", "80"))
    in_rate = float(os.getenv("AG4_GEO_INPUT_EUR_PER_1M", "0.25"))
    out_rate = float(os.getenv("AG4_GEO_OUTPUT_EUR_PER_1M", "2.00"))
    return (input_tokens / 1_000_000 * in_rate) + (output_tokens / 1_000_000 * out_rate)


def log_jsonl(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def load_rows(con: duckdb.DuckDBPyConnection, since: str, batch_size: int) -> list[dict[str, Any]]:
    cols = [
        "dedupe_key", "event_key", "run_id", "canonical_url", "published_at", "title", "source",
        "source_tier", "snippet", "impact_score", "confidence", "urgency", "first_seen_at",
        "last_seen_at", "analyzed_at", "currencies_bullish", "currencies_bearish", "regime",
        "theme", "type", "notes", "sectors_bullish", "sectors_bearish",
    ]
    rows = con.execute(
        f"""
        SELECT {", ".join(cols)}
        FROM main.news_history
        WHERE (tagger_version IS NULL OR tagger_version < ?)
          AND published_at >= ?
        ORDER BY published_at ASC NULLS LAST, dedupe_key ASC
        LIMIT ?
        """,
        [TARGET_TAGGER_VERSION, since, batch_size],
    ).fetchall()
    return [dict(zip(cols, row)) for row in rows]


def update_global(con: duckdb.DuckDBPyConnection, dedupe_key: str, tags: dict[str, str]) -> None:
    con.execute(
        """
        UPDATE main.news_history
        SET impact_region = ?,
            impact_asset_class = ?,
            impact_magnitude = ?,
            impact_fx_pairs = ?,
            tagger_version = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE dedupe_key = ?
        """,
        [
            tags["impact_region"],
            tags["impact_asset_class"],
            tags["impact_magnitude"],
            tags["impact_fx_pairs"],
            TARGET_TAGGER_VERSION,
            dedupe_key,
        ],
    )


def write_forex(con: duckdb.DuckDBPyConnection, row: dict[str, Any], tags: dict[str, str]) -> bool:
    classes = {p.strip() for p in tags["impact_asset_class"].split(",") if p.strip()}
    if "FX" not in classes and "Mixed" not in classes:
        return False

    def urgency_score(v: Any) -> float:
        s = str(v or "").strip().lower()
        return {"immediate": 1.0, "today": 0.75, "this_week": 0.45, "low": 0.2}.get(s, 0.2)

    con.execute(
        """
        INSERT OR REPLACE INTO main.fx_news_history (
            dedupe_key, event_key, run_id, origin, canonical_url, published_at,
            title, source, source_tier, snippet,
            impact_region, impact_magnitude, impact_fx_pairs,
            currencies_bullish, currencies_bearish, regime, theme,
            urgency, confidence, impact_score,
            fx_narrative, fx_directional_hint, tagger_version,
            first_seen_at, last_seen_at, analyzed_at, updated_at
        ) VALUES (?, ?, ?, 'global_base', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            row.get("dedupe_key"),
            row.get("event_key") or "",
            row.get("run_id") or "",
            row.get("canonical_url") or "",
            row.get("published_at"),
            row.get("title") or "",
            row.get("source") or "",
            str(row.get("source_tier") or ""),
            row.get("snippet") or "",
            tags["impact_region"],
            tags["impact_magnitude"],
            tags["impact_fx_pairs"],
            row.get("currencies_bullish") or "",
            row.get("currencies_bearish") or "",
            row.get("regime") or "",
            row.get("theme") or "",
            urgency_score(row.get("urgency")),
            float(row.get("confidence") or 0.0),
            int(row.get("impact_score") or 0),
            row.get("notes") or "",
            "",
            TARGET_TAGGER_VERSION,
            row.get("first_seen_at"),
            row.get("last_seen_at"),
            row.get("analyzed_at"),
        ],
    )
    return True


def main() -> int:
    args = parse_args()
    workdir = Path(__file__).resolve().parent
    state_path = workdir / "state.json"
    log_path = workdir / f"log_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"

    with duckdb.connect(args.ag4_db) as ag4:
        ensure_ag4_schema(ag4)
        total = ag4.execute(
            """
            SELECT COUNT(*)
            FROM main.news_history
            WHERE (tagger_version IS NULL OR tagger_version < ?)
              AND published_at >= ?
            """,
            [TARGET_TAGGER_VERSION, args.since],
        ).fetchone()[0]

    if args.limit and args.limit < total:
        total = args.limit

    estimate = estimate_cost_eur(total)
    print(f"Backfill cible: {total} news depuis {args.since}")
    print(f"Modele: {args.model}")
    print(f"Estimation indicative: {estimate:.2f} EUR (configurable via AG4_GEO_*_EUR_PER_1M)")
    if not args.yes:
        answer = input("Confirmer les appels LLM ? [y/N] ").strip().lower()
        if answer != "y":
            print("Abandon avant appel LLM.")
            return 1

    try:
        from openai import OpenAI
    except ImportError:
        print("Package openai manquant. Installer openai ou lancer depuis l'environnement n8n/maintenance.", file=sys.stderr)
        return 2

    client = OpenAI()
    processed = 0
    fx_written = 0
    errors = 0
    usage_total = {"input_tokens": 0, "output_tokens": 0}

    with duckdb.connect(args.ag4_db) as ag4, duckdb.connect(args.forex_db) as forex:
        ensure_ag4_schema(ag4)
        init_forex_schema(forex)

        while True:
            if args.limit and processed >= args.limit:
                break
            rows = load_rows(ag4, args.since, min(args.batch_size, args.limit - processed if args.limit else args.batch_size))
            if not rows:
                break

            for row in rows:
                dedupe_key = str(row["dedupe_key"])
                try:
                    raw_tags, usage = call_openai(client, args.model, row_prompt(row))
                    currencies = ", ".join([row.get("currencies_bullish") or "", row.get("currencies_bearish") or ""])
                    tags, violations = sanitize_payload(raw_tags, currencies)
                    update_global(ag4, dedupe_key, tags)
                    if write_forex(forex, row, tags):
                        fx_written += 1
                    processed += 1
                    usage_total["input_tokens"] += usage["input_tokens"]
                    usage_total["output_tokens"] += usage["output_tokens"]
                    log_jsonl(log_path, {
                        "ts": datetime.now(timezone.utc),
                        "dedupe_key": dedupe_key,
                        "status": "ok",
                        "tags": tags,
                        "violations": violations,
                        "usage": usage,
                    })
                except Exception as exc:
                    errors += 1
                    log_jsonl(log_path, {
                        "ts": datetime.now(timezone.utc),
                        "dedupe_key": dedupe_key,
                        "status": "error",
                        "error": repr(exc),
                    })

                state_path.write_text(
                    json.dumps(
                        {
                            "last_run_at": datetime.now(timezone.utc).isoformat(),
                            "last_dedupe_key": dedupe_key,
                            "processed": processed,
                            "fx_written": fx_written,
                            "errors": errors,
                            "usage": usage_total,
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )

    print(
        "resume "
        f"processed={processed} fx_written={fx_written} errors={errors} "
        f"input_tokens={usage_total['input_tokens']} output_tokens={usage_total['output_tokens']}"
    )
    return 0 if errors == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
