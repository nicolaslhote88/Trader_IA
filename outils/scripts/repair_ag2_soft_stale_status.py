#!/usr/bin/env python3
"""Repair AG2 rows misclassified by the former UTC 3-hour H1 rule.

Dry-run is the default.  ``--apply`` stores every previous value in a dedicated
backup table before updating only the latest row of each symbol.  ``--rollback``
restores those values.  The hard H1/D1 age limit remains 96 hours.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import duckdb


DEFAULT_DB = "/files/duckdb/ag2_v3.duckdb"
BACKUP_TABLE = "maintenance_ag2_soft_stale_status_backup_20260806"

TARGET_SQL = """
WITH ranked AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(TRIM(t.symbol))
      ORDER BY t.workflow_date DESC NULLS LAST, t.updated_at DESC NULLS LAST
    ) AS rn
  FROM technical_signals t
), latest AS (
  SELECT * EXCLUDE(rn)
  FROM ranked
  WHERE rn = 1
)
SELECT
  id,
  symbol,
  workflow_date,
  h1_date,
  d1_date,
  data_age_h1_hours,
  data_age_d1_hours,
  h1_status,
  h1_warnings,
  data_quality_flags,
  filter_reason,
  updated_at
FROM latest
WHERE COALESCE(h1_closed_only, FALSE)
  AND COALESCE(d1_closed_only, FALSE)
  AND h1_status = 'STALE'
  AND d1_status = 'OK'
  AND h1_date IS NOT NULL
  AND d1_date IS NOT NULL
  AND GREATEST(
        COALESCE(data_age_h1_hours, 1000000),
        COALESCE(date_diff('minute', h1_date, CURRENT_TIMESTAMP) / 60.0, 1000000)
      ) <= 96
  AND GREATEST(
        COALESCE(data_age_d1_hours, 1000000),
        COALESCE(date_diff('minute', d1_date, CURRENT_TIMESTAMP) / 60.0, 1000000)
      ) <= 96
ORDER BY symbol
"""


def _json_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    try:
        parsed = json.loads(str(value or "[]"))
    except Exception:
        return []
    return [str(v) for v in parsed] if isinstance(parsed, list) else []


def _repaired_warnings(value: object) -> str:
    warnings = [
        item
        for item in _json_list(value)
        if not (item.startswith("H1 data is ") and item.endswith("h old - STALE"))
    ]
    return json.dumps(warnings, ensure_ascii=False)


def _repaired_flags(value: object) -> str:
    flags = [item for item in _json_list(value) if item != "STALE_H1"]
    if "H1_OUTSIDE_AI_FRESHNESS_WINDOW" not in flags:
        flags.append("H1_OUTSIDE_AI_FRESHNESS_WINDOW")
    return json.dumps(sorted(set(flags)), ensure_ascii=False)


def _rows_as_dicts(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, object]]:
    cur = con.execute(sql)
    columns = [str(desc[0]) for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _ensure_backup_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {BACKUP_TABLE} (
          id VARCHAR PRIMARY KEY,
          symbol VARCHAR,
          workflow_date TIMESTAMP,
          h1_status VARCHAR,
          h1_warnings VARCHAR,
          data_quality_flags VARCHAR,
          filter_reason VARCHAR,
          updated_at TIMESTAMP,
          repaired_at TIMESTAMP
        )
        """
    )


def apply_repair(con: duckdb.DuckDBPyConnection, targets: list[dict[str, object]]) -> int:
    _ensure_backup_table(con)
    changed = 0
    con.execute("BEGIN TRANSACTION")
    try:
        for row in targets:
            con.execute(
                f"""
                INSERT INTO {BACKUP_TABLE} (
                  id, symbol, workflow_date, h1_status, h1_warnings,
                  data_quality_flags, filter_reason, updated_at, repaired_at
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                WHERE NOT EXISTS (SELECT 1 FROM {BACKUP_TABLE} WHERE id = ?)
                """,
                [
                    row["id"],
                    row["symbol"],
                    row["workflow_date"],
                    row["h1_status"],
                    row["h1_warnings"],
                    row["data_quality_flags"],
                    row["filter_reason"],
                    row["updated_at"],
                    row["id"],
                ],
            )
            new_reason = str(row.get("filter_reason") or "")
            if new_reason in {"NO_H1_DATA", "STALE_H1_OR_D1_DATA"}:
                new_reason = "H1_OR_D1_OUTSIDE_AI_FRESHNESS_WINDOW"
            con.execute(
                """
                UPDATE technical_signals
                SET h1_status = 'OK',
                    h1_warnings = ?,
                    data_quality_flags = ?,
                    filter_reason = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND h1_status = 'STALE'
                """,
                [
                    _repaired_warnings(row.get("h1_warnings")),
                    _repaired_flags(row.get("data_quality_flags")),
                    new_reason,
                    row["id"],
                ],
            )
            changed += 1
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return changed


def rollback(con: duckdb.DuckDBPyConnection) -> int:
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [BACKUP_TABLE],
    ).fetchone()[0]
    if not exists:
        return 0
    count = int(con.execute(f"SELECT COUNT(*) FROM {BACKUP_TABLE}").fetchone()[0])
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"""
            UPDATE technical_signals AS t
            SET h1_status = b.h1_status,
                h1_warnings = b.h1_warnings,
                data_quality_flags = b.data_quality_flags,
                filter_reason = b.filter_reason,
                updated_at = b.updated_at
            FROM {BACKUP_TABLE} AS b
            WHERE t.id = b.id
            """
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_DB)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--rollback", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect(args.db, read_only=not (args.apply or args.rollback))
    try:
        if args.rollback:
            restored = rollback(con)
            print(json.dumps({"mode": "rollback", "restored": restored}, ensure_ascii=False))
            return 0

        targets = _rows_as_dicts(con, TARGET_SQL)
        summary = {
            "mode": "apply" if args.apply else "dry-run",
            "db": args.db,
            "hard_age_limit_hours": 96,
            "targets": len(targets),
            "sample_symbols": [str(row["symbol"]) for row in targets[:20]],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        if args.apply:
            summary["changed"] = apply_repair(con, targets)
            summary["remaining_targets"] = len(_rows_as_dicts(con, TARGET_SQL))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
