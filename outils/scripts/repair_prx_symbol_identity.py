#!/usr/bin/env python3
"""Repair the erroneous PRX.PA identity after the PRX.AS IBKR fill.

The script is intentionally scoped to PRX and requires live fill proof that
conid 382625193 was bought from listing AEB for an internal PRX.AS order.
Run without --apply for a read-only report. A full DuckDB file backup must be
made before --apply; per-table backup rows are also retained in the database.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import duckdb


WRONG = "PRX.PA"
CANONICAL = "PRX.AS"
EXPECTED_CONID = 382625193
BACKUP_PREFIX = "maintenance_prx_symbol_identity_20260813"


SYMBOL_TABLES = [
    ("core", "market_prices", ["ts", "symbol", "source"]),
    ("core", "positions_snapshot", ["run_id", "symbol"]),
    ("main", "portfolio_positions_ibkr_latest", ["symbol"]),
    ("main", "portfolio_positions_mtm_history", ["id"]),
    ("main", "portfolio_positions_mtm_latest", ["symbol"]),
]


JSON_FIELDS = [
    ("core", "consensus_votes", "vote_id", ["payload_json"]),
    ("core", "model_proposals", "proposal_id", ["actions_json", "decision_json", "warnings_json"]),
    ("core", "runs", "run_id", ["agent_output_json"]),
]


def table_name(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def backup_name(schema: str, table: str) -> str:
    return f'"main"."{BACKUP_PREFIX}__{schema}__{table}"'


def recursive_exact_symbol_replace(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: recursive_exact_symbol_replace(v) for k, v in value.items()}
    if isinstance(value, list):
        return [recursive_exact_symbol_replace(v) for v in value]
    if isinstance(value, str) and value.strip().upper() == WRONG:
        return CANONICAL
    return value


def counts(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    out = {}
    for schema, table, _ in SYMBOL_TABLES:
        out[f"{schema}.{table}"] = con.execute(
            f"SELECT COUNT(*) FROM {table_name(schema, table)} WHERE UPPER(symbol) = ?",
            [WRONG],
        ).fetchone()[0]
    out["core.instruments"] = con.execute(
        "SELECT COUNT(*) FROM core.instruments WHERE UPPER(symbol) = ?", [WRONG]
    ).fetchone()[0]
    out["core.consensus_votes"] = con.execute(
        "SELECT COUNT(*) FROM core.consensus_votes WHERE UPPER(symbol) = ?", [WRONG]
    ).fetchone()[0]
    return out


def verify_fill_proof(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = con.execute(
        """
        SELECT o.order_id, o.symbol, o.broker_order_id, CAST(f.raw_fill_json AS VARCHAR)
        FROM core.orders o
        JOIN core.fills f ON f.order_id = o.order_id
        WHERE UPPER(o.symbol) = 'PRX.AS'
        ORDER BY f.ts_fill DESC
        """
    ).fetchall()
    for order_id, symbol, broker_order_id, raw in rows:
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        fill = payload.get("ibkrFill", payload) if isinstance(payload, dict) else {}
        conid = int(fill.get("conid") or fill.get("conidEx") or 0)
        listing = str(fill.get("listing_exchange") or "").strip().upper()
        if conid == EXPECTED_CONID and listing == "AEB":
            return {
                "order_id": order_id,
                "symbol": symbol,
                "broker_order_id": broker_order_id,
                "conid": conid,
                "listing_exchange": listing,
            }
    raise RuntimeError("PRX_REPAIR_BLOCKED_NO_MATCHING_PRX_AS_AEB_FILL_PROOF")


def create_backups(con: duckdb.DuckDBPyConnection) -> list[str]:
    created = []
    specs = [(s, t) for s, t, _ in SYMBOL_TABLES]
    specs += [("core", "instruments")]
    specs += [(s, t) for s, t, _, _ in JSON_FIELDS]
    for schema, table in specs:
        backup = backup_name(schema, table)
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
            [f"{BACKUP_PREFIX}__{schema}__{table}"],
        ).fetchone()[0]
        if exists:
            raise RuntimeError(f"PRX_REPAIR_BACKUP_ALREADY_EXISTS:{backup}")
        source = table_name(schema, table)
        if table == "instruments" or any((schema, table) == (s, t) for s, t, _ in SYMBOL_TABLES):
            predicate = "UPPER(symbol) IN ('PRX.PA', 'PRX.AS')"
        else:
            fields = next(fields for s, t, _, fields in JSON_FIELDS if (s, t) == (schema, table))
            clauses = [f"CONTAINS(UPPER(CAST(\"{field}\" AS VARCHAR)), 'PRX.PA')" for field in fields]
            if table == "consensus_votes":
                clauses.extend(["UPPER(symbol)='PRX.PA'", "CONTAINS(UPPER(vote_id), 'PRX.PA')"])
            predicate = " OR ".join(clauses)
        con.execute(f"CREATE TABLE {backup} AS SELECT * FROM {source} WHERE {predicate}")
        created.append(backup)
    return created


def update_json_rows(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    key: str,
    fields: list[str],
) -> int:
    source = table_name(schema, table)
    clauses = [f"CONTAINS(UPPER(CAST(\"{field}\" AS VARCHAR)), 'PRX.PA')" for field in fields]
    if table == "consensus_votes":
        clauses.extend(["UPPER(symbol)='PRX.PA'", "CONTAINS(UPPER(vote_id), 'PRX.PA')"])
    cur = con.execute(
        f'SELECT "{key}", {", ".join(f"CAST(\"{field}\" AS VARCHAR)" for field in fields)} '
        f"FROM {source} WHERE {' OR '.join(clauses)}"
    )
    rows = cur.fetchall()
    for row in rows:
        old_key = row[0]
        values = []
        for raw in row[1:]:
            if raw in (None, ""):
                values.append(raw)
                continue
            try:
                parsed = json.loads(raw)
                values.append(json.dumps(recursive_exact_symbol_replace(parsed), ensure_ascii=False))
            except Exception:
                values.append(raw)
        new_key = str(old_key).replace(WRONG, CANONICAL) if table == "consensus_votes" else old_key
        assignments = [f'"{field}"=?' for field in fields]
        if table == "consensus_votes":
            assignments.extend(['symbol=?', 'vote_id=?'])
            values.extend([CANONICAL, new_key])
        con.execute(
            f'UPDATE {source} SET {", ".join(assignments)} WHERE "{key}"=?',
            values + [old_key],
        )
    return len(rows)


def apply_repair(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    proof = verify_fill_proof(con)
    before = counts(con)
    if not any(before.values()):
        return {"status": "NOOP_ALREADY_REPAIRED", "proof": proof, "before": before, "after": before}

    con.execute("BEGIN TRANSACTION")
    try:
        backups = create_backups(con)

        for schema, table, _ in SYMBOL_TABLES:
            source = table_name(schema, table)
            if table == "portfolio_positions_mtm_history":
                con.execute(
                    f"UPDATE {source} SET id=REPLACE(id, ?, ?), symbol=?, symbol_raw=? WHERE UPPER(symbol)=?",
                    [WRONG, CANONICAL, CANONICAL, CANONICAL, WRONG],
                )
            elif table == "portfolio_positions_mtm_latest":
                con.execute(
                    f"UPDATE {source} SET symbol=?, symbol_raw=? WHERE UPPER(symbol)=?",
                    [CANONICAL, CANONICAL, WRONG],
                )
            else:
                con.execute(f"UPDATE {source} SET symbol=? WHERE UPPER(symbol)=?", [CANONICAL, WRONG])

        json_updates = {}
        for schema, table, key, fields in JSON_FIELDS:
            json_updates[f"{schema}.{table}"] = update_json_rows(con, schema, table, key, fields)

        con.execute(
            "UPDATE core.instruments SET exchange='AEB', updated_at=CURRENT_TIMESTAMP WHERE UPPER(symbol)=?",
            [CANONICAL],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    # DuckDB does not always allow deleting a referenced parent key in the same
    # transaction that moved the child rows. Remove the now-unreferenced alias
    # in a second transaction; the full file backup remains the rollback path.
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM core.instruments WHERE UPPER(symbol)=?", [WRONG])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    after = counts(con)
    if any(after.values()):
        raise RuntimeError(f"PRX_REPAIR_POSTCONDITION_FAILED:{after}")

    return {
        "status": "REPAIRED",
        "proof": proof,
        "before": before,
        "after": after,
        "backup_tables": backups,
        "json_rows_updated": json_updates,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect(args.db_path, read_only=not args.apply)
    try:
        proof = verify_fill_proof(con)
        if not args.apply:
            report = {"status": "DRY_RUN", "proof": proof, "counts": counts(con)}
        else:
            report = apply_repair(con)
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
