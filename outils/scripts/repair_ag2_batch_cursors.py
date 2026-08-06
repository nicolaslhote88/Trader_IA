#!/usr/bin/env python3
"""Inspect or reset the two AG2 split-rotation cursors safely.

The command is read-only unless ``--apply`` is supplied.  In apply mode both
expected current values are mandatory so a concurrent or unexpected change
cannot be overwritten silently.
"""

import argparse
import json
from pathlib import Path

import duckdb


KEYS = {
    "held_core": "last_index_actions_held_core",
    "watchlist": "last_index_actions_watchlist",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path",
        default="/files/duckdb/ag2_v3.duckdb",
        help="Path to ag2_v3.duckdb",
    )
    parser.add_argument("--value", type=int, default=0, help="New cursor value")
    parser.add_argument("--held-core-value", type=int)
    parser.add_argument("--watchlist-value", type=int)
    parser.add_argument("--expected-held-core", type=int)
    parser.add_argument("--expected-watchlist", type=int)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def rows_by_key(connection):
    rows = connection.execute(
        "SELECT key, value, updated_at FROM batch_state WHERE key IN (?, ?) ORDER BY key",
        [KEYS["held_core"], KEYS["watchlist"]],
    ).fetchall()
    return {
        str(key): {
            "value": int(value),
            "updated_at": updated_at.isoformat() if updated_at else None,
        }
        for key, value, updated_at in rows
    }


def main():
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.is_file():
        raise SystemExit(f"Database not found: {db_path}")
    target = {
        KEYS["held_core"]: args.held_core_value if args.held_core_value is not None else args.value,
        KEYS["watchlist"]: args.watchlist_value if args.watchlist_value is not None else args.value,
    }
    if any(value < 0 for value in target.values()):
        raise SystemExit("cursor values must be >= 0")
    if args.apply and (
        args.expected_held_core is None or args.expected_watchlist is None
    ):
        raise SystemExit(
            "--apply requires --expected-held-core and --expected-watchlist"
        )

    con = duckdb.connect(str(db_path), read_only=not args.apply)
    try:
        before = rows_by_key(con)
        expected = {
            KEYS["held_core"]: args.expected_held_core,
            KEYS["watchlist"]: args.expected_watchlist,
        }
        missing = [key for key in KEYS.values() if key not in before]
        if missing:
            raise RuntimeError(f"Missing batch_state keys: {missing}")

        if not args.apply:
            print(json.dumps({"mode": "dry-run", "before": before}, indent=2))
            return

        mismatches = {
            key: {"expected": expected[key], "actual": before[key]["value"]}
            for key in KEYS.values()
            if before[key]["value"] != expected[key]
        }
        if mismatches:
            raise RuntimeError(f"Cursor precondition failed: {mismatches}")

        con.execute("BEGIN TRANSACTION")
        for key in KEYS.values():
            con.execute(
                "UPDATE batch_state SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?",
                [target[key], key],
            )
        after = rows_by_key(con)
        if any(after[key]["value"] != target[key] for key in KEYS.values()):
            raise RuntimeError(f"Cursor verification failed: {after}")
        con.execute("COMMIT")
        print(
            json.dumps(
                {"mode": "applied", "before": before, "after": after}, indent=2
            )
        )
    except Exception:
        if args.apply:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
