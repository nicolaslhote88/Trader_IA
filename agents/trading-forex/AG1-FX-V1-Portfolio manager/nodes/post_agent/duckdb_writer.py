#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

DEFAULT_DB = os.getenv("AG1_FX_V1_CHATGPT52_DUCKDB_PATH", "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb")
DEFAULT_SCHEMA = os.getenv("AG1_FX_V1_LEDGER_SCHEMA_PATH", "/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql")


def split_sql(text: str) -> list[str]:
    out: list[str] = []
    buff: list[str] = []
    sq = False
    dq = False
    for ch in text:
        if ch == "'" and not dq:
            sq = not sq
        elif ch == '"' and not sq:
            dq = not dq
        if ch == ";" and not sq and not dq:
            stmt = "".join(buff).strip()
            if stmt:
                out.append(stmt)
            buff = []
        else:
            buff.append(ch)
    tail = "".join(buff).strip()
    if tail:
        out.append(tail)
    return out


def init_schema(db: str, schema: str) -> None:
    sql = Path(schema).read_text(encoding="utf-8")
    with duckdb.connect(db) as con:
        for stmt in split_sql(sql):
            con.execute(stmt)


def main() -> None:
    parser = argparse.ArgumentParser(description="AG1-FX-V1 DuckDB helper")
    parser.add_argument("cmd", choices=["init-schema"])
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    args = parser.parse_args()
    if args.cmd == "init-schema":
        init_schema(args.db, args.schema)
        print({"ok": True, "db": args.db, "schema": args.schema})


if __name__ == "__main__":
    main()
