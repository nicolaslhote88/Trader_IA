import duckdb


def _get_cols(con, table_name):
    rows = con.execute("PRAGMA table_info('" + table_name + "')").fetchall()
    cols = set()
    for r in rows:
        cols.add(str(r[1]).lower())
    return cols


def _ensure_mtm_table(con):
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_latest (
        symbol VARCHAR,
        symbol_raw VARCHAR,
        row_number INTEGER,
        name VARCHAR,
        asset_class VARCHAR,
        quantity DOUBLE,
        avg_price DOUBLE,
        last_price DOUBLE,
        market_value DOUBLE,
        unrealized_pnl DOUBLE,
        updated_at VARCHAR,
        next_review_date VARCHAR,
        run_id VARCHAR
    )
    """
    )


def _sync_ledger_to_mtm(con, run_id):
    _ensure_mtm_table(con)

    # Verifie que le snapshot existe, sans recuperer ts
    snap_exists = con.execute(
        "SELECT COUNT(*) FROM core.portfolio_snapshot WHERE run_id = ?",
        [run_id]
    ).fetchone()[0]

    if not snap_exists:
        return False, "NO_CORE_PORTFOLIO_SNAPSHOT_FOR_RUN"

    mtm_cols = _get_cols(con, "portfolio_positions_mtm_latest")

    # Nettoyage du miroir
    if "symbol" in mtm_cols:
        con.execute("DELETE FROM portfolio_positions_mtm_latest WHERE symbol != '__META__'")
    else:
        con.execute("DELETE FROM portfolio_positions_mtm_latest")

    # -------------------------
    # Insert CASH en SQL pur
    # -------------------------
    cash_insert_cols = []
    cash_select_parts = []

    def add_cash(col, expr):
        if col in mtm_cols:
            cash_insert_cols.append(col)
            cash_select_parts.append(expr)

    add_cash("symbol", "'CASH_EUR'")
    add_cash("symbol_raw", "'CASH_EUR'")
    add_cash("row_number", "0")
    add_cash("name", "'Cash'")
    add_cash("asset_class", "'Cash'")
    add_cash("quantity", "0")
    add_cash("avg_price", "1")
    add_cash("last_price", "1")
    add_cash("market_value", "cash_eur")
    add_cash("unrealized_pnl", "0")
    add_cash("updated_at", "CAST(ts AS VARCHAR)")
    add_cash("run_id", "run_id")

    if len(cash_insert_cols) > 0:
        con.execute(
            "INSERT INTO portfolio_positions_mtm_latest (" + ", ".join(cash_insert_cols) + ") "
            "SELECT " + ", ".join(cash_select_parts) + " "
            "FROM core.portfolio_snapshot "
            "WHERE run_id = ?",
            [run_id]
        )

    # -------------------------
    # Insert positions en SQL pur
    # -------------------------
    insert_cols = []
    select_parts = []

    def add_pos(col, expr):
        if col in mtm_cols:
            insert_cols.append(col)
            select_parts.append(expr)

    add_pos("symbol", "pos.symbol")
    add_pos("symbol_raw", "pos.symbol")
    add_pos("row_number", "CAST(ROW_NUMBER() OVER (ORDER BY pos.market_value_eur DESC) AS INTEGER)")
    add_pos("name", "COALESCE(inst.name, pos.symbol)")
    add_pos("asset_class", "COALESCE(inst.asset_class, 'EQUITY')")
    add_pos("quantity", "pos.qty")
    add_pos("avg_price", "pos.avg_cost")
    add_pos("last_price", "pos.last_price")
    add_pos("market_value", "pos.market_value_eur")
    add_pos("unrealized_pnl", "pos.unrealized_pnl_eur")
    add_pos("updated_at", "CAST(pos.ts AS VARCHAR)")
    add_pos("run_id", "pos.run_id")

    if len(insert_cols) > 0:
        con.execute(
            "INSERT INTO portfolio_positions_mtm_latest (" + ", ".join(insert_cols) + ") "
            "SELECT " + ", ".join(select_parts) + " "
            "FROM core.positions_snapshot pos "
            "LEFT JOIN core.instruments inst ON inst.symbol = pos.symbol "
            "WHERE pos.run_id = ?",
            [run_id]
        )

    return True, None


items = _items or []
out = []

for it in items:
    j = it.get("json", {})

    if j.get("status") == "FATAL_ERROR":
        out.append(
            {
                "json": {
                    "run_id": j.get("run_id"),
                    "health_ok": False,
                    "mtm_sync_ok": False,
                    "mtm_sync_error": "NODE9_FATAL:" + str(j.get("error_message")),
                    "db_path": j.get("db_path"),
                }
            }
        )
        continue

    run_id = j.get("run_id")
    db_path = j.get("db_path")

    con = duckdb.connect(db_path)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM core.runs WHERE run_id = ?",
            [run_id]
        ).fetchone()[0]

        health_ok = exists > 0

        if not health_ok:
            out.append(
                {
                    "json": {
                        "run_id": run_id,
                        "health_ok": False,
                        "mtm_sync_ok": False,
                        "mtm_sync_error": "RUN_NOT_FOUND_IN_CORE_RUNS",
                        "db_path": db_path,
                    }
                }
            )
            continue

        ok, err = _sync_ledger_to_mtm(con, run_id)

        out.append(
            {
                "json": {
                    "run_id": run_id,
                    "health_ok": True,
                    "mtm_sync_ok": bool(ok),
                    "mtm_sync_error": err,
                    "db_path": db_path,
                }
            }
        )

    except Exception as e:
        out.append(
            {
                "json": {
                    "run_id": run_id,
                    "health_ok": True,
                    "mtm_sync_ok": False,
                    "mtm_sync_error": "EXCEPTION:" + str(e),
                    "db_path": db_path,
                }
            }
        )
    finally:
        con.close()

return out
