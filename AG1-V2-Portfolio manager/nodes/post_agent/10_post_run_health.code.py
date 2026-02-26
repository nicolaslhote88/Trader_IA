import json
from datetime import datetime, timezone
import duckdb

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _sync_ledger_to_mtm(con, run_id):
    """
    Resynchronise portfolio_positions_mtm_latest depuis les tables du ledger AG1-V2
    (core.positions_snapshot, core.portfolio_snapshot, core.instruments).

    Garantit que le prochain run lit un cashEUR et des quantités REELS,
    evitant ainsi que l'agent investisse plus que le cash disponible.
    Les metadonnees (sector, industry, isin, name) sont preservees si la
    nouvelle valeur est vide, pour eviter de les ecraser.
    """
    try:
        # Creer la table MTM si elle n'existe pas encore dans ce fichier DB
        con.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_positions_mtm_latest (
                symbol              VARCHAR PRIMARY KEY,
                row_number          INTEGER,
                symbol_raw          VARCHAR,
                name                VARCHAR,
                asset_class         VARCHAR,
                sector              VARCHAR,
                industry            VARCHAR,
                isin                VARCHAR,
                quantity            DOUBLE,
                avg_price           DOUBLE,
                last_price          DOUBLE,
                market_value        DOUBLE,
                unrealized_pnl      DOUBLE,
                updated_at          TIMESTAMP,
                source_updated_at   VARCHAR,
                run_id              VARCHAR,
                ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Supprimer les positions cloturees (plus dans le snapshot courant)
        con.execute("""
            DELETE FROM portfolio_positions_mtm_latest
            WHERE symbol NOT IN ('CASH_EUR', '__META__')
              AND symbol NOT IN (
                  SELECT symbol FROM core.positions_snapshot WHERE run_id = ?
              )
        """, [run_id])

        # Mettre a jour / inserer les positions depuis le ledger
        con.execute("""
            INSERT INTO portfolio_positions_mtm_latest (
                symbol, row_number, symbol_raw, name, asset_class, sector, industry, isin,
                quantity, avg_price, last_price, market_value, unrealized_pnl,
                updated_at, source_updated_at, run_id
            )
            SELECT
                pos.symbol,
                CAST(ROW_NUMBER() OVER (ORDER BY pos.market_value_eur DESC NULLS LAST) AS INTEGER),
                pos.symbol,
                COALESCE(NULLIF(inst.name,        ''), pos.symbol),
                COALESCE(NULLIF(inst.asset_class, ''), 'Equity'),
                COALESCE(inst.sector,   ''),
                COALESCE(inst.industry, ''),
                COALESCE(inst.isin,     ''),
                pos.qty,
                pos.avg_cost,
                pos.last_price,
                pos.market_value_eur,
                pos.unrealized_pnl_eur,
                TRY_CAST(pos.ts AS TIMESTAMP),
                pos.ts,
                pos.run_id
            FROM core.positions_snapshot pos
            LEFT JOIN core.instruments inst ON inst.symbol = pos.symbol
            WHERE pos.run_id = ?
            ON CONFLICT (symbol) DO UPDATE SET
                row_number        = excluded.row_number,
                symbol_raw        = excluded.symbol_raw,
                quantity          = excluded.quantity,
                avg_price         = excluded.avg_price,
                last_price        = excluded.last_price,
                market_value      = excluded.market_value,
                unrealized_pnl    = excluded.unrealized_pnl,
                updated_at        = excluded.updated_at,
                source_updated_at = excluded.source_updated_at,
                run_id            = excluded.run_id,
                name        = CASE WHEN excluded.name        IS NOT NULL AND excluded.name        <> '' THEN excluded.name        ELSE portfolio_positions_mtm_latest.name        END,
                asset_class = CASE WHEN excluded.asset_class IS NOT NULL AND excluded.asset_class <> '' THEN excluded.asset_class ELSE portfolio_positions_mtm_latest.asset_class END,
                sector      = CASE WHEN excluded.sector      IS NOT NULL AND excluded.sector      <> '' THEN excluded.sector      ELSE portfolio_positions_mtm_latest.sector      END,
                industry    = CASE WHEN excluded.industry    IS NOT NULL AND excluded.industry    <> '' THEN excluded.industry    ELSE portfolio_positions_mtm_latest.industry    END,
                isin        = CASE WHEN excluded.isin        IS NOT NULL AND excluded.isin        <> '' THEN excluded.isin        ELSE portfolio_positions_mtm_latest.isin        END
        """, [run_id])

        # Mettre a jour le cash depuis core.portfolio_snapshot (source de verite)
        con.execute("""
            INSERT INTO portfolio_positions_mtm_latest (
                symbol, row_number, symbol_raw, name, asset_class, sector, industry, isin,
                quantity, avg_price, last_price, market_value, unrealized_pnl,
                updated_at, source_updated_at, run_id
            )
            SELECT
                'CASH_EUR', 0, 'CASH_EUR', 'Cash', 'Cash', 'Cash', 'Cash', '',
                0.0, 1.0, 1.0,
                CAST(ps.cash_eur AS DOUBLE),
                0.0,
                TRY_CAST(ps.ts AS TIMESTAMP),
                ps.ts,
                ps.run_id
            FROM core.portfolio_snapshot ps
            WHERE ps.run_id = ?
            ON CONFLICT (symbol) DO UPDATE SET
                market_value      = excluded.market_value,
                updated_at        = excluded.updated_at,
                source_updated_at = excluded.source_updated_at,
                run_id            = excluded.run_id
        """, [run_id])

        return True
    except Exception:
        return False


items = _items or []
if not items:
    return []

out = []
for it in items:
    j = dict(it.get("json", {}) or {})

    # --- LE NOUVEAU DÉTECTEUR ---
    # Si le Noeud 9 a échoué, on affiche SA vraie erreur et on s'arrête
    if j.get("status") == "FATAL_ERROR":
        err_msg = j.get("error_message", "Erreur inconnue")
        trace = j.get("traceback", "")
        raise RuntimeError(f"🔥 LE NOEUD 9 A ÉCHOUÉ :\nErreur: {err_msg}\nTrace:\n{trace}")
    # ----------------------------

    run_id = str(j.get("run_id") or "").strip()
    db_path = str(j.get("db_path") or "").strip()
    if not run_id:
        raise ValueError("Post-Run Health: missing run_id")
    if not db_path:
        raise ValueError("Post-Run Health: missing db_path")

    con = duckdb.connect(db_path)
    try:
        portfolio_row = con.execute(
            """
            SELECT
              COUNT(*) AS cnt,
              COALESCE(MAX(CAST(equity_eur AS DOUBLE)), 0) AS equity_eur
            FROM core.portfolio_snapshot
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
        positions_row = con.execute(
            "SELECT COUNT(*) FROM core.positions_snapshot WHERE run_id = ?",
            [run_id],
        ).fetchone()

        portfolio_cnt = int(portfolio_row[0] or 0)
        equity_eur = float(portfolio_row[1] or 0.0)
        positions_cnt = int(positions_row[0] or 0)

        health_ok = portfolio_cnt > 0 and (positions_cnt > 0 or equity_eur <= 0.0)
        checks = {
            "portfolio_snapshot_count": portfolio_cnt,
            "positions_snapshot_count": positions_cnt,
            "equity_eur": equity_eur,
            "rule": "portfolio must exist; positions may be empty only if equity is zero",
        }

        if not health_ok:
            alert_id = f"ALT|{run_id}|POST_HEALTH"
            payload = json.dumps(checks, ensure_ascii=False)
            ts = utc_now_iso()
            con.execute(
                """
                INSERT INTO core.alerts (
                  alert_id, run_id, ts, severity, category, symbol, message, code, payload_json
                )
                VALUES (?, ?, ?, 'CRITICAL', 'SYSTEM', 'GLOBAL', ?, 'POST_RUN_HEALTH_FAIL', ?)
                ON CONFLICT (alert_id) DO UPDATE SET
                  ts = excluded.ts,
                  message = excluded.message,
                  payload_json = excluded.payload_json
                """,
                [
                    alert_id,
                    run_id,
                    ts,
                    "Post-run health check failed: missing portfolio/positions snapshot for run",
                    payload,
                ],
            )

        # Synchroniser le ledger vers portfolio_positions_mtm_latest
        # afin que le prochain run lise le vrai cash et les vraies quantites
        mtm_sync_ok = _sync_ledger_to_mtm(con, run_id)

        out.append(
            {
                "json": {
                    "run_id": run_id,
                    "db_path": db_path,
                    "health_ok": health_ok,
                    "checks": checks,
                    "mtm_sync_ok": mtm_sync_ok,
                    "writer": j.get("writer_path"),
                    "upsert": j.get("upsert"),
                    "snapshots": j.get("snapshots"),
                },
                "pairedItem": it.get("pairedItem"),
            }
        )
    finally:
        try:
            con.close()
        except Exception:
            pass

return out
