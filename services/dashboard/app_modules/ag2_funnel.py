from __future__ import annotations

from typing import Any

import pandas as pd


SUPPORTED_AG1_ASSET_CLASSES = {"EQUITY", "ETF", "CRYPTO"}
TECH_MAX_AGE_HOURS = 96.0


def _truthy(series: pd.Series, default: bool = False) -> pd.Series:
    if series is None:
        return pd.Series(dtype=bool)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(default).astype(bool)
    normalized = series.fillna(default).astype(str).str.strip().str.lower()
    return normalized.isin({"1", "true", "yes", "y", "ok"})


def _series(df: pd.DataFrame, name: str, default: Any) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series(default, index=df.index)


def _effective_age_hours(
    df: pd.DataFrame,
    stored_column: str,
    workflow_ts: pd.Series,
    now_utc: pd.Timestamp,
) -> pd.Series:
    stored = pd.to_numeric(_series(df, stored_column, pd.NA), errors="coerce")
    real = (now_utc - workflow_ts).dt.total_seconds() / 3600.0
    return pd.concat([stored, real], axis=1).max(axis=1, skipna=True)


def build_ag2_operational_scope(
    df_signals: pd.DataFrame,
    df_universe: pd.DataFrame | None,
    *,
    now_utc: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build the unambiguous AG2 -> AG1 operational funnel.

    The stages are intentionally monotonic and global. They do not depend on
    presentation filters:
      configured universe -> active AG2 rotation -> AG1-ready technical data
      -> no persisted AG2 LLM REJECT.
    """
    if df_signals is None or df_signals.empty:
        return pd.DataFrame(), _empty_metrics()

    now = pd.Timestamp.now(tz="UTC") if now_utc is None else pd.Timestamp(now_utc)
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")

    signals = df_signals.copy()
    signals.columns = [str(col).strip().lower() for col in signals.columns]
    signals["symbol"] = _series(signals, "symbol", "").fillna("").astype(str).str.strip().str.upper()
    signals = signals[signals["symbol"] != ""].copy()
    if signals.empty:
        return pd.DataFrame(), _empty_metrics()

    workflow_ts = pd.to_datetime(
        _series(signals, "workflow_date", pd.NaT), errors="coerce", utc=True
    )
    signals["_workflow_ts_sort"] = workflow_ts
    signals = (
        signals.sort_values("_workflow_ts_sort", na_position="first")
        .drop_duplicates("symbol", keep="last")
        .drop(columns="_workflow_ts_sort")
        .reset_index(drop=True)
    )

    universe_total = int(len(signals))
    if df_universe is not None and not df_universe.empty:
        universe = df_universe.copy()
        universe.columns = [str(col).strip().lower() for col in universe.columns]
        universe["symbol"] = _series(universe, "symbol", "").fillna("").astype(str).str.strip().str.upper()
        universe = universe[universe["symbol"] != ""].drop_duplicates("symbol", keep="last")
        if "enabled" in universe.columns:
            universe = universe[_truthy(universe["enabled"], default=True)]
        universe_total = int(len(universe))
        metadata_columns = [
            col
            for col in (
                "symbol",
                "asset_class",
                "segments",
                "is_held",
                "is_core",
                "is_watchlist",
                "quarantine_active",
            )
            if col in universe.columns
        ]
        signal_payload = signals.drop(
            columns=[col for col in metadata_columns if col != "symbol" and col in signals.columns],
            errors="ignore",
        )
        signals = universe[metadata_columns].merge(signal_payload, on="symbol", how="left")

    segments = _series(signals, "segments", "").fillna("").astype(str).str.strip()
    rotation = segments.ne("")
    for flag in ("is_held", "is_core", "is_watchlist"):
        rotation = rotation | _truthy(_series(signals, flag, False))

    quarantine = _truthy(_series(signals, "quarantine_active", False))
    asset_class = _series(signals, "asset_class", "EQUITY").fillna("EQUITY").astype(str).str.strip().str.upper()
    supported_asset = asset_class.isin(SUPPORTED_AG1_ASSET_CLASSES)

    workflow_ts = pd.to_datetime(
        _series(signals, "workflow_date", pd.NaT), errors="coerce", utc=True
    )
    h1_age = _effective_age_hours(signals, "data_age_h1_hours", workflow_ts, now)
    d1_age = _effective_age_hours(signals, "data_age_d1_hours", workflow_ts, now)
    h1_status_ok = _series(signals, "h1_status", "").fillna("").astype(str).str.upper().eq("OK")
    d1_status_ok = _series(signals, "d1_status", "").fillna("").astype(str).str.upper().eq("OK")
    closed_contract = _truthy(_series(signals, "h1_closed_only", False)) & _truthy(
        _series(signals, "d1_closed_only", False)
    )

    technical_contract = (
        workflow_ts.notna()
        & closed_contract
        & h1_status_ok
        & d1_status_ok
        & h1_age.le(TECH_MAX_AGE_HOURS)
        & d1_age.le(TECH_MAX_AGE_HOURS)
    )
    rotation_active = rotation & ~quarantine & supported_asset
    tech_ready = rotation_active & technical_contract

    ai_decision = _series(signals, "ai_decision", "").fillna("").astype(str).str.strip().str.upper()
    call_ai = _truthy(_series(signals, "call_ai", False))
    not_rejected = tech_ready & ai_decision.ne("REJECT")
    d1_action = _series(signals, "d1_action", "NEUTRAL").fillna("NEUTRAL").astype(str).str.upper()
    directional = tech_ready & d1_action.isin({"BUY", "SELL"})
    called_ready = tech_ready & call_ai

    signals["ag2_rotation_active"] = rotation_active
    signals["ag2_h1_age_hours_effective"] = h1_age
    signals["ag2_d1_age_hours_effective"] = d1_age
    signals["ag2_technical_contract"] = technical_contract
    signals["ag2_tech_ready"] = tech_ready
    signals["ag2_ai_not_rejected"] = not_rejected

    quality = pd.to_numeric(_series(signals, "ai_quality", pd.NA), errors="coerce")
    called_quality = quality[called_ready & quality.notna()]

    metrics: dict[str, Any] = {
        "universe_total": universe_total,
        "signals_latest": int(workflow_ts.notna().sum()),
        "rotation_active": int(rotation_active.sum()),
        "outside_rotation": max(0, universe_total - int(rotation_active.sum())),
        "tech_ready": int(tech_ready.sum()),
        "tech_blocked": int((rotation_active & ~technical_contract).sum()),
        "ai_not_rejected": int(not_rejected.sum()),
        "directional_d1": int(directional.sum()),
        "directional_buy": int((tech_ready & d1_action.eq("BUY")).sum()),
        "directional_sell": int((tech_ready & d1_action.eq("SELL")).sum()),
        "ai_calls_ready": int(called_ready.sum()),
        "ai_approve_ready": int((called_ready & ai_decision.eq("APPROVE")).sum()),
        "ai_watch_ready": int((called_ready & ai_decision.eq("WATCH")).sum()),
        "ai_reject_ready": int((called_ready & ai_decision.eq("REJECT")).sum()),
        "ai_other_ready": int((called_ready & ~ai_decision.isin({"APPROVE", "WATCH", "REJECT"})).sum()),
        "ai_quality_ready_mean": float(called_quality.mean()) if not called_quality.empty else None,
        "h1_age_p50": _quantile(h1_age[rotation_active], 0.50),
        "h1_age_p90": _quantile(h1_age[rotation_active], 0.90),
        "d1_age_p50": _quantile(d1_age[rotation_active], 0.50),
        "d1_age_p90": _quantile(d1_age[rotation_active], 0.90),
    }
    return signals, metrics


def _quantile(series: pd.Series, q: float) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.quantile(q)) if not values.empty else None


def _empty_metrics() -> dict[str, Any]:
    return {
        "universe_total": 0,
        "signals_latest": 0,
        "rotation_active": 0,
        "outside_rotation": 0,
        "tech_ready": 0,
        "tech_blocked": 0,
        "ai_not_rejected": 0,
        "directional_d1": 0,
        "directional_buy": 0,
        "directional_sell": 0,
        "ai_calls_ready": 0,
        "ai_approve_ready": 0,
        "ai_watch_ready": 0,
        "ai_reject_ready": 0,
        "ai_other_ready": 0,
        "ai_quality_ready_mean": None,
        "h1_age_p50": None,
        "h1_age_p90": None,
        "d1_age_p50": None,
        "d1_age_p90": None,
    }
