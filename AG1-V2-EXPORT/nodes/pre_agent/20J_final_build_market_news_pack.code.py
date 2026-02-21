import gc
import os
import re
import time
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import duckdb

LOOKBACK_DAYS = 20
MIN_IMPACT = 2
AG2_DB_PATH = os.getenv("AG2_DUCKDB_PATH", "/files/duckdb/ag2_v2.duckdb")


def safe_float(val):
    try:
        return float(val)
    except Exception:
        return 0.0


def parse_dt(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        dt = val
    else:
        s = str(val).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = None
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            pass
        if dt is None:
            fmts = (
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M%z",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            )
            for fmt in fmts:
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except Exception:
                    dt = None
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def strip_accents(s):
    nfkd = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def norm_key(label):
    s = str(label or "").strip()
    if not s:
        return ""
    s = strip_accents(s)
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"[\t\r\n]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" / ", "/").replace(" - ", "-")
    return s.casefold()


def split_labels(txt):
    if not isinstance(txt, str):
        return []
    s = txt.replace("|", ",").replace(";", ",")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


@contextmanager
def db_con(path, retries=5, delay=0.25):
    con = None
    for i in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as exc:
            msg = str(exc).lower()
            if ("lock" in msg or "busy" in msg) and i < retries - 1:
                time.sleep(delay * (2 ** i))
                continue
            con = None
            break
    try:
        yield con
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def load_universe_sector_map(db_path):
    out = {}
    with db_con(db_path) as con:
        if con is None:
            return out
        try:
            rows = con.execute(
                """
                SELECT DISTINCT TRIM(sector) AS sector
                FROM universe
                WHERE sector IS NOT NULL AND TRIM(sector) <> ''
                ORDER BY sector
                """
            ).fetchall()
        except Exception:
            return out

    for row in rows:
        label = str(row[0] or "").strip()
        key = norm_key(label)
        if key and key not in out:
            out[key] = label
    return out


def match_allowed_sector(label, allowed_map):
    key = norm_key(label)
    if not key:
        return ""

    exact = allowed_map.get(key)
    if exact:
        return exact

    for allowed_key, allowed_label in allowed_map.items():
        if key in allowed_key or allowed_key in key:
            return allowed_label
    return ""


items = _items or []
rows = []
for item in items:
    j = (item or {}).get("json") or {}
    if isinstance(j, dict) and j:
        rows.append(j)

if not rows:
    return [{"json": {"sector_brief": "No recent news data available."}}]

allowed_sector_map = load_universe_sector_map(AG2_DB_PATH)
allowed_sector_count = len(allowed_sector_map)

now = datetime.now(timezone.utc)
cutoff = now - timedelta(days=LOOKBACK_DAYS)

sector_scores = {}
sector_counts = {}
sector_display = {}
filtered_out = 0


def register_label(label):
    key = norm_key(label)
    if not key:
        return None
    if key not in sector_display:
        sector_display[key] = str(label).strip()
    return key


for row in rows:
    published = parse_dt(row.get("publishedAt"))
    if published is None or published < cutoff:
        continue

    impact = safe_float(row.get("ImpactScore", 0))
    if abs(impact) < MIN_IMPACT:
        continue

    winners = split_labels(row.get("Winners"))
    losers = split_labels(row.get("Losers"))

    for raw in winners:
        matched = match_allowed_sector(raw, allowed_sector_map)
        if not matched:
            filtered_out += 1
            continue
        key = register_label(matched)
        if not key:
            continue
        sector_scores[key] = sector_scores.get(key, 0.0) + abs(impact)
        sector_counts[key] = sector_counts.get(key, 0) + 1

    for raw in losers:
        matched = match_allowed_sector(raw, allowed_sector_map)
        if not matched:
            filtered_out += 1
            continue
        key = register_label(matched)
        if not key:
            continue
        sector_scores[key] = sector_scores.get(key, 0.0) - abs(impact)
        sector_counts[key] = sector_counts.get(key, 0) + 1


if not sector_scores:
    reason = (
        "Universe sector list unavailable."
        if allowed_sector_count == 0
        else "No significant universe-sector divergence detected."
    )
    brief = (
        f"MARKET REGIME: Neutral/Quiet. {reason} "
        f"Window={LOOKBACK_DAYS}d. AllowedSectors={allowed_sector_count}."
    )
else:
    sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)

    bullish = []
    bearish = []
    neutral = []
    for key, score in sorted_sectors:
        label = sector_display.get(key, key)
        count = sector_counts.get(key, 0)
        entry = f"{label} (Score: {score:+.0f}, Vol: {count} news)"

        if score >= 10:
            bullish.append(entry)
        elif score <= -10:
            bearish.append(entry)
        else:
            neutral.append(entry)

    lines = [f"=== SECTOR MOMENTUM REPORT (Universe sectors only, last {LOOKBACK_DAYS} days) ==="]
    lines.append(f"Universe sectors considered: {allowed_sector_count}")
    if filtered_out > 0:
        lines.append(f"Filtered non-universe labels: {filtered_out}")

    if bullish:
        lines.append("\nLEADERS (Strong Buying Pressure):")
        lines.extend([f"- {x}" for x in bullish])

    if bearish:
        lines.append("\nLAGGARDS (Strong Selling Pressure):")
        lines.extend([f"- {x}" for x in bearish])

    if neutral:
        lines.append("\nMIXED/NEUTRAL (Conflicting flows):")
        lines.extend([f"- {x}" for x in neutral[:5]])

    brief = "\n".join(lines)

return [{"json": {"sector_brief": brief}}]
