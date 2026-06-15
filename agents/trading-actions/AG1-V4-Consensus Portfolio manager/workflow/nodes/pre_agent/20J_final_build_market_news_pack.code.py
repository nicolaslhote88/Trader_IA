import json
import re
import unicodedata
from datetime import datetime, timedelta, timezone

# --- CONFIGURATION ---
LOOKBACK_DAYS_DEFAULT = 20
MIN_IMPACT = 2.0  # Ignore anecdotal news

# Canonical sector list (Yahoo-style labels used in AG4 universe normalization).
CANONICAL_SECTORS = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
    "Unknown",
]


def safe_float(val):
    try:
        return float(val)
    except Exception:
        return 0.0


def safe_int(val, default):
    try:
        if val is None or val == "":
            return default
        return int(float(val))
    except Exception:
        return default


def parse_dt(val):
    """
    Parse dates tolerating:
    - ISO 8601 (with 'Z' or offset)
    - 'YYYY-MM-DD HH:MM[:SS]' (naive => UTC)
    - 'YYYY-MM-DD'
    Returns UTC-aware datetime, or None.
    """
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
            dt = None

        if dt is None:
            for fmt in (
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M%z",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            ):
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


def split_labels(raw):
    """
    Parse lists from:
    - JSON arrays
    - Python-like stringified lists (single quotes)
    - Comma/pipe/semicolon-separated strings
    - Native arrays
    """
    if raw is None:
        return []

    if isinstance(raw, (list, tuple, set)):
        values = list(raw)
    else:
        s = str(raw).strip()
        if not s or s.lower() in ("nan", "none", "null"):
            return []

        values = None
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    values = parsed
            except Exception:
                values = None

            if values is None:
                inner = s[1:-1].strip()
                values = [] if not inner else re.split(r"[|;,]", inner)
        else:
            values = re.split(r"[|;,]", s)

    out = []
    seen = set()
    for v in values:
        x = str(v or "").strip()
        if not x:
            continue
        x = re.sub(r"^[\[\(\{]+|[\]\)\}]+$", "", x).strip()
        x = x.strip("\"'")
        x = re.sub(r"\s+", " ", x).strip()
        if not x:
            continue
        k = norm_key(x)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def build_allowed_sector_list(rows):
    # If an upstream node ever provides `universeSectors`, prefer it.
    detected = []
    seen = set()
    for row in rows:
        for label in split_labels(row.get("universeSectors")):
            k = norm_key(label)
            if not k or k in seen:
                continue
            seen.add(k)
            detected.append(label)
    if detected:
        return detected
    return list(CANONICAL_SECTORS)


def build_allowed_maps(allowed_labels):
    display_by_key = {}
    for label in allowed_labels:
        k = norm_key(label)
        if k and k not in display_by_key:
            display_by_key[k] = str(label).strip()

    # Common aliases (English + a few French variants) to canonical labels.
    aliases = {
        "basic materials": "Basic Materials",
        "materials": "Basic Materials",
        "materiaux de base": "Basic Materials",
        "communication services": "Communication Services",
        "telecommunication services": "Communication Services",
        "telecom services": "Communication Services",
        "services de communication": "Communication Services",
        "consumer cyclical": "Consumer Cyclical",
        "consumer discretionary": "Consumer Cyclical",
        "consommation cyclique": "Consumer Cyclical",
        "consumer defensive": "Consumer Defensive",
        "consumer staples": "Consumer Defensive",
        "consommation defensive": "Consumer Defensive",
        "energy": "Energy",
        "energie": "Energy",
        "financial services": "Financial Services",
        "financials": "Financial Services",
        "finance": "Financial Services",
        "services financiers": "Financial Services",
        "healthcare": "Healthcare",
        "health care": "Healthcare",
        "sante": "Healthcare",
        "industrials": "Industrials",
        "industrie": "Industrials",
        "real estate": "Real Estate",
        "immobilier": "Real Estate",
        "technology": "Technology",
        "information technology": "Technology",
        "technologie": "Technology",
        "utilities": "Utilities",
        "services publics": "Utilities",
        "unknown": "Unknown",
    }

    alias_to_allowed = {}
    for alias, canonical in aliases.items():
        alias_k = norm_key(alias)
        canonical_k = norm_key(canonical)
        if alias_k and canonical_k in display_by_key:
            alias_to_allowed[alias_k] = display_by_key[canonical_k]

    heuristic_rules = [
        ("communication", "Communication Services"),
        ("telecom", "Communication Services"),
        ("financial", "Financial Services"),
        ("bank", "Financial Services"),
        ("insurance", "Financial Services"),
        ("health", "Healthcare"),
        ("pharma", "Healthcare"),
        ("biotech", "Healthcare"),
        ("industrial", "Industrials"),
        ("aerospace", "Industrials"),
        ("defense", "Industrials"),
        ("technology", "Technology"),
        ("software", "Technology"),
        ("semiconductor", "Technology"),
        ("energy", "Energy"),
        ("oil", "Energy"),
        ("gas", "Energy"),
        ("utility", "Utilities"),
        ("real estate", "Real Estate"),
        ("reit", "Real Estate"),
        ("consumer cyc", "Consumer Cyclical"),
        ("discretionary", "Consumer Cyclical"),
        ("consumer def", "Consumer Defensive"),
        ("staples", "Consumer Defensive"),
        ("materials", "Basic Materials"),
    ]

    return display_by_key, alias_to_allowed, heuristic_rules


def match_sector(label, display_by_key, alias_to_allowed, heuristic_rules):
    raw = str(label or "").strip()
    if not raw:
        return None
    k = norm_key(raw)
    if not k:
        return None

    if k in display_by_key:
        return display_by_key[k]
    if k in alias_to_allowed:
        return alias_to_allowed[k]

    # Conservative cleanup match for labels with extra wrappers/punctuation.
    for allowed_k, allowed_label in display_by_key.items():
        if k == allowed_k:
            return allowed_label

    # Heuristic fallback for legacy winners/losers that may contain industry labels.
    for token, canonical in heuristic_rules:
        if token in k:
            canonical_k = norm_key(canonical)
            if canonical_k in display_by_key:
                return display_by_key[canonical_k]

    return None


# 1) Collect input rows from n8n
items = _items or []
rows = []
for item in items:
    j = (item or {}).get("json") or {}
    if isinstance(j, dict) and j:
        rows.append(j)

if not rows:
    return [
        {
            "json": {
                "sector_brief": "No recent macro news data available.",
                "sector_momentum_window_days": LOOKBACK_DAYS_DEFAULT,
                "sector_momentum_universe": CANONICAL_SECTORS,
                "sector_momentum_scores": [],
                "sector_momentum_meta": {"rows_total": 0, "rows_used": 0, "dropped_labels": 0},
            }
        }
    ]

lookback_days = LOOKBACK_DAYS_DEFAULT
for row in rows:
    if row.get("lookbackDays") is not None:
        lookback_days = max(1, safe_int(row.get("lookbackDays"), LOOKBACK_DAYS_DEFAULT))
        break

now = datetime.now(timezone.utc)
cutoff = now - timedelta(days=lookback_days)

allowed_sectors = build_allowed_sector_list(rows)
display_by_key, alias_to_allowed, heuristic_rules = build_allowed_maps(allowed_sectors)

# Track all sectors from universe even if net score is zero.
stats = {}
for label in allowed_sectors:
    k = norm_key(label)
    if not k:
        continue
    stats[k] = {
        "sector": str(label).strip(),
        "net": 0.0,
        "bull_score": 0.0,
        "bear_score": 0.0,
        "mentions": 0,
        "bull_mentions": 0,
        "bear_mentions": 0,
    }

rows_used = 0
dropped_labels = 0
raw_mentions = 0

for row in rows:
    if row.get("_emptyNews"):
        continue

    published = parse_dt(row.get("publishedAt"))
    if published is None or published < cutoff:
        continue

    impact = abs(safe_float(row.get("ImpactScore", 0)))
    if impact < MIN_IMPACT:
        continue

    rows_used += 1

    winners_raw = row.get("sectors_bullish")
    losers_raw = row.get("sectors_bearish")
    if winners_raw in (None, ""):
        winners_raw = row.get("Winners")
    if losers_raw in (None, ""):
        losers_raw = row.get("Losers")

    winners = []
    seen_w = set()
    for lbl in split_labels(winners_raw):
        raw_mentions += 1
        sector = match_sector(lbl, display_by_key, alias_to_allowed, heuristic_rules)
        if not sector:
            dropped_labels += 1
            continue
        sk = norm_key(sector)
        if sk in seen_w:
            continue
        seen_w.add(sk)
        winners.append(sk)

    losers = []
    seen_l = set()
    for lbl in split_labels(losers_raw):
        raw_mentions += 1
        sector = match_sector(lbl, display_by_key, alias_to_allowed, heuristic_rules)
        if not sector:
            dropped_labels += 1
            continue
        sk = norm_key(sector)
        if sk in seen_l:
            continue
        seen_l.add(sk)
        losers.append(sk)

    for sk in winners:
        if sk not in stats:
            continue
        stats[sk]["net"] += impact
        stats[sk]["bull_score"] += impact
        stats[sk]["mentions"] += 1
        stats[sk]["bull_mentions"] += 1

    for sk in losers:
        if sk not in stats:
            continue
        stats[sk]["net"] -= impact
        stats[sk]["bear_score"] += impact
        stats[sk]["mentions"] += 1
        stats[sk]["bear_mentions"] += 1


score_rows = list(stats.values())
for row in score_rows:
    row["netScore"] = round(row.pop("net"), 2)
    row["bullScore"] = round(row.pop("bull_score"), 2)
    row["bearScore"] = round(row.pop("bear_score"), 2)

score_rows.sort(key=lambda r: (r["netScore"], r["mentions"], r["sector"]), reverse=True)

active_rows = [r for r in score_rows if abs(r["netScore"]) > 0 or r["mentions"] > 0]
leaders = [r for r in active_rows if r["netScore"] >= 10]
laggards = [
    r
    for r in sorted(active_rows, key=lambda r: (r["netScore"], r["mentions"], r["sector"]))
    if r["netScore"] <= -10
]
mixed = [r for r in active_rows if -10 < r["netScore"] < 10]

if not active_rows:
    brief = (
        f"MARKET REGIME: Neutral/Quiet. No significant sector divergence detected in the last "
        f"{lookback_days} days (universe-aligned sectors only)."
    )
else:
    lines = [f"=== SECTOR MOMENTUM REPORT (Last {lookback_days} Days | Universe Sectors Only) ==="]
    lines.append(f"Tracked sectors: {len(score_rows)} | Active sectors: {len(active_rows)} | Rows used: {rows_used}")
    if raw_mentions > 0:
        lines.append(f"Filtered non-sector/legacy labels: {dropped_labels}/{raw_mentions}")

    def fmt_entry(r):
        return (
            f"{r['sector']} (Net: {r['netScore']:+.0f}, "
            f"Bull: {r['bullScore']:.0f}, Bear: {r['bearScore']:.0f}, "
            f"Mentions: {r['mentions']})"
        )

    if leaders:
        lines.append("")
        lines.append("LEADERS (Strong Positive Flows):")
        lines.extend([f"- {fmt_entry(r)}" for r in leaders[:6]])

    if laggards:
        lines.append("")
        lines.append("LAGGARDS (Strong Negative Flows):")
        lines.extend([f"- {fmt_entry(r)}" for r in laggards[:6]])

    if mixed:
        lines.append("")
        lines.append("MIXED / NEUTRAL (Universe sectors with conflicting or weak flows):")
        lines.extend([f"- {fmt_entry(r)}" for r in sorted(mixed, key=lambda r: abs(r['netScore']), reverse=True)[:6]])

    brief = "\n".join(lines)


return [
    {
        "json": {
            "sector_brief": brief,
            "sector_momentum_window_days": lookback_days,
            "sector_momentum_universe": [r["sector"] for r in score_rows],
            "sector_momentum_scores": score_rows,
            "sector_momentum_meta": {
                "rows_total": len(rows),
                "rows_used": rows_used,
                "lookback_days": lookback_days,
                "min_impact": MIN_IMPACT,
                "raw_mentions": raw_mentions,
                "dropped_labels": dropped_labels,
            },
        }
    }
]
