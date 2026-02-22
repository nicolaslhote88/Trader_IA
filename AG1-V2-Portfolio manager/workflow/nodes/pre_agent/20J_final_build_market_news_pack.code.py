from datetime import datetime, timedelta, timezone
import re
import unicodedata

# --- CONFIGURATION ---
LOOKBACK_DAYS = 20  # On regarde la tendance des 2 dernières semaines
MIN_IMPACT = 2      # On ignore les news anecdotiques

def safe_float(val):
    try:
        return float(val)
    except Exception:
        return 0.0

def parse_dt(val):
    """
    Parse dates tolérant:
    - ISO 8601 (avec 'Z' ou offset)
    - 'YYYY-MM-DD HH:MM[:SS]' (sans tz => UTC)
    - 'YYYY-MM-DD'
    Retourne datetime timezone-aware UTC, ou None.
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

def strip_accents(s: str) -> str:
    # "Énergie" -> "Energie" pour éviter doublons
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))

def norm_key(label: str) -> str:
    """
    Normalisation robuste pour dédoublonner :
    - trim
    - suppression accents
    - casefold
    - harmonisation séparateurs
    - collapse espaces
    """
    s = str(label or "").strip()
    if not s:
        return ""
    s = strip_accents(s)
    s = s.replace("’", "'")  # apostrophes
    s = re.sub(r"[\t\r\n]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Optionnel : uniformiser quelques séparateurs/punctuations
    s = s.replace(" / ", "/").replace(" - ", "-")
    return s.casefold()

def split_sectors(txt):
    """
    Retourne une liste de labels (string) en tolérant divers séparateurs.
    """
    if not isinstance(txt, str):
        return []
    s = txt.replace("|", ",").replace(";", ",").replace("/", ",")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]

# 1) Récupération des données depuis l'input n8n
items = _items or []
rows = []
for item in items:
    j = (item or {}).get("json") or {}
    if isinstance(j, dict) and j:
        rows.append(j)

if not rows:
    return [{"json": {"sector_brief": "No recent news data available."}}]

now = datetime.now(timezone.utc)
cutoff = now - timedelta(days=LOOKBACK_DAYS)

# 2) Scores sectoriels + mapping display
sector_scores = {}    # key_norm -> score
sector_counts = {}    # key_norm -> nb news
sector_display = {}   # key_norm -> label affichage (premier rencontré)

def register_label(label: str):
    k = norm_key(label)
    if not k:
        return None
    if k not in sector_display:
        # Conserve le 1er label rencontré (souvent déjà "bien")
        sector_display[k] = label.strip()
    return k

for row in rows:
    published = parse_dt(row.get("publishedAt"))
    if published is None or published < cutoff:
        continue

    impact = safe_float(row.get("ImpactScore", 0))
    if abs(impact) < MIN_IMPACT:
        continue

    winners = split_sectors(row.get("Winners"))
    losers = split_sectors(row.get("Losers"))

    for w in winners:
        k = register_label(w)
        if not k:
            continue
        sector_scores[k] = sector_scores.get(k, 0.0) + abs(impact)
        sector_counts[k] = sector_counts.get(k, 0) + 1

    for l in losers:
        k = register_label(l)
        if not k:
            continue
        sector_scores[k] = sector_scores.get(k, 0.0) - abs(impact)
        sector_counts[k] = sector_counts.get(k, 0) + 1

# 3) Génération du briefing
if not sector_scores:
    brief = f"MARKET REGIME: Neutral/Quiet. No significant sector divergence detected in the last {LOOKBACK_DAYS} days."
else:
    sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)

    bullish, bearish, neutral = [], [], []
    for k, score in sorted_sectors:
        label = sector_display.get(k, k)
        count = sector_counts.get(k, 0)
        entry = f"{label} (Score: {score:+.0f}, Vol: {count} news)"

        if score >= 10:
            bullish.append(entry)
        elif score <= -10:
            bearish.append(entry)
        else:
            neutral.append(entry)

    lines = [f"=== SECTOR MOMENTUM REPORT (Last {LOOKBACK_DAYS} Days) ==="]

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
