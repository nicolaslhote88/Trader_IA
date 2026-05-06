import hashlib
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.request import Request, urlopen

CURRENCIES = ("USD", "EUR", "JPY", "GBP", "CHF", "AUD", "CAD", "NZD")
PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
    "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD",
    "AUDJPY", "AUDNZD", "AUDCAD",
    "NZDJPY", "NZDCAD", "CADJPY", "CHFJPY", "CADCHF", "CHFCAD", "JPYNZD",
]
SOURCES = [
    {"id": "fed_monetary", "url": "https://www.federalreserve.gov/feeds/press_monetary.xml", "currency": "USD", "tier": "S"},
    {"id": "ecb_press", "url": "https://www.ecb.europa.eu/rss/press.html", "currency": "EUR", "tier": "S"},
    {"id": "boj_whatsnew", "url": "https://www.boj.or.jp/en/rss/whatsnew.xml", "currency": "JPY", "tier": "S"},
    {"id": "boe_news", "url": "https://www.bankofengland.co.uk/rss/news", "currency": "GBP", "tier": "S"},
    {"id": "bis_press", "url": "https://www.bis.org/doclist/all_pressrels.rss", "currency": "", "tier": "S"},
]
HIGH_TERMS = ("rate", "monetary", "policy", "inflation", "cpi", "minutes", "statement", "decision", "intervention")


def text_of(node, names):
    for name in names:
        child = node.find(name)
        if child is not None and child.text:
            return child.text.strip()
    for child in list(node):
        tag = child.tag.split("}", 1)[-1].lower()
        if tag in {n.lower() for n in names} and child.text:
            return child.text.strip()
    return ""


def parse_dt(value):
    text = str(value or "").strip()
    if not text:
        return datetime.now(timezone.utc).isoformat()
    try:
        dt = parsedate_to_datetime(text)
    except Exception:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def impacted_pairs(currency):
    if not currency:
        return ["EURUSD", "USDJPY", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF"]
    return [p for p in PAIRS if currency in (p[:3], p[3:])]


def classify(source, title, snippet):
    ccy = source.get("currency") or ""
    body = f"{title} {snippet}".lower()
    impact = "High" if any(t in body for t in HIGH_TERMS) else "Medium"
    bull, bear = [], []
    hint = ""
    if ccy:
        if any(t in body for t in ("hawkish", "hike", "higher rate", "inflation", "tightening")):
            bull = [ccy]
            hint = f"{ccy} potentially bullish if official communication is hawkish."
        elif any(t in body for t in ("dovish", "cut", "lower rate", "easing", "slowdown")):
            bear = [ccy]
            hint = f"{ccy} potentially bearish if official communication is dovish."
    return impact, bull, bear, hint


def fetch_source(src, limit=12):
    req = Request(src["url"], headers={"User-Agent": "Trader-IA/AG4-FX-V1"})
    with urlopen(req, timeout=10) as resp:
        root = ET.fromstring(resp.read())
    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
    if not items:
        items = [node for node in root.iter() if node.tag.split("}", 1)[-1].lower() in ("item", "entry")]
    out = []
    for item in items[:limit]:
        title = text_of(item, ["title"])
        snippet = re.sub(r"<[^>]+>", " ", text_of(item, ["description", "summary", "content"]))
        published = parse_dt(text_of(item, ["pubDate", "published", "updated"]))
        if not title:
            continue
        impact, bull, bear, hint = classify(src, title, snippet)
        key = hashlib.sha1(f"{src['id']}|{title}|{published[:10]}".encode("utf-8")).hexdigest()
        out.append({
            "dedupe_key": key,
            "published_at": published,
            "title": title,
            "source": src["id"],
            "snippet": snippet[:1200],
            "impact_magnitude": impact,
            "impact_fx_pairs": ",".join(impacted_pairs(src.get("currency"))),
            "currencies_bullish": ",".join(bull),
            "currencies_bearish": ",".join(bear),
            "fx_directional_hint": hint,
            "regime": "CentralBankWatch",
            "confidence": "0.65" if src.get("tier") == "S" else "0.50",
            "impact_score": "7" if impact == "High" else "5",
            "urgency": "today",
            "origin": "official_source",
        })
    return out


ctx = (_items or [{"json": {}}])[0].get("json", {})
enabled = str(ctx.get("official_sources_enabled", "true")).lower() != "false"
items = []
errors = []
if enabled:
    for src in SOURCES:
        try:
            items.extend(fetch_source(src))
        except Exception as exc:
            errors.append(f"{src['id']}: {exc}")

return [{"json": {**ctx, "official_news": items, "official_news_pulled": len(items), "official_news_errors": errors}}]
