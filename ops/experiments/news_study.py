from collections import defaultdict
from datetime import timedelta
from html import unescape
import re, unicodedata
from urllib.parse import urlsplit, parse_qsl, urlencode
from common import digest, moment


def norm(s):
    s = unicodedata.normalize("NFKD", unescape(s or "").lower())
    return " ".join(re.findall(r"[a-z0-9]+", "".join(c for c in s if not unicodedata.combining(c))))


def material(r, protocol):
    title = unescape(r.get("title") or "")
    return next((k for k, pattern in protocol["news_types"].items() if re.search(pattern, title, re.I)), None)


def url_id(r):
    u = urlsplit(r.get("canonical_url") or r.get("url") or "")
    if not u.hostname:
        return ""
    params = [
        (k, v)
        for k, v in parse_qsl(u.query)
        if k.lower() in ("id", "articleid", "article_id", "storyid", "newsid")
    ]
    path = u.path.rstrip("/")
    if path in ("/api/news", "/news", "/article", "/story") and not params:
        return ""
    if len(path) < 9 and not params:
        return ""
    return u.hostname + path + ("?" + urlencode(sorted(params)) if params else "")


def public_url(r):
    u = urlsplit(r.get("canonical_url") or r.get("url") or "")
    return u.scheme + "://" + url_id(r) if u.scheme in ("http", "https") and url_id(r) else None


def groups(rows, protocol):
    result = []
    buckets = defaultdict(list)
    links = {}
    for r in sorted(rows, key=lambda r: (r["seen"], r["id"])):
        title = norm(
            re.sub(
                r"^(correction|corrected|rectificatif|update\s*\d*)\s*[:–-]?\s*",
                "",
                r.get("title") or "",
                flags=re.I,
            )
        )
        words = set(title.split())
        url = url_id(r)
        sym = r["symbol"]
        date = moment(r["pub"] or r["seen"])
        candidates = list(reversed(buckets[sym][-100:]))
        if url and url in links:
            candidates.insert(0, links[url])
        eid = None
        for i in candidates:
            e = result[i]
            dt = abs((date - e["date"]).total_seconds()) / 3600
            same_url = bool(url and url in e["urls"])
            if dt > (168 if same_url else protocol["time_window_hours"]):
                continue
            common = len(words & e["words"]) / max(1, len(words | e["words"]))
            corrected = bool(re.match(r"^(correction|corrected|rectificatif)\b", title, re.I))
            if (same_url and (common >= 0.4 or title == e["key"] or (corrected and sym in e["symbols"]))) or (
                len(words) >= 4 and (title == e["key"] or common >= protocol["title_jaccard"])
            ):
                eid = i
                break
        if eid is None:
            eid = len(result)
            result.append(
                dict(
                    id=digest([r["id"], sym])[:20],
                    key=title,
                    words=words,
                    date=date,
                    urls=set(),
                    records=[],
                    symbols=set(),
                )
            )
        e = result[eid]
        e["records"].append(r)
        e["symbols"].add(sym)
        if url:
            e["urls"].add(url)
            links[url] = eid
        if eid not in buckets[sym]:
            buckets[sym].append(eid)
    return result


def evidence(rows, at, protocol):
    cutoff = moment(at)
    earliest = (cutoff - timedelta(days=30)).isoformat()
    raw = []
    analyzed = []
    for r in rows:
        if not r.get("seen") or not r.get("title"):
            continue
        # A later correction or enrichment must not supply a previous date's text or score.
        if max(x for x in (r.get("seen"), r.get("updated"), r.get("created")) if x) > at:
            continue
        published = r.get("pub") or r["seen"]
        if not earliest <= published <= at:
            continue
        raw.append(r)
        if r["in_digest"] and r.get("analyzed") and r["analyzed"] <= at:
            analyzed.append(r)
    seven = (cutoff - timedelta(days=7)).isoformat()

    def counts(rs):
        agg = defaultdict(lambda: dict(n7=0, n30=0, impact7=0, impact30=0))
        for x in rs:
            k = agg[x["symbol"]]
            impact = x.get("impact") or 0
            k["n30"] += 1
            k["impact30"] += impact
            if x["pub"] and x["pub"] >= seven:
                k["n7"] += 1
                k["impact7"] += impact
        return dict(agg)

    raw.sort(key=lambda x: (x["pub"] or x["seen"], x["id"]), reverse=True)
    baseline = counts(raw[:6000])
    old = defaultdict(list)
    for r in sorted(analyzed, key=lambda x: (x["pub"] or "", x["id"]), reverse=True):
        if len(old[r["symbol"]]) < 3:
            old[r["symbol"]].append(r)
    active = [
        r
        for r in analyzed
        if (r.get("pub") or r["seen"]) >= (cutoff - timedelta(days=protocol["news_window"])).isoformat()
    ]
    events = groups(active, protocol)
    pack = defaultdict(list)
    selected = []
    quant = []
    mapping = {}
    picked = set()
    for e in events:
        rs = e["records"]
        for r in rs:
            mapping[(r["symbol"], r["id"])] = e["id"]
        typed = [r for r in rs if material(r, protocol)]
        if not typed:
            continue
        updates = [
            r
            for r in rs
            if re.match(r"^(?:\w{1,9} - [\w-]+ )?(corrected|correction|rectificatif)\b", r["title"], re.I)
        ]
        official = [r for r in rs if r["source"] == "issuer_rss"]
        pick = max(updates or official or typed, key=lambda r: (r["seen"], r["id"]))
        rec = dict(
            id=e["id"],
            title=pick["title"],
            kind=material(typed[0], protocol),
            first=min(x["seen"] for x in rs),
            published=pick["pub"],
            symbols=sorted(e["symbols"]),
            official=[x["id"] for x in official],
            corrections=[x["id"] for x in updates],
            sources=[
                dict(id=x["id"], source=x["source"], at=x["seen"], published=x["pub"], url=public_url(x))
                for x in rs
            ],
            importance=abs(pick.get("impact") or 0),
        )
        selected.append(rec)
        for sym in e["symbols"]:
            pack[sym].append(rec)
            quant.append(dict(pick, symbol=sym))
    for sym, v in pack.items():
        pack[sym] = sorted(
            v, key=lambda e: (e["importance"], e["published"] or e["first"], e["id"]), reverse=True
        )[:3]
        for ev in pack[sym]:
            picked.update(x["id"] for x in ev["sources"])
    duplicate = sum(len(v) - len({mapping.get((sym, x["id"]), x["id"]) for x in v}) for sym, v in old.items())
    return dict(
        old=dict(old),
        pack=dict(pack),
        r8=baseline,
        v2=counts(quant),
        events=selected,
        raw=len(raw),
        duplicates=duplicate,
        picked=picked,
        active=active,
        analyzed=len(analyzed),
    )


def evaluate(view, rows, labels, base):
    items = {x["label"]: x for x in rows if x["label"] in labels}
    label_rows = []
    old = {a["id"] for v in view["old"].values() for a in v}
    selected = {s["id"] for ev in view["events"] for s in ev["sources"]}
    for k, y in labels.items():
        r = items.get(k)
        if r:
            label_rows.append(
                dict(
                    key=k,
                    symbol=r["symbol"],
                    title=r["title"],
                    label=y,
                    selected=r["id"] in selected,
                    initial_digest=r["id"] in old,
                )
            )
    segments = defaultdict(set)
    for r in base["rows"]:
        if r["scoped"]:
            for seg in r["segments"]:
                segments[seg].add(r["symbol"])
    segments["DÉTENUS FLEX"] = set(base["holdings"])
    return dict(
        labels=label_rows,
        missing_labels=len(labels) - len(label_rows),
        missed=sum(x["label"] == 1 and not x["selected"] for x in label_rows),
        false_positive=sum(x["label"] == 0 and x["selected"] for x in label_rows),
        duplicates_old=view["duplicates"],
        duplicates_new=sum(len(v) - len({e["id"] for e in v}) for v in view["pack"].values()),
        coverage=[
            dict(
                segment=s,
                n=len(v),
                old=len(v & view["old"].keys()),
                new=len(v & view["pack"].keys()),
                missing=sorted(v - view["pack"].keys()) if s == "DÉTENUS FLEX" else [],
            )
            for s, v in segments.items()
        ],
    )
