"""Immutable private IBKR Activity Flex source documents and their reversible projections."""
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import sqlite3
import xml.etree.ElementTree as ET

TYPES = {"ChangeInNAV","CashReportCurrency","OpenPosition","Trade","CashTransaction","CorporateAction",
         "ConversionRate","StatementOfFundsLine","FxTransaction","Transfer","EquitySummaryByReportDateInBase"}
EVENTS = {"Trade","CashTransaction","CorporateAction","FxTransaction","Transfer","StatementOfFundsLine"}
IDS = {
    "Trade":("tradeID","ibExecID","transactionID"),
    "CashTransaction":("transactionID",), "CorporateAction":("transactionID",),
    "FxTransaction":("transactionID",), "Transfer":("transactionID",),
    "StatementOfFundsLine":("transactionID",),
}


def dump(v):
    return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(",",":"))


def digest(v):
    return hashlib.sha256(v).hexdigest()


def key_account(account):
    if not account or not str(account).strip():
        raise ValueError("Identité de compte manquante")
    return digest(str(account).strip().upper().encode())


def day(s):
    if not s:
        return ""
    s=str(s).strip()
    for pattern,fmt in ((r"^\d{8}(?=$|[; T,-])","%Y%m%d"),(r"^\d{4}-\d\d-\d\d","%Y-%m-%d")):
        m=re.match(pattern,s)
        if m:
            return datetime.strptime(m.group(),fmt).date().isoformat()
    raise ValueError("Format de date non reconnu. Choisir YYYYMMDD dans Flex.")


def generated(s):
    if not s:
        return ""
    if not re.fullmatch(r"(?:\d{8}|\d{4}-\d\d-\d\d)(?:[;, T-](?:\d{6}|\d\d:\d\d:\d\d))?",s):
        raise ValueError("Format de date de génération non reconnu")
    return re.sub(r"\D","",s).ljust(14,"0")


def num(x):
    if x is None or str(x).strip()=="":
        return None
    try:
        d=Decimal(str(x))
    except InvalidOperation as exc:
        raise ValueError("Valeur numérique invalide") from exc
    if not d.is_finite():
        raise ValueError("Valeur numérique non finie")
    return d


def parse(raw,account):
    if len(raw)>100*1024**2:
        raise ValueError("Relevé supérieur à 100 Mio")
    if re.search(br"<!\s*(DOCTYPE|ENTITY)",raw.replace(b"\x00",b""),re.I):
        raise ValueError("Déclaration XML non admise")
    try:
        root=ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("XML incomplet ou invalide") from exc
    if root.tag!="FlexQueryResponse" or root.get("type","").upper() not in ("AF","ACTIVITY","ACTIVITY FLEX"):
        raise ValueError("Un export XML Flex Activité est requis")
    sets=root.findall("./FlexStatements/FlexStatement")
    if not sets:
        raise ValueError("Export sans relevé")
    results=[]
    for stmt in sets:
        acct=stmt.get("accountId")
        if key_account(acct)!=account:
            raise ValueError("Compte du relevé différent du compte attendu")
        start,end=day(stmt.get("fromDate")),day(stmt.get("toDate"))
        if not start or not end or start>end:
            raise ValueError("Période invalide")
        info=stmt.find("AccountInformation")
        currency=info.get("currency","") if info is not None else ""
        head={"start":start,"end":end,"generated":generated(stmt.get("whenGenerated")),
              "currency":currency or stmt.get("baseCurrency",""),"sections":[el.tag for el in stmt]}
        if stmt.get("model") or any(child.get("accountId") not in (None,"",acct) for child in stmt.iter()):
            raise ValueError("Relevé de modèle ou mélange de comptes")
        rows=[]
        for el in stmt.iter():
            if el.tag in TYPES:
                values={k:v for k,v in el.attrib.items() if k not in ("accountId","acctAlias") and v!=""}
                for k in ("tradeDate","dateTime","settleDateTarget","settleDate","date","reportDate","fromDate","toDate"):
                    if values.get(k):
                        day(values[k])
                for k in ("amount","quantity","position","tradePrice","ibCommission","fxRateToBase","rate",
                          "startingValue","endingValue","startingCash","endingCash","endingSettledCash","proceeds","credit","debit"):
                    if k in values:
                        num(values[k])
                if el.tag=="ChangeInNAV":
                    from reconcile import NAV_FIELDS
                    for k in NAV_FIELDS & values.keys():
                        num(values[k])
                rows.append((el.tag,values))
        results.append((head,rows))
    return results


def connect(path):
    db=sqlite3.connect(path,timeout=30)
    db.row_factory=sqlite3.Row
    db.execute("pragma foreign_keys=on")
    db.executescript("""
        create table if not exists sources(id text primary key, raw blob not null, active integer not null);
        create table if not exists periods(source text references sources(id), seq integer, header text not null,
            primary key(source,seq));
        create table if not exists records(source text, seq integer, line integer, kind text, payload text,
            primary key(source,seq,line), foreign key(source,seq) references periods(source,seq));
        create table if not exists actions(id integer primary key, source text references sources(id), event text, utc text);
        create table if not exists scope(account text primary key);
    """)
    return db


def log(db,key,event):
    db.execute("insert into actions(source,event,utc) values(?,?,?)",(key,event,datetime.now(timezone.utc).isoformat()))


def ingest(db,raw,account):
    parts=parse(raw,account)
    key=digest(raw)
    with db:
        db.execute("begin immediate")
        bound=db.execute("select account from scope").fetchone()
        if bound and bound[0]!=account:
            raise ValueError("Base privée liée à un autre compte")
        db.execute("insert or ignore into scope values(?)",(account,))
        if db.execute("select 1 from sources where id=?",(key,)).fetchone():
            return key,False  # A withdrawn source stays withdrawn when its inbox file is seen again.
        db.execute("insert into sources values(?,?,1)",(key,raw))
        for seq,(head,rows) in enumerate(parts):
            db.execute("insert into periods values(?,?,?)",(key,seq,dump(head)))
            db.executemany("insert into records values(?,?,?,?,?)",((key,seq,i,kind,dump(p)) for i,(kind,p) in enumerate(rows)))
        log(db,key,"import")
    return key,True


def activate(db,key,enabled):
    with db:
        r=db.execute("update sources set active=? where id=? and active<>?",(int(enabled),key,int(enabled)))
        if r.rowcount:
            log(db,key,"réintégration" if enabled else "retrait")
        elif not db.execute("select 1 from sources where id=?",(key,)).fetchone():
            raise ValueError("Identifiant d’import inconnu")


def canonical(candidates):
    """Use broker generation time, not arrival order. Ties or undated conflicts stay open."""
    def signatures(rows):
        return {dump(x["v"]) for x in rows}
    if len(signatures(candidates))==1:
        return candidates[0]
    if any(not x["gen"] for x in candidates):
        return None
    last=max(x["gen"] for x in candidates)
    newest=[x for x in candidates if x["gen"]==last]
    return newest[0] if len(signatures(newest))==1 else None


def events(records):
    # Multiple identifiers may describe one execution on differently configured overlapping exports.
    parents={}
    def root(a):
        parents.setdefault(a,a)
        while a!=parents[a]:
            parents[a]=parents[parents[a]]
            a=parents[a]
        return a
    items=[]
    issues=[]
    for r in records:
        t,v=r["kind"],r["v"]
        if t not in EVENTS or v.get("model"):
            continue
        if t=="Trade" and v.get("levelOfDetail","EXECUTION").upper() not in ("EXECUTION","EXECUTIONS"):
            continue
        if (t=="StatementOfFundsLine" and not v.get("activityCode") and not v.get("transactionID")
                and v.get("activityDescription") in ("Starting Balance","Ending Balance") and num(v.get("amount"))==0):
            continue
        currency=v.get("currency","") if t=="StatementOfFundsLine" else ""
        ids=[(t,currency,k,v[k]) for k in IDS[t] if v.get(k) not in ("",None,"0")]
        if not ids:
            issues.append((t,"Identifiant source absent",r))
            continue
        for i in ids[1:]:
            parents[root(i)]=root(ids[0])
        items.append((ids,r))
    groups=defaultdict(list)
    for ids,r in items:
        groups[root(ids[0])].append(r)
    chosen=[]
    for rows in groups.values():
        r=canonical(rows)
        if r:
            chosen.append(r)
        else:
            issues.append((rows[0]["kind"],"Versions contradictoires sans ordre de révision certain",rows[0]))
    retired=set()
    valid=[]
    for r in chosen:
        v=r["v"]
        flags=set(re.split(r"[,;|\s]+",v.get("notes",v.get("code",""))))
        cancel="Ca" in flags
        correction="Co" in flags
        if r["kind"]=="Trade" and (cancel or correction):
            orig=v.get("origTradeID")
            if orig and orig!="0":
                if orig!=v.get("tradeID"):
                    retired.add(orig)
            else:
                issues.append(("Trade","Correction sans identifiant du trade d’origine",r))
                continue
            if cancel:
                continue
        valid.append(r)
    return [r for r in valid if r["kind"]!="Trade" or r["v"].get("tradeID") not in retired],issues


def load(db):
    periods=[]
    data=[]
    for s in db.execute("select p.* from periods p join sources s on s.id=p.source where s.active=1 order by p.source,p.seq"):
        h=json.loads(s["header"])
        periods.append(dict(h,source=s["source"],seq=s["seq"]))
        for r in db.execute("select kind,payload,line from records where source=? and seq=? order by line",(s["source"],s["seq"])):
            data.append(dict(kind=r["kind"],v=json.loads(r["payload"]),gen=h["generated"],source=s["source"],seq=s["seq"],line=r["line"],start=h["start"],end=h["end"]))
    return periods,data
