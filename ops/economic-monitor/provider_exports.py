"""Minimise provider exports locally. Key labels, ids and account identifiers never leave the input."""
import csv
from datetime import datetime,timedelta
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import re
import zipfile
from collections import defaultdict

def import_exports(folder):
    manifests=[];daily=defaultdict(Decimal);tokens=defaultdict(lambda:defaultdict(int));priced=defaultdict(Decimal)
    priced_by_type=defaultdict(lambda:defaultdict(Decimal))
    ranges=defaultdict(list);covered=set()
    for f in sorted(folder.iterdir()):
        if f.suffix.lower() not in (".csv",".zip"):continue
        raw=f.read_bytes();sha=hashlib.sha256(raw).hexdigest()
        if f.name.startswith("claude_api_cost_"):provider="Anthropic"
        elif f.name.startswith("cost_"):provider="OpenAI"
        elif f.name.startswith("usage_data_"):provider="DeepSeek"
        else:continue
        dates=re.findall(r"20\d\d[-_]\d\d[-_]\d\d",f.name)
        if len(dates)!=2:raise ValueError("Période fournisseur inconnue")
        start,end=[datetime.fromisoformat(d.replace("_","-")).date() for d in dates]
        for i in range((end-start).days+1):
            key=(provider,str(start+timedelta(days=i)))
            if key in covered:raise ValueError("Exports fournisseur superposés")
            covered.add(key)
        manifests.append(dict(provider=provider,source=f.name,sha=sha,start=str(start),end=str(end)))
        ranges[provider].append([str(start),str(end)])
        def rows(b):return csv.DictReader(io.StringIO(b.decode("utf-8-sig")))
        if provider=="DeepSeek":
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                for n in z.namelist():
                    if not n.endswith(".csv"):continue
                    for r in rows(z.read(n)):
                        day=r["start_time_iso"][:10];key=(provider,day,r["model"])
                        if n.startswith("cost-"):
                            if r["currency"].upper()!="USD":raise ValueError("Devise export DeepSeek")
                            daily[key]+=Decimal(r["cost"])
                        elif n.startswith("amount-"):
                            tokens[key][r["type"]]+=int(r["amount"])
                            if r["price"]:
                                charge=Decimal(r["amount"])*Decimal(r["price"])
                                priced[key]+=charge
                                priced_by_type[key][r["type"]]+=charge
        else:
            for r in rows(raw):
                if provider=="OpenAI":
                    if r["amount_value"] and r["amount_currency"].upper()!="USD":raise ValueError("Devise export OpenAI")
                    day=r["start_time_iso"][:10];model="Organisation";amt=r["amount_value"]
                else:
                    day=r["usage_date_utc"];model=r["model"];amt=r["cost_usd"]
                if amt:daily[(provider,day,model)]+=Decimal(amt)
    return dict(manifests=manifests,ranges=ranges,lines=[
        dict(provider=p,date=d,model=m,usd=str(v),usage=dict(tokens.get((p,d,m),{})),
             component_usd={k:str(n) for k,n in priced_by_type.get((p,d,m),{}).items()},
             meter_delta=str(priced[(p,d,m)]-v) if (p,d,m) in priced else None)
        for (p,d,m),v in sorted(daily.items())])
