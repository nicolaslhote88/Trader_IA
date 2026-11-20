"""Publish private, static reconciliation reports from one consistent generation."""
import csv
from html import escape
import io
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from decimal import Decimal
from bridge import event_date
from statements import day,digest,dump,num


def put(path,data):
    with NamedTemporaryFile(dir=path.parent,delete=False) as stream:
        stream.write(data)
        temp=stream.name
    os.replace(temp,path)


def csvfile(path,rows,cols):
    buf=io.StringIO(newline="")
    writer=csv.writer(buf,delimiter=";")
    writer.writerow(cols)
    for r in rows:
        values=[]
        for k in cols:
            v=r.get(k)
            v="" if v is None else str(v)
            if v.lstrip().startswith(("=","+","-","@")):
                try:
                    num(v)
                except ValueError:
                    v="'"+v
            values.append(v)
        writer.writerow(values)
    put(path,buf.getvalue().encode("utf-8-sig"))


def fmt(x,digits=2):
    if x is None or x=="":return "—"
    return f"{num(x):,.{digits}f}".replace(","," ").replace(".",",")


def table(head,records):
    def cell(v):return escape(str(v)) if v is not None else "—"
    return "<div><table><thead><tr>"+"".join("<th>"+cell(x)+"</th>" for x in head)+"</tr></thead><tbody>"+"".join(
        "<tr>"+"".join("<td>"+cell(v)+"</td>" for v in r)+"</tr>" for r in records)+"</tbody></table></div>"


def all_months(ref):
    days=sorted(d for d,n in ref["nav"].items() if d<ref["captured"][:10] and n.get("value") is not None)
    months={}
    for dt in days:
        months.setdefault(dt[:7],[]).append(dt)
    prev=None
    out=[]
    for m,dates in months.items():
        first=ref["nav"][dates[0]]
        last=ref["nav"][dates[-1]]
        base=prev or first
        b,c=num(base["twr"]),num(last["twr"])
        ret=(1+c)/(1+b)-1 if b is not None and c is not None and 1+b!=0 else None
        out.append({"mois":m,"ouverture_observee":base["value"],"date_fin":dates[-1],"nav":last["value"],
                    "variation":num(last["value"])-num(base["value"]),"rendement":ret,"cumul":c})
        prev=last
    return out


def publish(dest,frozen,ref,out,problems,imports,release):
    # Publish all detail files first. The index switches atomically to a coherent set of relative links.
    generation=digest(json.dumps([release,ref["captured"],imports,out,problems],sort_keys=True,default=str,ensure_ascii=False).encode())
    sub=dest/"etats"/generation
    sub.mkdir(parents=True,exist_ok=True)
    lines=out["lines"]
    cols="periode rubrique etat montant comparatif ecart devise motif source".split()
    csvfile(sub/"controles.csv",lines,cols)
    execrows=out["exec_evidence"]
    csvfile(sub/"executions.csv",execrows,["execution","date_registre","date_collecteur","conid","registre","collecteur","qté_registre","qté_collecteur","état","motif"])
    csvfile(sub/"mouvements_ouverts.csv",[{"source":r["source"],"ligne":r["line"],"section":r["kind"],"motif":r["motif"],
            "piece":dump(r["v"])} for r in out["open_events"]],["source","ligne","section","motif","piece"])
    ev=[]
    for r in out["events"]:
        v=r["v"]
        ev.append({"section":r["kind"],"date":event_date(r),"règlement":v.get("settleDateTarget") or v.get("settleDate"),
                   "source":r["source"],"ligne":r["line"],"tradeID":v.get("tradeID"),"transactionID":v.get("transactionID"),
                   "ibExecID":v.get("ibExecID"),"conid":v.get("conid"),"devise":v.get("currency"),"type":v.get("type"),
                   "quantité":v.get("quantity") or v.get("position"),"montant":v.get("amount"),"produit":v.get("proceeds"),
                   "commission":v.get("ibCommission"),"devise_commission":v.get("ibCommissionCurrency"),"taux_base":v.get("fxRateToBase"),
                   "original":v.get("origTradeID"),"codes":v.get("notes") or v.get("code")})
    csvfile(sub/"mouvements.csv",ev,["section","date","règlement","tradeID","transactionID","ibExecID","conid","type","devise","quantité","montant","produit","commission","devise_commission","taux_base","original","codes","source","ligne"])
    monthly=all_months(ref)
    for m in monthly:m.update(out["couverture"][m["mois"]])
    csvfile(sub/"portfolioanalyst.csv",monthly,"mois ouverture_observee date_fin nav variation rendement cumul dates_couvertes dates_observees pont_dans_le_mois".split())
    counts=out["exec_summary"]
    tail=monthly[-1]
    historic=next(r for r in lines if r["rubrique"]=="Résidu historique à ventiler")
    open_balance=historic["ecart"] if historic["ecart"] is not None else historic["montant"]
    status={"version":release,"observation":ref["captured"],"imports_actifs":out["imports"],"periodes":out["periods"],
            "mouvements_uniques":len(ev),"incidents_import":problems,"evenements_a_instruire":out["issues"],
            "executions_hors_releve":counts,"residu_historique_eur":str(frozen["residual"]),"residu_restant_eur":str(open_balance),
            "date_derniere_nav_anterieure_au_jour":tail["date_fin"],"nav":str(tail["nav"]),"twr_cumule":str(tail["cumul"]),
            "etat_residu":historic["etat"],"couverture":out["couverture"],"generation":generation}
    put(sub/"etat.json",json.dumps(status,ensure_ascii=False,indent=2).encode())
    rel="etats/"+generation+"/"
    body="<h1>Rapprochement du compte IBKR</h1><p>"+str(out["imports"])+" import(s) actif(s) · "+str(out["periods"])+" période(s) de relevé · observation du "+escape(ref["captured"][:19].replace("T"," "))+" UTC.</p>"
    if not out["periods"]:
        body+="<p><b>Relevé d’activité en attente.</b> Déposer l’export Flex Activité XML dans le dossier entrant.</p>"
    if problems:
        body+="<h2>Pièces à reprendre</h2>"+table(["Fichier","Motif"],problems)
    body+="<h2>Écart du 18 novembre 2026</h2>"+table(["Composante du diagnostic","EUR"],[
        ["Valeur du compte observée chez IBKR",fmt(frozen["instant"])],["Capital de référence",fmt(frozen["capital"])],
        ["Résultat réalisé du registre FIFO",fmt(frozen["fifo"])],["Résultat latent du grand livre IBKR",fmt(frozen["latent"])],
        ["Écart initial",fmt(frozen["residual"])],["Montant restant à ventiler",fmt(open_balance)]])
    body+="<p>Le diagnostic combine une mesure instantanée du compte avec le réalisé interne. L’attribution requiert les rubriques du relevé et la différence de valorisation jusqu’à la clôture.</p>"
    rev=next((x for x in lines if x["rubrique"]=="Révision observée de la NAV PortfolioAnalyst"),None)
    if rev:
        body+="<p>Pour le 18 novembre, PortfolioAnalyst affichait "+fmt(rev["comparatif"])+" € dans l’audit et publie désormais "+fmt(rev["montant"])+" €. La variation mesurée entre les publications est de "+fmt(rev["ecart"])+" €. Elle est suivie séparément du résidu comptable.</p>"
    body+="<h2>Valeur et rendement publiés par PortfolioAnalyst</h2><p>Le TWR neutralise les apports et retraits. La dernière ligne s’arrête à la dernière date antérieure au jour de la collecte. La variation en euros inclut les flux éventuels.</p>"
    body+=table(["Période","Dernière date observée","NAV de départ (€)","NAV finale (€)","Variation (€)","TWR période","Dates couvertes par relevé","Pont du mois disponible"],(
        [m["mois"],m["date_fin"],fmt(m["ouverture_observee"]),fmt(m["nav"]),fmt(m["variation"]),fmt(m["rendement"]*100 if m["rendement"] is not None else None,3)+" %",str(m["dates_couvertes"])+"/"+str(m["dates_observees"]),"Oui" if m["pont_dans_le_mois"] else "En attente"] for m in monthly))
    body+="<h2>Registre d’exécutions et collecteur IBKR</h2><p>"+str(counts.get("rapproché",0))+" identifiants concordent en contrat, sens et quantité. "+str(counts.get("ouvert",0))+" nécessitent une pièce supplémentaire. Le détail indique dans quelle source chaque identifiant apparaît.</p>"
    assumed=sum("fallback_eur_no_ccy" in (x.get("commission_source") or "") for x in ref["fills"])
    body+="<p>Le registre a supposé l’euro pour la commission de "+str(assumed)+" exécution(s). La devise de commission du relevé permettra de les reprendre individuellement.</p>"
    if ref["positions"]:
        inventory=max(ref["positions"],key=lambda x:x["at"])
        body+="<details><summary>Dernières positions observées par contrat</summary><p>"+escape(inventory["at"])+"</p>"+table(
            ["Contrat IBKR","Titre","Quantité observée"],([x["conid"],x.get("symbol"),x["qty"]] for x in inventory["rows"]))+"</details>"
    if ref["cash"]:
        latest=max(x["date"] for x in ref["cash"])
        body+="<details><summary>Espèces par devise le "+escape(latest)+"</summary>"+table(["Devise","Espèces","Espèces réglées","Observation"],(
            [x["currency"],x["cashbalance"],x.get("settledcash"),x["at_utc"]] for x in ref["cash"] if x["date"]==latest and x["currency"] not in ("BASE","BASE_SUMMARY")))+"</details>"
    if ref["flows"]:
        body+="<details><summary>Flux externes inscrits au registre AG1</summary>"+table(["Date d’écriture","Type","Montant","Devise"],(
            [x["ts"],x["type"],x["amount"],x["currency"]] for x in ref["flows"] if x["type"] in ("DEPOSIT","WITHDRAWAL")))+"<p>La date d’écriture interne reste à comparer à la date de valeur IBKR.</p></details>"
    body+="<h2>Contrôles par période</h2>"+table(["Période","Rubrique","État","Montant","Comparatif","Écart","Unité","Justification"],(
        [r["periode"],r["rubrique"],r["etat"],str(r["montant"]) if r["montant"] is not None else None,
         str(r["comparatif"]) if r["comparatif"] is not None else None,str(r["ecart"]) if r["ecart"] is not None else None,r["devise"],r["motif"]] for r in lines))
    body+="<h2>Pièces importées</h2>"+(table(["Identifiant SHA-256","État"],imports) if imports else "<p>Aucun relevé importé.</p>")
    body+="<p>"+" · ".join('<a href="'+rel+fname+'">'+label+'</a>' for fname,label in
                             [("controles.csv","Contrôles détaillés"),("executions.csv","Identifiants d’exécution"),("mouvements.csv","Mouvements Flex"),("mouvements_ouverts.csv","Mouvements à qualifier"),("portfolioanalyst.csv","Série mensuelle"),("etat.json","État vérifiable")])+"</p>"
    html='<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Rapprochement IBKR</title><style>body{font:16px/1.5 system-ui,sans-serif;max-width:82rem;margin:2rem auto;padding:0 1rem;color:#18282e}h1,h2{line-height:1.2}h2{margin-top:2rem}div{overflow:auto}table{border-collapse:collapse}td,th{padding:.4rem .7rem;border-bottom:1px solid #ccd7d8;text-align:left;vertical-align:top}th{background:#eef3f2}a{color:#005d72}</style><main>'+body+'</main></html>'
    put(dest/"rapprochement.html",html.encode())
    put(dest/"etat.json",json.dumps(status,ensure_ascii=False,indent=2).encode())
    return status
