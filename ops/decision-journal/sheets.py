"""Self-contained, escaped decision sheets. No assets, JS, telemetry or network requests."""
from html import escape
import json
from pathlib import Path
from urllib.parse import quote
from evidence import fingerprint
from journal import atomic


def h(x):
    return escape(str(x if x is not None else "non renseigné"))


def tab(heads, rows):
    rows=list(rows)
    if not rows:
        return "<p>Aucune ligne enregistrée.</p>"
    return "<table><thead><tr>"+"".join("<th>"+h(x)+"</th>" for x in heads)+"</tr></thead><tbody>"+"".join(
        "<tr>"+"".join("<td>"+h(v)+"</td>" for v in row)+"</tr>" for row in rows)+"</tbody></table>"


def details(label,data):
    return "<details><summary>"+h(label)+"</summary><pre>"+h(json.dumps(data,ensure_ascii=False,indent=2))+"</pre></details>"


def page(title,body):
    return ("""<!doctype html><html lang="fr"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'">
<title>"""+h(title)+"""</title><style>
*{box-sizing:border-box}body{font:16px/1.55 system-ui,sans-serif;color:#182830;background:#fafaf8;max-width:78rem;margin:2.5rem auto;padding:0 1.5rem}
h1,h2{line-height:1.2}h1{font-size:2rem}h2{margin-top:2rem;font-size:1.32rem}table{display:block;overflow:auto;border-collapse:collapse}
td,th{padding:.4rem .65rem;border-bottom:1px solid #d4ddda;text-align:left;vertical-align:top}th{background:#e9eeeb}
pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#edf1ef;padding:.8rem}details{margin:.6rem 0}summary{cursor:pointer}
a{color:#005b6d}small{color:#415559}</style><main><h1>"""+h(title)+"</h1>"+body+"</main></html>").encode()


def sheet_name(rid):
    return "run-"+fingerprint(rid)[:24]+".html"


def sheet(d,t,a):
    rid=d["run"]["run_id"]
    orders=d["orders"]
    body='<a href="index.html">Toutes les séances</a><p>'+h(rid)+" · exécution n8n "+h(d["run"].get("n8n_execution_id"))+"</p>"
    if t:
        body+="<p>Trace n8n du "+h(t["started_utc"])+" UTC. Version exécutée "+h(t["graph"]["version"])+".</p>"
    else:
        body+="<p>Le registre conserve les données figurant ci-dessous. Le texte des messages effectivement envoyés est indisponible.</p>"
    sel=None
    pack=(((t or {}).get("stages",{}).get("input_pack") or [{}])[0])
    if pack:
        sel=(pack.get("opportunity_pack") or {}).get("selection")
    if not sel and a:
        sel=a.get("selection")
    if sel:
        body+="<p>"+h(sel.get("screenedEntries"))+" entrées examinées, "+h(sel.get("eligibleEntries"))+" admissibles, "+h(sel.get("selectedEntries"))+" sélectionnées"+(" (résumé de l’audit historique)" if not pack else "")+".</p>"
    body+="<h2>Décision et suites</h2>"
    if not orders:
        if (t or {}).get("stages",{}).get("dispatch") and all(x.get("broker",{}).get("ordersSent")==0 for x in t["stages"]["dispatch"]):
            body+="<p>Le nœud d’envoi enregistre zéro ordre transmis.</p>"
        else:
            body+="<p>Aucun ordre enregistré pour cette séance dans le registre consulté.</p>"
    body+=tab(["Décision","Titre","Action","Votes","Consensus","Motif","Lien avec l’ordre"],(
        [x["decision_id"],x["symbol"],x["action"],x["vote_count"],x["status"],x["reason"],x["link_method"]] for x in d["decisions"]))
    for o in orders:
        body+="<h3>"+h(o["symbol"])+" · "+h(o["side"])+" · "+h(o["qty"])+"</h3>"
        body+="<p>Ordre "+h(o["order_id"])+" · référence client "+h(o["client_id"])+" · ordre broker "+h(o["broker_order_id"])+" · "+h(o["status"])+".</p>"
        body+=tab(["Exécution broker","Date UTC","Quantité","Prix natif","Devise","Commission EUR","Source","Frais registre EUR","Appariement"],
                  ([f["execution_id"],f["ts_fill"],f["qty"],f["price_native"],f["currency"],f["commission_eur"],f["commission_source"],f["fees_eur"],", ".join(f["matched_on"])] for f in o["fills"]))
    body+="<h2>Propositions et votes</h2>"+tab(["Modèle","Version","Extraction","Actions explicites"],(
        [x["model_key"],x["model_id"],x["extractor_status"],", ".join(str(i.get("action","?"))+" "+str(i.get("symbol","")) for i in (x["actions_json"] or []) if isinstance(i,dict)) or "Aucune action renseignée"] for x in d["models"]))
    body+=details("Propositions du registre",d["models"])+details("Votes nominatifs",d["votes"])+details("Consensus et identifiants des ordres",d["decisions"])
    body+="<h2>Entrées et contrôles</h2>"
    if t:
        body+=tab(["Branche","Modèle demandé","Modèle inscrit par l’extracteur","Source de la réponse"],(
            [c.get("ledger_model_key"),c.get("model"),c.get("declared_model"),c.get("response_source")] for c in t["calls"]))
        for c in t["calls"]:
            body+=details(str(c["model"])+" · messages et empreinte avant masquage",c)
        body+=details("Réponses et extractions disponibles",t["outputs"])+details("Pack et contrôles de faisabilité",pack)+details("Sécurité et envoi",{k:v for k,v in t["stages"].items() if k!="input_pack"})+details("Référence du graphe exécuté",t["graph"])
    if a and not t:
        body+=details("Extrait vérifié de l’audit",a)
    body+=details("Paramètres du run, coût IA en EUR si renseigné et contrôle du risque",d["run"])
    return page("Fiche de décision AG1",body)


def render(j,dest,reception):
    dest.mkdir(exist_ok=True,mode=0o700)
    baseline=j.get("audit","reference") or {}
    cases=baseline.get("available_pack_summaries",{})
    links=[]
    detailed=0
    model_sessions=0
    complete_inputs=0
    for rid,d in j.records("ledger"):
        eid=str(d["run"].get("n8n_execution_id") or "")
        t=j.get("trace",eid)
        detailed+=bool(t)
        complete_inputs+=bool(t and len({p["model"] for p in t["calls"] if p.get("input",{}).get("messages")})>=3)
        model_sessions+=bool(d["models"])
        name=sheet_name(rid)
        atomic(dest/name,sheet(d,t,cases.get(eid)))
        links.append((rid,name,len(d["orders"]),bool(t)))
    body="<p>"+h(model_sessions)+" séances avec propositions · "+h(detailed)+" traces n8n rattachées · "+h(complete_inputs)+" séances avec messages observés pour trois modèles.</p>"
    body+="<p>Réception du premier passage naturel après livraison · "+h(reception["etat"].replace("_"," "))+".</p>"+details("Détail du contrôle",reception)
    if baseline:
        body+="<p>Archive de référence · "+h(baseline["approved"])+" consensus approuvés, "+h(baseline["with_order"])+" reliés à un ordre et "+h(baseline["with_fill"])+" à une exécution par séance, titre et sens. "+h(baseline["sessions_with_proposals"])+" séances avec propositions.</p>"
    body+="<h2>Séances</h2><ul>"
    for rid,name,n,t in sorted(links,reverse=True):
        body+='<li><a href="'+quote(name)+'">'+h(rid)+'</a> · '+h(n)+" ordre(s) enregistré(s) · "+("trace n8n" if t else "registre historique")+"</li>"
    atomic(dest/"index.html",page("Journal privé des décisions AG1",body+"</ul>"))
