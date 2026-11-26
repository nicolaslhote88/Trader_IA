"""Pure transition checks. Orders are never submitted by the reporting process."""
from collections import Counter, defaultdict
from decimal import Decimal
from datetime import timedelta
from common import digest, moment


def pilot_gate(campaign, obs, now, mandate=None, owners=None, inflight=None, risk=None):
    eligible = [r["variant"] for r in campaign["results"] if r["verdict"] == "retenir"]
    blockers = []
    if not eligible:
        blockers.append("Aucune variante qualifiée sur les deux fenêtres.")
    if not mandate:
        blockers.append("Mandat réel nominatif à établir après la revue économique.")
    else:
        required = ("variant","campaign","capital","max_loss","operating_budget","review_by","suspend",
                    "approved_by","version","starts","fee_tolerance","cash_tolerance","max_signal_age_s")
        if any(mandate.get(k) is None for k in required) or mandate.get("variant") not in eligible or mandate.get("campaign") != campaign["id"]:
            blockers.append("Mandat incomplet ou version non qualifiée.")
        else:
            if (not mandate["approved_by"] or not mandate["suspend"] or mandate["version"]!=campaign.get("strategy") or
                min(mandate["capital"],mandate["max_loss"],mandate["max_signal_age_s"])<=0 or
                min(mandate["fee_tolerance"],mandate["cash_tolerance"],mandate["operating_budget"])<0 or
                not moment(mandate["starts"])<=now<=moment(mandate["review_by"])):
                blockers.append("Limites, signature ou dates du mandat réel invalides.")
            if (not risk or not risk.get("verified") or risk.get("drawdown_eur") is None or
                risk.get("operating_cost_eur") is None or risk.get("capital_deployed") is None or
                risk["drawdown_eur"]>=mandate["max_loss"] or risk["operating_cost_eur"]>mandate["operating_budget"] or
                risk["capital_deployed"]>mandate["capital"] or risk.get("duplicate") or risk.get("unexplained")):
                blockers.append("Risque, budget, doublons ou comptabilité du pilote à contrôler.")
    if not obs["broker"]["aligned"]:
        blockers.append("Compte IBKR réel non aligné.")
    if obs["broker"]["pending"] != 0 or inflight is None or inflight:
        blockers.append("Inventaire des ordres en vol et approbations à solder.")
    reconciled = moment(obs["reconciled"])
    if (not timedelta(0)<=now-moment(obs["at"])<=timedelta(minutes=5) or not reconciled or
        not timedelta(0)<=now-reconciled<=timedelta(hours=1)):
        blockers.append("Inventaire et rapprochement du compte à rafraîchir avant bascule.")
    actual=obs["broker"].get("positions")
    if actual is None or not owners or owners.get("cash") != "pilote" or not owners.get("legacy_fenced") or any(
        owners.get("positions",{}).get(s) not in ("pilote","AG1") for s in actual):
        blockers.append("Exclusivité des positions et de la trésorerie à constater dans la chaîne réelle.")
    return dict(state="en attente" if blockers else "prêt pour répétition",blockers=blockers,eligible=eligible,
                account_day=obs["account_day"],last_reconciled=obs["reconciled"],paper=obs["broker"]["paper"],
                pending=obs["broker"]["pending"], real_capital=None if not mandate else mandate.get("capital"))


def reconcile(initial, orders, fills, observed, tolerance, fee_tolerance=None):
    """A repeated identical fill is idempotent; conflicting or orphan executions suspend the handover.
    Amounts: executed native price times event FX; fees already converted to the account currency.
    Cash is compared to trade-date cash, with investor flows, taxes and FX cash adjustments explicit.
    """
    D = lambda x: Decimal(str(x))
    positions = defaultdict(Decimal,{k:D(v) for k,v in initial["positions"].items()})
    cash = D(initial["cash"]) + D(observed.get("cash_adjustment",0))
    seen, qty, errors = {}, defaultdict(Decimal), []
    fees=defaultdict(Decimal)
    requests = {o["id"]:o for o in orders}
    if len(requests) != len(orders):
        errors.append("identifiant d’ordre dupliqué")
    for fill in fills:
        eid = fill["execution"]
        if eid in seen:
            if seen[eid] != fill:
                errors.append("exécution contradictoire")
            continue
        seen[eid] = fill
        order = requests.get(fill["order"])
        if not order or fill["side"] != order["side"] or fill["symbol"] != order["symbol"]:
            errors.append("exécution sans ordre concordant")
            continue
        n, px, fx = (D(fill[k]) for k in ("qty","price","fx"))
        if min(n,px,fx)<=0 or order["side"] not in ("BUY","SELL"):
            errors.append("montant, change ou sens invalide")
            continue
        qty[fill["order"]] += n
        fees[fill["order"]] += D(fill["fee_eur"])
        signed = n if order["side"]=="BUY" else -n
        positions[order["symbol"]] += signed
        cash -= signed*px*fx + D(fill["fee_eur"])
    for oid,n in qty.items():
        if n>D(requests[oid]["qty"]):
            errors.append("quantité exécutée supérieure à l’ordre")
        if (fee_tolerance is not None and n==D(requests[oid]["qty"]) and requests[oid].get("expected_fee_eur") is not None
                and abs(fees[oid]-D(requests[oid]["expected_fee_eur"]))>D(fee_tolerance)):
            errors.append("écart de commission au-delà de la tolérance")
    differences = {s:str(positions[s]-D(observed["positions"].get(s,0)))
                   for s in set(positions)|set(observed["positions"]) if positions[s]!=D(observed["positions"].get(s,0))}
    delta = cash - D(observed["cash"])
    if differences or abs(delta)>D(tolerance):
        errors.append("écart de positions ou de trésorerie")
    return dict(status="suspendre" if errors else "rapproché",errors=errors, cash_delta=str(delta),
                quantities=differences,fills=len(seen),fees_eur={i:str(v) for i,v in fees.items()},
                remaining={i:str(D(o["qty"])-qty[i]) for i,o in requests.items()})


def retirement(obs):
    usage = Counter()
    for row in obs["calls"]:
        usage[row["workflow"].split("-",1)[-1]] += row["calls"]
    inventory = {x["id"]:dict(id=x["id"],name=x["name"],group=x["group"],published=x["version"],graph=x["graph"],
                 consumers=x["readers"],direct=x["direct_readers"],calls=usage.get(x["id"]),
                 state="en service",saving=None) for x in obs["inventory"]}
    for record in obs.get("changes",[]):
        wid=record["plan"]["producer"]
        if wid not in inventory and record.get("component"):
            inventory[wid]=dict(record["component"],state="inactif")
        if wid in inventory:
            item=inventory[wid]
            item["state"]=record["phase"]+" · "+("actif" if wid in {x["id"] for x in obs["inventory"]} else "inactif")
            if isinstance(record.get("result"),dict):
                item["saving"]=record["result"].get("saved_eur")
    return list(inventory.values())


def retirement_plan(items, decision, pilot_review, consumers):
    """Produce a single version-bound change/rollback record for the existing n8n deployment chain."""
    ids = decision.get("ids",[])
    if len(ids)!=1 or not pilot_review.get("accepted") or not decision.get("approved_by"):
        raise ValueError("Un retrait nommé et une revue du pilote acceptée sont requis")
    item = next((x for x in items if x["id"]==ids[0]),None)
    if not item or item["group"]=="AG1" or item["graph"]!=decision.get("graph"):
        raise ValueError("Version publiée ou périmètre de retrait incompatible")
    declared = set(item["consumers"]+item["direct"])
    if not declared or not declared.issubset(set(consumers)) or not decision.get("backup_sha"):
        raise ValueError("Lecteurs à vérifier ou sauvegarde absente")
    return dict(change_id=digest([item["id"],item["graph"],decision]),producer=item["id"],
                observed_version=item["published"], backup=decision["backup_sha"], consumers=sorted(declared),
                rollback="Réactiver la version sauvegardée et reprendre au dernier point de collecte validé.",
                expected_eur=decision["expected_eur"])


def verify_retirement(plan, before, after):
    if not set(plan["consumers"]).issubset(after.get("complete_cycles",[])):
        return "en attente du cycle des lecteurs"
    if (after["coverage"]<before["coverage"] or after["stale"]>before["stale"]
        or after["errors"]>before["errors"] or not after["dashboard_ok"]):
        return "rétablir la version sauvegardée"
    if before.get("billed_eur") is None or after.get("billed_eur") is None:
        return "économie en attente de facturation"
    if not before.get("days") or after.get("days")!=before["days"] or not before.get("scope") or before["scope"]!=after.get("scope"):
        return "périodes et périmètres de facturation à rendre comparables"
    return {"calls_saved":before["calls"]-after["calls"],"saved_eur":before["billed_eur"]-after["billed_eur"],
            "expected_eur":plan["expected_eur"]}
