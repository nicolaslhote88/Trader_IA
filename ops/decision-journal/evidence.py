"""Evidence from finished n8n executions. Never read credentials or call a workflow."""
import hashlib
import json
import re
import sqlite3
from pathlib import Path

AG1 = "AG1V4CONSENSUS"
WORKFLOWS = (AG1, "iKnGA9gCMUFZfKYCCsWVF", "l94gHBuugVE6c2aX", "1uDcsNpDyXyAG616")


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False, default=str).encode()


def fingerprint(value):
    return hashlib.sha256(encode(value)).hexdigest()


# Account numbers can occur inside packs or prompt text. Hash the original for comparison.
PRIVATE_TEXT = re.compile(r"\b(?:DU|U)\d{6,12}\b|\bsk-[a-zA-Z0-9_-]{18,}|\b(?:Bearer|Basic) [a-zA-Z0-9._/+=-]{16,}", re.I)
PRIVATE_KEYS = {"account", "accountid", "account_id", "accountcode", "selected_account",
                "gateway_account", "api_key", "apikey", "authorization", "headers",
                "password", "secret", "token", "credentials", "signature", "reasoning_content", "thinking"}


def protect(v):
    if isinstance(v, dict):
        return {k: protect(x) for k, x in v.items() if k.lower() not in PRIVATE_KEYS}
    if isinstance(v, list):
        return [protect(x) for x in v]
    return PRIVATE_TEXT.sub("[masqué]", v) if isinstance(v, str) else v


def project(row, keys):
    return {k: row[k] for k in keys.split() if k in row}


def graph(wid, version, nodes, edges):
    # Configuration/code may embed a credential or an approval URL. Only their signatures leave n8n.
    return {"workflow": wid, "version": version, "connections": edges, "nodes": [
        {**project(n, "name type typeVersion disabled"), "parameters_sha256": fingerprint(n.get("parameters", {}))}
        for n in nodes]}


class Trace:
    """flatted serializes every string as a reference, including numeric-looking text."""
    def __init__(self, raw):
        self.pool = json.loads(raw)
        self.runs = self.follow(self.follow(self.pool[0]["resultData"]).get("runData", {}))

    def follow(self, ref):
        return self.pool[int(ref)] if isinstance(ref, str) else ref

    def expand(self, value):
        if isinstance(value, dict):
            return {k: self.expand(self.follow(v)) for k, v in value.items()}
        if isinstance(value, list):
            return [self.expand(self.follow(v)) for v in value]
        return value

    def items(self, name, branch="main", field="data"):
        for idx, ref in enumerate(self.follow(self.runs.get(name, []))):
            node = self.follow(ref)
            branches = self.follow(node.get(field, {})) or {}
            for lane in self.follow(branches.get(branch, [])) or []:
                for item in self.follow(lane) or []:
                    row = self.follow(self.follow(item).get("json", {}))
                    yield idx, row

    def take(self, row, keys):
        return {k: self.expand(self.follow(v)) for k, v in project(row, keys).items()}


def covers(published, executed):
    # n8n materializes additional default parameters when it prepares an execution.
    if isinstance(published, dict) and isinstance(executed, dict):
        return all(k in executed and covers(v,executed[k]) for k,v in published.items())
    if isinstance(published,list) and isinstance(executed,list):
        return len(published)==len(executed) and all(covers(a,b) for a,b in zip(published,executed))
    return type(published) is type(executed) and published==executed


def capture(raw, workflow, history):
    trace = Trace(raw)
    w = json.loads(workflow)
    g = graph(w["id"], w.get("activeVersionId") or w.get("versionId"), w["nodes"], w["connections"])
    evidence = {"graph": g, "calls": [], "outputs": {}, "stages": {}, "executed_nodes": list(trace.runs)}
    if history:
        nodes,edges = (json.loads(x) for x in history)
        by_name = {n["name"]:n for n in w["nodes"]}
        evidence["published_graph_sha256"] = fingerprint(graph(w["id"],g["version"],nodes,edges))
        evidence["published_parameters_verified"] = edges==w["connections"] and len(nodes)==len(by_name) and all(
            n["name"] in by_name and project(n,"name type typeVersion")==project(by_name[n["name"]],"name type typeVersion")
            and covers(n.get("parameters",{}),by_name[n["name"]].get("parameters",{})) for n in nodes)
    names = list(trace.runs)
    for name in names:
        if "Chat Model" in name:
            replies = {}
            for idx, row in trace.items(name, "ai_languageModel"):
                response = trace.follow(row.get("response", {}))
                texts = []
                for gen in trace.follow(response.get("generations", [])):
                    for obj in trace.follow(gen):
                        texts.append(trace.take(trace.follow(obj), "text"))
                replies[idx] = texts
            for idx, row in trace.items(name, "ai_languageModel", "inputOverride"):
                opts = trace.follow(row.get("options", {}))
                message = trace.take(row, "messages")
                evidence["calls"].append({"node": name, "index": idx, "model": trace.follow(opts.get("model")),
                    "input": message, "input_sha256": fingerprint(message), "response": replies.get(idx),
                    "source": "n8n.inputOverride.ai_languageModel"})
        elif name == "Agent #1 - Portfolio manager2":
            for idx, row in trace.items(name):
                msg = trace.take(row, "messages system")
                evidence["calls"].append({"node": name, "index": idx, "model": trace.follow(row.get("model")),
                    "input": msg, "input_sha256": fingerprint(msg), "source": "n8n.http_body"})
        elif name.startswith("Anthropic Messages"):
            for idx, row in trace.items(name):
                blocks = [trace.follow(x) for x in trace.follow(row.get("content", []))]
                texts = [trace.take(b, "text") for b in blocks if trace.follow(b.get("type")) == "text"]
                evidence["outputs"].setdefault(name, []).append({**trace.take(row,"model id stop_reason usage"),"index":idx,"response":texts})
        elif name.startswith("Agent #1 - Portfolio manager"):
            evidence["outputs"][name] = [trace.take(row, "output") for _, row in trace.items(name)]
        elif name.startswith("Information Extractor"):
            evidence["outputs"][name] = [trace.take(row, "output modelKey modelName modelId extractorStatus") for _,row in trace.items(name)]
        elif "Attach Dated Accounts" in name:
            evidence["stages"]["input_pack"] = [trace.take(row, "portfolio_pack opportunity_pack global_context opportunity_stats matrix_thresholds execution_audit") for _,row in trace.items(name)]
        elif "Validate & Enforce Safety" in name or "IBKR Send Orders" in name:
            stage = "dispatch" if "IBKR Send Orders" in name else "safety"
            results = []
            for _, row in trace.items(name):
                summary = trace.follow(row.get("ibkrSendSummary", {}))
                item = {"orders": [trace.take(trace.follow(o), "orderId clientOrderId brokerOrderId symbol side action quantity currency orderType limitPrice expectedFeesEUR riskCheckPassed riskChecks ibkrStatus") for o in trace.follow(row.get("orders", []))]}
                item.update(trace.take(row,"warnings"))
                if summary:
                    item["broker"] = trace.take(summary, "brokerCalled ordersConsidered ordersSent errors dryRun")
                results.append(item)
            evidence["stages"][stage] = results
        elif name.startswith("2B - Init Run Context"):
            for _, row in trace.items(name):
                ctx = trace.follow(row.get("run", {}))
                evidence["context"] = trace.take(ctx, "runId executionId strategyVersion promptVersion configVersion")
    def next_names(node, channel):
        return [e["node"] for lane in w["connections"].get(node,{}).get(channel,[]) for e in lane]
    for call in evidence["calls"]:
        if call["source"]=="n8n.http_body":
            agents=[call["node"]]
            for name in next_names(call["node"],"main"):
                for output in evidence["outputs"].get(name,[]):
                    if output.get("index")==call["index"]:
                        call["response"]=output["response"]
                        call["response_source"]=name
        else:
            agents=next_names(call["node"],"ai_languageModel")
            call["response_source"]=call["node"]
            if not any(x.get("text") for x in (call.get("response") or [])):
                call["response"]=[out for agent in agents for out in evidence["outputs"].get(agent,[]) if out.get("output")]
                call["response_source"]="n8n.agent_structured_output"
        # Follow actual graph connections to the extractor, including Claude's HTTP and parser nodes.
        frontier=list(agents)
        seen=set()
        while frontier:
            node=frontier.pop()
            if node in seen:
                continue
            seen.add(node)
            if node.startswith("Information Extractor"):
                results=evidence["outputs"].get(node,[])
                if results:
                    call["ledger_model_key"]=results[0].get("modelKey")
                    call["declared_model"]=results[0].get("modelId")
                break
            frontier.extend(next_names(node,"main"))
    return protect(evidence)


def collect_n8n(dbpath, cached, put):
    read=set()
    con = sqlite3.connect(Path(dbpath).resolve().as_uri()+"?mode=ro", uri=True, timeout=1)
    try:
        con.execute("PRAGMA query_only=ON")
        for wid in WORKFLOWS:
            row = con.execute("""select w.active,w.activeVersionId,h.nodes,h.connections
                from workflow_entity w left join workflow_history h on h.workflowId=w.id and h.versionId=w.activeVersionId
                where w.id=?""",(wid,)).fetchone()
            if not row or not row[0] or not row[2]:
                raise RuntimeError("Référence publiée indisponible pour "+wid)
            put("published", wid, graph(wid,row[1],json.loads(row[2]),json.loads(row[3])))
        rows = con.execute("""select id,mode,status,startedAt,stoppedAt from execution_entity
            where workflowId=? and stoppedAt is not null order by id""",(AG1,)).fetchall()
        for eid, mode, status, start, end in rows:
            key = str(eid)
            put("event",key,{"execution_id":key,"mode":mode,"status":status,"started_utc":start,"ended_utc":end})
            if cached("trace", key):
                continue
            data = con.execute("select data, workflowData from execution_data where executionId=?", (eid,)).fetchone()
            if data and all(data):
                meta=json.loads(data[1])
                version=meta.get("activeVersionId") or meta.get("versionId")
                history=con.execute("select nodes,connections from workflow_history where workflowId=? and versionId=?",(AG1,version)).fetchone()
                trace = capture(*data, history)
                trace.update({"execution_id":key, "mode":mode, "status":status, "started_utc":start, "ended_utc":end})
                put("trace",key,trace)
                read.add(key)
    finally:
        con.close()
    return read
