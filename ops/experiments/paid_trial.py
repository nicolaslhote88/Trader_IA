"""Run one reserved, manual inference batch through n8n's existing credential binding."""

from pathlib import Path
import hashlib, json, os, sqlite3, subprocess, sys, uuid
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
DST = Path("/var/lib/trader-research-feed/experiments")
ADMIN = Path("/var/lib/trader-experiment-budget")
CONTAINER = "root-n8n-1"


def hash_(v):
    return hashlib.sha256(
        json.dumps(v, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def run(*args, timeout=120):
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)


def publish(data):
    import grp

    tmp = DST / "paid.tmp"
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf8")
    os.chmod(tmp, 0o640)
    os.chown(tmp, 0, grp.getgrnam("trader-research").gr_gid)
    os.replace(tmp, DST / "paid.json")


def execute(jobs, protocol):
    prices = protocol
    ADMIN.mkdir(mode=0o700, exist_ok=True)
    budget = sqlite3.connect(ADMIN / "budget.sqlite", timeout=30)
    budget.execute(
        "create table if not exists batches(id text primary key,reserved real,status text,evidence text)"
    )
    batch = hash_([jobs, protocol])
    db = sqlite3.connect("file:/var/lib/docker/volumes/n8n_data/_data/database.sqlite?mode=ro", uri=True)
    graph = json.loads(
        db.execute(
            """select h.nodes from workflow_entity e join workflow_history h on e.id=h.workflowId and e.activeVersionId=h.versionId where e.id='AG1V4CONSENSUS' and e.active=1"""
        ).fetchone()[0]
    )
    template = next(x for x in graph if x["name"].startswith("Anthropic Messages"))
    assert (
        template["parameters"]["url"] == "https://api.anthropic.com/v1/messages"
        and "anthropicApi" in template["credentials"]
    )
    wid = "EXP" + batch[:13]
    nodes = [
        dict(
            parameters={},
            id=str(uuid.uuid4()),
            name="Lancement manuel",
            type="n8n-nodes-base.manualTrigger",
            typeVersion=1,
            position=[0, 0],
        )
    ]
    connections = {}
    reserve = 0
    for i, j in enumerate(jobs):
        assert (
            j["arm"] in protocol["arms"]
            and j["day"] in protocol["only_dates"]
            and j["hash"] == hash_(j["payload"])
        )
        user = json.dumps(j["payload"], ensure_ascii=False, separators=(",", ":"))
        body = dict(
            model=protocol["model"],
            system=protocol["system"],
            max_tokens=protocol["max_output"],
            messages=[dict(role="user", content=user)],
        )
        if protocol.get("structured"):
            item = {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "side": {"type": "string", "enum": ["BUY", "SELL"]},
                },
                "required": ["symbol", "side"],
                "additionalProperties": False,
            }
            body["output_config"] = {
                "format": {
                    "type": "json_schema",
                    "schema": {
                        "type": "object",
                        "properties": {"actions": {"type": "array", "items": item}},
                        "required": ["actions"],
                        "additionalProperties": False,
                    },
                }
            }
        # At most one billed token per UTF-8 byte plus ample message framing. No tools, cache, inference region surcharge or retry.
        size = len(json.dumps(body, ensure_ascii=False).encode()) + 500
        if size > 40000:
            raise ValueError("Paquet trop volumineux")
        reserve += (
            protocol["eur_reserve_per_usd"]
            * (
                size * prices["input_usd_per_million"]
                + protocol["max_output"] * prices["output_usd_per_million"]
            )
            / 1e6
        )
        prev = nodes[-1]["name"]
        name = f"Essai {i + 1:02d}"
        nodes.append(
            dict(
                id=str(uuid.uuid4()),
                name=name,
                type=template["type"],
                typeVersion=template["typeVersion"],
                position=[200 * (i + 1), 0],
                credentials=template["credentials"],
                onError="continueRegularOutput",
                retryOnFail=False,
                parameters=dict(
                    method="POST",
                    url="https://api.anthropic.com/v1/messages",
                    authentication="predefinedCredentialType",
                    nodeCredentialType="anthropicApi",
                    sendHeaders=True,
                    headerParameters={"parameters": [{"name": "anthropic-version", "value": "2023-06-01"}]},
                    sendBody=True,
                    specifyBody="json",
                    jsonBody=json.dumps(body, ensure_ascii=False),
                    options={"timeout": 90000},
                ),
            )
        )
        connections[prev] = {"main": [[{"node": name, "type": "main", "index": 0}]]}
    budget.execute("begin immediate")
    prev = budget.execute("select status,evidence from batches where id=?", (batch,)).fetchone()
    if prev:
        if prev[0] == "done" and not (DST / "paid.json").exists():
            receipt = json.loads(prev[1])
            receipt["earlier"] = [
                json.loads(raw)
                for (raw,) in budget.execute(
                    "select evidence from batches where status='done' and id<>?", (batch,)
                )
            ]
            publish(receipt)
        print("Réservation existante", prev[0])
        return
    used = budget.execute("select coalesce(sum(reserved),0) from batches").fetchone()[0]
    if used + reserve > min(5, protocol["cap_eur"]):
        raise ValueError("Plafond expérimental atteint")
    if db.execute("select 1 from workflow_entity where id=?", (wid,)).fetchone():
        raise ValueError("Identifiant déjà utilisé")
    budget.execute("insert into batches values(?,?,?,null)", (batch, reserve, "reserved"))
    budget.commit()
    wf = dict(
        id=wid,
        name="Trader IA - comparaison privée lots 11 et 12 " + batch[:8],
        active=False,
        nodes=nodes,
        connections=connections,
        settings={"executionOrder": "v1"},
        versionId=str(uuid.uuid4()),
    )
    p = ADMIN / (wid + ".json")
    p.write_text(json.dumps(wf, ensure_ascii=False))
    os.chmod(p, 0o600)
    remote = "/tmp/" + wid + ".json"
    assert run("docker", "cp", str(p), CONTAINER + ":" + remote).returncode == 0
    assert run("docker", "exec", "--user", "root", CONTAINER, "chown", "node:node", remote).returncode == 0
    args = [
        "docker",
        "exec",
        "-e",
        "N8N_RUNNERS_BROKER_PORT=5817",
        "-e",
        "N8N_DIAGNOSTICS_ENABLED=false",
        CONTAINER,
        "n8n",
    ]
    imp = run(*args, "import:workflow", "--input=" + remote)
    if (
        imp.returncode
        or not db.execute("select 1 from workflow_entity where id=? and active=0", (wid,)).fetchone()
    ):
        raise RuntimeError("Import inactif refusé")
    print(
        "Plafond réservé EUR",
        round(reserve, 4),
        "appels prévus",
        len(jobs),
        "workflow manuel",
        wid,
        flush=True,
    )
    ans = run(*args, "execute", "--id=" + wid, "--rawOutput", timeout=1800)
    txt = ans.stdout.decode(errors="replace")
    # CLI emits startup messages before its JSON result. Never log provider payloads or headers.
    data = None
    for pos in (i for i, c in enumerate(txt) if c == "{" and (i == 0 or txt[i - 1] == "\n")):
        try:
            obj, _ = json.JSONDecoder().raw_decode(txt[pos:])
        except ValueError:
            continue
        if isinstance(obj, dict) and isinstance(obj.get("data"), dict) and "resultData" in obj["data"]:
            data = obj
            break
    if data is None:
        print(
            "Résultat CLI indisponible",
            ans.returncode,
            "courtier de tâches occupé",
            "already in use" in txt.lower(),
        )
        raise RuntimeError("Contrôler l’exécution et conserver la réservation")
    records = []
    rd = data["data"]["resultData"]["runData"]
    for node, j in zip(nodes[1:], jobs):
        rs = rd.get(node["name"], [])
        try:
            v = rs[-1]["data"]["main"][0][0]["json"]
        except (KeyError, IndexError, TypeError):
            v = {}
        usage = v.get("usage", {})
        cost = (
            (
                usage["input_tokens"] * prices["input_usd_per_million"]
                + usage["output_tokens"] * prices["output_usd_per_million"]
            )
            / 1e6
            if "input_tokens" in usage and "output_tokens" in usage
            else None
        )
        text = "".join(b.get("text", "") for b in v.get("content", []) if b.get("type") == "text").strip()
        try:
            raw = json.loads(text.removeprefix("```json").removesuffix("```").strip())
            actions = raw["actions"]
            syms = {x["symbol"] for x in j["payload"]["stocks"]}
            valid = isinstance(actions, list) and all(
                a.get("symbol") in syms
                and a.get("side") in ("BUY", "SELL")
                and (a["side"] != "SELL" or a["symbol"] in j["payload"]["holdings"])
                for a in actions
            )
        except (ValueError, KeyError, TypeError, AttributeError):
            valid = False
            actions = []
        records.append(
            dict(
                run=j["run"],
                arm=j["arm"],
                input=j["hash"],
                response=hash_(v.get("content", [])),
                provider_id=v.get("id"),
                model=v.get("model"),
                usage=usage,
                usd=cost,
                actions=actions if valid else [],
                valid=valid,
                received=bool(v.get("id")),
                http_status=(v.get("error") or {}).get("status")
                if isinstance(v.get("error"), dict)
                else None,
            )
        )
    receipt = dict(
        batch=batch,
        workflow=wid,
        at=datetime.now(timezone.utc).isoformat(),
        structured=bool(protocol.get("structured")),
        reserved_eur=reserve,
        cap=protocol["cap_eur"],
        requested=protocol["model"],
        records=records,
    )
    budget.execute("update batches set status='done',evidence=? where id=?", (json.dumps(receipt), batch))
    budget.commit()
    receipt["earlier"] = [
        json.loads(raw)
        for (raw,) in budget.execute(
            "select evidence from batches where status='done' and id<>? order by rowid", (batch,)
        )
    ]
    publish(receipt)
    print(
        "Réponses reçues",
        sum(x["received"] for x in records),
        "valides",
        sum(x["valid"] for x in records),
        flush=True,
    )


if __name__ == "__main__":
    spec = json.loads((HERE / "paid_protocol.json").read_bytes())
    if "--structured" in sys.argv[2:]:
        spec["structured"] = True
    execute(json.loads(Path(sys.argv[1]).read_bytes()), spec)
