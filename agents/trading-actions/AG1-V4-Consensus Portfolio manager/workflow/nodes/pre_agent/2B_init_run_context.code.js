// 2B - Init Run Context
// Code node (typeVersion 2)
// Output: enrichit la config avec run { runId, timestampParis, timestampUtc, tz, executionId, versions }

const cfg = $json ?? {};
const N8N_CONTEXT = this;
const tz = String(cfg.timezone || "Europe/Paris");

const now = new Date();

const pad2 = (n) => String(n).padStart(2, "0");

// Construit un "local ISO" en timezone Europe/Paris via Intl (sans librairie externe)
const partsToObj = (parts) =>
  parts
    .filter((p) => p.type !== "literal")
    .reduce((acc, p) => ((acc[p.type] = p.value), acc), {});

const dtf = new Intl.DateTimeFormat("en-GB", {
  timeZone: tz,
  hour12: false,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

const parts = partsToObj(dtf.formatToParts(now));
const isoLocal = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`;

// Calcule l'offset (minutes) entre UTC et Europe/Paris a cet instant
// Astuce : on interprete l'heure "Paris" comme UTC, et on compare a l'instant reel.
const msLocalAsUtc = Date.parse(`${isoLocal}Z`);
const offsetMin = Math.round((msLocalAsUtc - now.getTime()) / 60000);
const sign = offsetMin >= 0 ? "+" : "-";
const abs = Math.abs(offsetMin);
const offsetStr = `${sign}${pad2(Math.floor(abs / 60))}:${pad2(abs % 60)}`;

const timestampParis = `${isoLocal}${offsetStr}`;
const timestampUtc = now.toISOString();

const weekday = new Intl.DateTimeFormat("en-US", {
  timeZone: tz,
  weekday: "short",
}).format(now);
const isWeekend = weekday === "Sat" || weekday === "Sun";
const allowWeekendRun = String(
  cfg.allow_weekend_run ??
    ((typeof $env !== "undefined" && $env.AG1_V4_ALLOW_WEEKEND_RUN) || "false")
).toLowerCase() === "true";

if (isWeekend && !allowWeekendRun) {
  return [];
}

const executionId = cfg.execution_id ? String(cfg.execution_id) : null;
const universeScope = ["EQUITY", "ETF", "CRYPTO"];
const envDbPath = (typeof $env !== "undefined" && $env.AG1_V4_DUCKDB_PATH) ? String($env.AG1_V4_DUCKDB_PATH) : "";
const envInitialCapital = (typeof $env !== "undefined" && $env.AG1_V4_INITIAL_CAPITAL_EUR) ? Number($env.AG1_V4_INITIAL_CAPITAL_EUR) : null;
const dbPath = String(cfg.ag1_v4_db_path || cfg.AG1_V4_DUCKDB_PATH || cfg.db_path || envDbPath || "/files/duckdb/ag1_v4_consensus.duckdb");
const initialCapitalEUR = Number(cfg.initialCapitalEUR || cfg.initial_capital_eur || envInitialCapital || 10000);

const brokerUrl = String((typeof $env !== "undefined" && $env.IBKR_BROKER_URL) || "http://ibkr-broker:8080").replace(/\/+$/, "");
const ibkrDryRun = String((typeof $env !== "undefined" && $env.IBKR_DRY_RUN) || "true").toLowerCase() !== "false";
const liveOrdersEnabled = String((typeof $env !== "undefined" && $env.AG1_ACTIONS_LIVE_ORDERS_ENABLED) || "false").toLowerCase() === "true";
const requireLiveAccountAlignment = String((typeof $env !== "undefined" && $env.AG1_ACTIONS_REQUIRE_LIVE_ACCOUNT_ALIGNMENT) || "true").toLowerCase() !== "false";

async function getJson(url) {
  if (N8N_CONTEXT?.helpers?.httpRequest) {
    return await N8N_CONTEXT.helpers.httpRequest({
      method: "GET",
      url,
      headers: { "Accept": "application/json" },
      json: true,
      timeout: 15000,
    });
  }
  throw new Error("No n8n HTTP helper available in this Code node");
}

if (!ibkrDryRun && liveOrdersEnabled && requireLiveAccountAlignment) {
  let health = null;
  try {
    health = await getJson(`${brokerUrl}/health`);
  } catch (err) {
    throw new Error(`AG1_V4_LIVE_PREFLIGHT_FAILED: unable to read ibkr-broker /health: ${err?.message || err}`);
  }

  const alignment = health?.account_alignment || {};
  const configured = String(alignment.configured_account_id || "").trim();
  const gatewayAccounts = Array.isArray(alignment.gateway_accounts) ? alignment.gateway_accounts : [];
  const gatewayIsPaper = alignment.gateway_is_paper === true;
  const aligned = alignment.aligned === true;
  const configuredLooksLive = configured && !configured.toUpperCase().startsWith("DU");
  if (!health?.authenticated || !aligned || (configuredLooksLive && gatewayIsPaper)) {
    throw new Error(
      `AG1_V4_LIVE_PREFLIGHT_BLOCKED: IBKR account is not aligned for live trading `
      + `(configured=${configured || "n/a"}, gateway=${gatewayAccounts.join(",") || "n/a"}, `
      + `selected=${alignment.selected_account || "n/a"}, gateway_is_paper=${gatewayIsPaper}, authenticated=${!!health?.authenticated})`
    );
  }
}

// run_id: RUN_YYYYMMDD_HHMMSS_<executionId|rand>
const yyyymmdd = `${parts.year}${parts.month}${parts.day}`;
const hhmmss = `${parts.hour}${parts.minute}${parts.second}`;
const rand = Math.random().toString(16).slice(2, 10) + Math.random().toString(16).slice(2, 10);
const runId = `RUN_${yyyymmdd}_${hhmmss}_${executionId ?? rand}`;

return [
  {
    json: {
      ...cfg,
      ag1_db_path: dbPath,
      db_path: dbPath,
      initialCapitalEUR,
      universe_scope: universeScope,
      run: {
        runId,
        timestampParis,
        timestampUtc,
        tz,
        offsetMin,
        executionId,
        strategyVersion: String(cfg.strategy_version || "strategy_v4_consensus"),
        configVersion: String(cfg.config_version || "ag1_v4_consensus_v1"),
        promptVersion: String(cfg.prompt_version || "prompt_v4_consensus"),
        model: "ag1_v4_consensus",
        db_path: dbPath,
        initialCapitalEUR,
        universe_scope: universeScope,
        inputSnapshot: {
          universe_scope: universeScope,
        },
      },
    },
  },
];
