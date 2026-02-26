// 2B - Init Run Context
// Code node (typeVersion 2)
// Output: enrichit la config avec run { runId, timestampParis, timestampUtc, tz, executionId, versions }

const cfg = $json ?? {};
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

// Calcule l’offset (minutes) entre UTC et Europe/Paris à cet instant
// Astuce : on interprète l'heure "Paris" comme UTC, et on compare à l'instant réel.
const msLocalAsUtc = Date.parse(`${isoLocal}Z`);
const offsetMin = Math.round((msLocalAsUtc - now.getTime()) / 60000);
const sign = offsetMin >= 0 ? "+" : "-";
const abs = Math.abs(offsetMin);
const offsetStr = `${sign}${pad2(Math.floor(abs / 60))}:${pad2(abs % 60)}`;

const timestampParis = `${isoLocal}${offsetStr}`;
const timestampUtc = now.toISOString();

const executionId = cfg.execution_id ? String(cfg.execution_id) : null;

// run_id: RUN_YYYYMMDD_HHMMSS_<executionId|rand>
const yyyymmdd = `${parts.year}${parts.month}${parts.day}`;
const hhmmss = `${parts.hour}${parts.minute}${parts.second}`;
const rand = Math.random().toString(16).slice(2, 10) + Math.random().toString(16).slice(2, 10);
const runId = `RUN_${yyyymmdd}_${hhmmss}_${executionId ?? rand}`;

return [
  {
    json: {
      ...cfg,
      run: {
        runId,
        timestampParis,
        timestampUtc,
        tz,
        offsetMin,
        executionId,
        strategyVersion: String(cfg.strategy_version || "strategy_v2"),
        configVersion: String(cfg.config_version || "config_v2"),
        promptVersion: String(cfg.prompt_version || "prompt_v2"),
        model: String(cfg.model || "gpt-5.2"),
      },
    },
  },
];
