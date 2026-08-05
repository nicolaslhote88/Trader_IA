// Attach one immutable advisory pack before the three-model fan-out.
// Mode: Run Once for All Items.

const items = $input.all().map((item) => item.json || {});
const base = items.find((row) => row.opportunity_pack && row.portfolio_pack) || {};
let pack = items.find((row) => row.schema_version === "AG1_GLOBAL_CONTEXT_PACK_V1") || null;

function stable(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stable(value[key])}`).join(",")}}`;
}

// SHA-256 autonome : le sandbox Code n'a pas besoin d'un module externe.
function sha256(text) {
  const rightRotate = (value, amount) => (value >>> amount) | (value << (32 - amount));
  const maxWord = 2 ** 32;
  let result = "";
  const words = [];
  const ascii = unescape(encodeURIComponent(String(text)));
  const asciiBitLength = ascii.length * 8;
  const hash = sha256.h = sha256.h || [];
  const k = sha256.k = sha256.k || [];
  let primeCounter = k.length;
  const isComposite = {};
  for (let candidate = 2; primeCounter < 64; candidate++) {
    if (!isComposite[candidate]) {
      for (let i = 0; i < 313; i += candidate) isComposite[i] = candidate;
      hash[primeCounter] = (candidate ** 0.5 * maxWord) | 0;
      k[primeCounter++] = (candidate ** (1 / 3) * maxWord) | 0;
    }
  }
  let padded = `${ascii}\x80`;
  while (padded.length % 64 !== 56) padded += "\x00";
  for (let i = 0; i < padded.length; i++) words[i >> 2] |= padded.charCodeAt(i) << ((3 - i) % 4) * 8;
  words[words.length] = (asciiBitLength / maxWord) | 0;
  words[words.length] = asciiBitLength;
  for (let j = 0; j < words.length;) {
    const chunk = words.slice(j, j += 16);
    const oldHash = hash.slice(0);
    let working = hash.slice(0, 8);
    for (let i = 0; i < 64; i++) {
      const w15 = chunk[i - 15];
      const w2 = chunk[i - 2];
      const a = working[0];
      const e = working[4];
      const temp1 = working[7]
        + (rightRotate(e, 6) ^ rightRotate(e, 11) ^ rightRotate(e, 25))
        + ((e & working[5]) ^ ((~e) & working[6])) + k[i]
        + (chunk[i] = i < 16 ? chunk[i] : (chunk[i - 16]
          + (rightRotate(w15, 7) ^ rightRotate(w15, 18) ^ (w15 >>> 3))
          + chunk[i - 7] + (rightRotate(w2, 17) ^ rightRotate(w2, 19) ^ (w2 >>> 10))) | 0);
      const temp2 = (rightRotate(a, 2) ^ rightRotate(a, 13) ^ rightRotate(a, 22))
        + ((a & working[1]) ^ (a & working[2]) ^ (working[1] & working[2]));
      working = [(temp1 + temp2) | 0].concat(working);
      working[4] = (working[4] + temp1) | 0;
      working.pop();
    }
    for (let i = 0; i < 8; i++) hash[i] = (working[i] + oldHash[i]) | 0;
  }
  for (let i = 0; i < 8; i++) {
    for (let j = 3; j + 1; j--) {
      const byte = (hash[i] >> (j * 8)) & 255;
      result += byte < 16 ? `0${byte.toString(16)}` : byte.toString(16);
    }
  }
  return result;
}

if (!pack) {
  pack = {
    schema_version: "AG1_GLOBAL_CONTEXT_PACK_V1",
    method_version: "GLOBAL_CONTEXT_SYNTHESIS_V1",
    snapshot_id: null,
    as_of: null,
    freshness_status: "missing",
    coverage_ratio: null,
    confidence: null,
    status: "GLOBAL_CONTEXT_UNAVAILABLE",
    advisory_only: true,
    macro_regime: {},
    rates_liquidity_regime: {},
    positioning_regime: {},
    fx_relative_valuation: { scope: "FX_RELATIVE_VALUATION_ONLY" },
    geopolitical_risk_regime: {},
    portfolio_exposure_review: [],
    opportunity_exposure_review: [],
    sector_overlays: [],
    country_overlays: [],
    critical_events: [],
    source_warnings: ["GLOBAL_CONTEXT_UNAVAILABLE"],
  };
  pack.payload_hash = sha256(stable(pack));
}

pack = JSON.parse(JSON.stringify(pack));
pack.advisory_only = true;
const run = { ...(base.run || {}) };
run.global_context_snapshot_id = pack.snapshot_id || null;
run.global_context_payload_hash = pack.payload_hash || sha256(stable({ ...pack, payload_hash: undefined }));
run.global_context_schema_version = pack.schema_version || null;
run.global_context_method_version = pack.method_version || null;
run.global_context_age = Number.isFinite(Number(pack.context_age_hours)) ? Number(pack.context_age_hours) : null;
run.global_context_status = pack.status || pack.freshness_status || "UNKNOWN";
run.global_context_pack = pack;

return [{
  json: {
    ...base,
    run,
    global_context: pack,
    __global_context_contract: {
      snapshot_id: run.global_context_snapshot_id,
      payload_hash: run.global_context_payload_hash,
      schema_version: run.global_context_schema_version,
      method_version: run.global_context_method_version,
      status: run.global_context_status,
      advisory_only: true,
    },
  },
}];
