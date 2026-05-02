# AG4-Forex - Dedicated FX News Watcher

AG4-Forex ingests dedicated FX sources from `infra/config/sources/fx_sources.yaml`,
deduplicates news before the LLM call, applies the AG4 geo/asset-class tagger to
new items only, and writes actionable FX/Mixed items into
`/files/duckdb/ag4_forex_v1.duckdb`.

## Dedupe and LLM bypass

`02_add_keys.js` computes a stable `dedupeKey` from the canonical URL, or from
normalized title + publication day when no URL is available. `03_route_seen_fx_news.py`
then checks `main.fx_news_history` before OpenAI is called:

- new `dedupeKey` -> `_action=analyze`, continue to `20H1 - Analyze with OpenAI`;
- existing `dedupeKey` -> update `last_seen_at`, set `_action=skip`, return to the
  item loop without spending another LLM call;
- check error or missing history table -> fail open to `_action=analyze` so ingestion
  does not silently drop potentially new market news.

The `dedupe_key` primary key and `INSERT OR REPLACE` in `05_write_fx_news_duckdb.py`
remain as the final write-side safety net, but duplicate suppression is expected to
happen before the LLM node.

## Regeneration

```bash
python agents/trading-forex/AG4-Forex/build_workflow.py
```

Then import `agents/trading-forex/AG4-Forex/AG4-Forex-workflow.json` into n8n.
