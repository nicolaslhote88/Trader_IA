# AG4-Forex - Dedicated FX News Watcher

AG4-Forex ingests dedicated FX sources from `infra/config/sources/fx_sources.yaml`,
deduplicates news, applies the AG4 geo/asset-class tagger, and writes actionable
FX/Mixed items into `/files/duckdb/ag4_forex_v1.duckdb`.

## Regeneration

```bash
python agents/trading-forex/AG4-Forex/build_workflow.py
```

Then import `agents/trading-forex/AG4-Forex/AG4-Forex-workflow.json` into n8n.

