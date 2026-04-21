# RAG Qdrant Migration Notes (VectorDoc_v2)

## Scope

Workflows updated:

- `AG2-V3/AG2-V3 - Analyse technique.json` (`financial_tech_v1`)
- `AG3-V2/AG3-V2-workflow.json` (`fundamental_analysis`)
- `AG4-SPE-V2/AG4-SPE-V2-workflow.json` (`financial_news_v3_clean`)

## Required Environment Variables

- `QDRANT_URL`
- `QDRANT_API_KEY`

These are used by new HTTP delete nodes before each Qdrant insert.

## Idempotence Strategy

Because current n8n Qdrant insert mode does not guarantee a caller-controlled point id, idempotence is enforced with:

1. `Qdrant Delete (... by doc_id)` (HTTP POST `points/delete` with filter on `doc_id`)
2. `Qdrant Upsert` (insert documents)
3. `Mark Vectorized` in DuckDB with stable `vector_id = doc_id`

Also added:

- vectorization loops using `Split Vector Docs (...)` with `batchSize=1`
- SQL filters to only vectorize rows with `vector_status IS NULL OR vector_status IN ('PENDING','FAILED')` (AG2/AG3 + reinforced AG4)

## Metadata Changes (VectorDoc_v2)

All three workflows now carry `doc_id` and `schema_version` in vector metadata payload.

- `doc_id`: stable document id used for delete-by-filter
- `schema_version`: `"VectorDoc_v2"`
- `doc_kind`: `TECH` / `FUNDA` / `NEWS`

DuckDB vector mark now uses metadata `doc_id` (or `id`) as stable `vector_id`.
