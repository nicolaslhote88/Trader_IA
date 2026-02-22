# Nodes Export

Extracted node definitions and code from the workflow JSON.

- `pre_agent/`: data preparation and context building before Agent #1
- `agent_input/`: final input assembly and Agent #1 related nodes
- `post_agent/`: validation, bundling, DuckDB write, health checks

`NODE_SUMMARY.tsv` is the quick index used for review/diff.
