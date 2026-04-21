# AG2-V3 — Analyse technique (double agent)

## Versions actives en n8n

Les deux workflows officiels importés dans n8n sont :

- `AG2-V3/AG2-V3 - Analyse technique (FX only).json` — univers FOREX.
- `AG2-V3/AG2-V3 - Analyse technique (non-FX).json` — univers EQUITY / ETF / CRYPTO.

Ces deux JSON sont désormais les **sources de vérité** : édités directement (via n8n puis exportés) ou manuellement, sans étape de build intermédiaire. L'ancien workflow canonique (`AG2-V3/AG2-V3 - Analyse technique.json`) ainsi que les scripts `build_workflow.py` / `build_split_workflows.py` ont été retirés du repo dans la phase de nettoyage d'avril 2026.

## Double agent technique (ACTIONS/ETF + FOREX)

Chaque workflow route vers un agent LLM dédié :

- `AG2-V3 - Analyse technique (non-FX).json` : gate long-only (SELL → REJECT) pour EQUITY / ETF / CRYPTO.
- `AG2-V3 - Analyse technique (FX only).json` : gate bidirectionnel (BUY / SELL) avec filtres SMA200 + Bollinger + RSI.

Points importants :

- Prompt USER identique sur les 2 agents (injecte `ai_context` brut).
- Prompt SYSTEM spécifique par univers.

## Champs Forex AI (V3)

Le validator FOREX renvoie et persiste :

- `bb_status` → `ai_bb_status`
- `rsi_status` → `ai_rsi_status`

Ces champs sont disponibles dans DuckDB (`technical_signals`), la vue `v_ag2_fx_output`, les payloads vectoriels, et la sortie Google Sheets (si le nœud de sync est utilisé).

## Scripts nœuds

Les fichiers dans `AG2-V3/nodes/` reflètent le code embarqué dans les deux workflows. Pour resynchroniser manuellement le contenu d'un nœud code : copier depuis n8n → coller dans le fichier correspondant, puis committer.
