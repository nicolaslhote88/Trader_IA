# AG2-V3 - Workflow Final

## Version unique

Le workflow officiel est:

- `AG2-V3/AG2-V3 - Analyse technique.json`

Il n'y a plus de versions miroir.

## Scripts nodes synchronises

Les fichiers `AG2-V3/nodes/*` sont sync depuis le workflow final via `build_workflow.py`.

## Double agent technique (ACTIONS/ETF + FOREX)

Le workflow AG2-V3 inclut maintenant un routage LLM dedie:

- `Route AI Agent (FX?)`:
  - `true` si `asset_class == "FX"` -> nœud `AI Validation GPT - FOREX`
  - `false` sinon (EQUITY/ETF/CRYPTO) -> nœud `AI Validation GPT - ACTIONS/ETF`
- Les deux branches convergent vers `Merge AI + Context`, puis `Extract AI + Write`.

Points importants:

- Prompt USER identique sur les 2 agents (injecte `ai_context` brut).
- Prompt SYSTEM specifique par univers:
  - ACTIONS/ETF: gate long-only (SELL => REJECT)
  - FOREX: gate bidirectionnel (BUY/SELL) avec filtres SMA200 + Bollinger + RSI.

## Champs Forex AI ajoutes (V3)

Le validator FOREX renvoie et persiste aussi:

- `bb_status` -> `ai_bb_status`
- `rsi_status` -> `ai_rsi_status`

Ces champs sont disponibles dans DuckDB (`technical_signals`), la vue `v_ag2_fx_output`,
les payloads vectoriels, et la sortie Google Sheets (si le node de sync est utilise).

## Commandes utiles

Depuis `AG2-V3/`:

```bash
# Afficher/exporter le workflow canonique
python build_workflow.py > "AG2-V3 - Analyse technique.json"

# Resynchroniser les scripts nodes/* depuis le workflow canonique
python build_workflow.py --sync-nodes

# Alias pour resynchroniser les scripts
python build_workflow.py --write-files
```
