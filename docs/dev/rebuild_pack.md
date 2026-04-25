# Reconstruire le pack AG1 (`rebuild_pack.py`)

Depuis avril 2026, le pack `agents/trading-actions/AG1-V3-Portfolio manager/` est **entiÃ¨rement centralisÃ© dans `workflow/`**. Le parent ne contient plus que ce script, un README pointeur et `export_to_github.ps1`.

## Arborescence cible

```
agents/trading-actions/AG1-V3-Portfolio manager/
â”œâ”€â”€ README.md                    # pointeur vers workflow/
â”œâ”€â”€ rebuild_pack.py              # ce script
â”œâ”€â”€ export_to_github.ps1         # helper commit/push
â””â”€â”€ workflow/
    â”œâ”€â”€ AG1_workflow_template_v3.json   # SOURCE DE VÃ‰RITÃ‰
    â”œâ”€â”€ README.md
    â”œâ”€â”€ generate_model_variants.py
    â”œâ”€â”€ variants/
    â”‚   â”œâ”€â”€ AG1_workflow_v3__chatgpt52.json
    â”‚   â”œâ”€â”€ AG1_workflow_v3__grok41_reasoning.json
    â”‚   â””â”€â”€ AG1_workflow_v3__gemini30_pro.json
    â”œâ”€â”€ nodes/
    â”‚   â”œâ”€â”€ NODE_SUMMARY.tsv
    â”‚   â”œâ”€â”€ agent_input/
    â”‚   â”œâ”€â”€ post_agent/
    â”‚   â”‚   â””â”€â”€ duckdb_writer.py    # prÃ©servÃ© (dep externe, pas extraite du template)
    â”‚   â””â”€â”€ pre_agent/
    â”œâ”€â”€ sql/
    â”‚   â”œâ”€â”€ README.md
    â”‚   â””â”€â”€ portfolio_ledger_schema_v2.sql
    â””â”€â”€ docs/
        â””â”€â”€ POST_AGENT_DUCKDB_LEDGER.md   # placeholder si absent
```

> **Attention â€” ne pas confondre avec l'arborescence VPS.** Sur le VPS, le pack est montÃ© sous `/opt/trader-ia/AG1-V3-EXPORT/...` avec sa propre structure. Le nÅ“ud 9 (`09_upsert_run_bundle_duckdb.code.py`) a une liste `STATIC_WRITER_PATHS` qui couvre plusieurs layouts historiques (plat + `workflow/`) ; la restructure GitHub reste donc compatible cÃ´tÃ© runtime.

## Source de vÃ©ritÃ©

- `workflow/AG1_workflow_template_v3.json` (Ã©diter dans n8n â†’ exporter â†’ commit).

## Quand exÃ©cuter `rebuild_pack.py`

- AprÃ¨s toute modification du template `AG1_workflow_template_v3.json`.
- AprÃ¨s toute modification de `workflow/nodes/post_agent/duckdb_writer.py`.
- AprÃ¨s toute modification de `workflow/sql/portfolio_ledger_schema_v2.sql`.
- Avant tout commit qui touche AG1.

## Usage

```bash
cd "agents/trading-actions/AG1-V3-Portfolio manager"
python3 rebuild_pack.py
```

Le script (idempotent) :

1. Charge `workflow/AG1_workflow_template_v3.json`.
2. RÃ©-Ã©crit `workflow/nodes/<category>/*.node.json` + `.code.{js,py}` pour les nÅ“uds listÃ©s dans `EXPORT_SPECS`.
3. PrÃ©serve `workflow/nodes/post_agent/duckdb_writer.py` mÃªme si son nÅ“ud n'est pas dans `EXPORT_SPECS`.
4. RÃ©Ã©crit `workflow/nodes/NODE_SUMMARY.tsv`.
5. VÃ©rifie la prÃ©sence de `workflow/sql/portfolio_ledger_schema_v2.sql` et rÃ©gÃ©nÃ¨re le README du dossier.
6. Ã‰crit un placeholder `workflow/docs/POST_AGENT_DUCKDB_LEDGER.md` si absent.
7. RÃ©Ã©crit `README.md` (pointeur).

Les variants par modÃ¨le (`workflow/variants/AG1_workflow_v3__*.json`) sont rÃ©gÃ©nÃ©rÃ©s sÃ©parÃ©ment via `workflow/generate_model_variants.py`.

## Helpers

- `export_to_github.ps1` : commit + push ciblÃ© uniquement sur `agents/trading-actions/AG1-V3-Portfolio manager/`.

## Notes

- Si un nÅ“ud listÃ© dans `EXPORT_SPECS` est absent du template, le script affiche un warning et continue (pas d'erreur fatale).
- `duckdb_writer.py` est prÃ©servÃ© via un round-trip bytes-exact avant suppression du dossier `post_agent/`, donc **ses Ã©ditions manuelles sont conservÃ©es** au travers des rÃ©exÃ©cutions.
