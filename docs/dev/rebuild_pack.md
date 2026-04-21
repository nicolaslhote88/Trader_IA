# Reconstruire le pack AG1 (`rebuild_pack.py`)

Depuis avril 2026, le pack `agents/AG1-V3-Portfolio manager/` est **entièrement centralisé dans `workflow/`**. Le parent ne contient plus que ce script, un README pointeur et `export_to_github.ps1`.

## Arborescence cible

```
agents/AG1-V3-Portfolio manager/
├── README.md                    # pointeur vers workflow/
├── rebuild_pack.py              # ce script
├── export_to_github.ps1         # helper commit/push
└── workflow/
    ├── AG1_workflow_template_v3.json   # SOURCE DE VÉRITÉ
    ├── README.md
    ├── generate_model_variants.py
    ├── variants/
    │   ├── AG1_workflow_v3__chatgpt52.json
    │   ├── AG1_workflow_v3__grok41_reasoning.json
    │   └── AG1_workflow_v3__gemini30_pro.json
    ├── nodes/
    │   ├── NODE_SUMMARY.tsv
    │   ├── agent_input/
    │   ├── post_agent/
    │   │   └── duckdb_writer.py    # préservé (dep externe, pas extraite du template)
    │   └── pre_agent/
    ├── sql/
    │   ├── README.md
    │   └── portfolio_ledger_schema_v2.sql
    └── docs/
        └── POST_AGENT_DUCKDB_LEDGER.md   # placeholder si absent
```

> **Attention — ne pas confondre avec l'arborescence VPS.** Sur le VPS, le pack est monté sous `/opt/trader-ia/AG1-V3-EXPORT/...` avec sa propre structure. Le nœud 9 (`09_upsert_run_bundle_duckdb.code.py`) a une liste `STATIC_WRITER_PATHS` qui couvre plusieurs layouts historiques (plat + `workflow/`) ; la restructure GitHub reste donc compatible côté runtime.

## Source de vérité

- `workflow/AG1_workflow_template_v3.json` (éditer dans n8n → exporter → commit).

## Quand exécuter `rebuild_pack.py`

- Après toute modification du template `AG1_workflow_template_v3.json`.
- Après toute modification de `workflow/nodes/post_agent/duckdb_writer.py`.
- Après toute modification de `workflow/sql/portfolio_ledger_schema_v2.sql`.
- Avant tout commit qui touche AG1.

## Usage

```bash
cd "agents/AG1-V3-Portfolio manager"
python3 rebuild_pack.py
```

Le script (idempotent) :

1. Charge `workflow/AG1_workflow_template_v3.json`.
2. Ré-écrit `workflow/nodes/<category>/*.node.json` + `.code.{js,py}` pour les nœuds listés dans `EXPORT_SPECS`.
3. Préserve `workflow/nodes/post_agent/duckdb_writer.py` même si son nœud n'est pas dans `EXPORT_SPECS`.
4. Réécrit `workflow/nodes/NODE_SUMMARY.tsv`.
5. Vérifie la présence de `workflow/sql/portfolio_ledger_schema_v2.sql` et régénère le README du dossier.
6. Écrit un placeholder `workflow/docs/POST_AGENT_DUCKDB_LEDGER.md` si absent.
7. Réécrit `README.md` (pointeur).

Les variants par modèle (`workflow/variants/AG1_workflow_v3__*.json`) sont régénérés séparément via `workflow/generate_model_variants.py`.

## Helpers

- `export_to_github.ps1` : commit + push ciblé uniquement sur `agents/AG1-V3-Portfolio manager/`.

## Notes

- Si un nœud listé dans `EXPORT_SPECS` est absent du template, le script affiche un warning et continue (pas d'erreur fatale).
- `duckdb_writer.py` est préservé via un round-trip bytes-exact avant suppression du dossier `post_agent/`, donc **ses éditions manuelles sont conservées** au travers des réexécutions.
