# Reconstruire le pack AG1 (`rebuild_pack.py`)

Le pack `AG1-V3-Portfolio manager/` est **partiellement généré** à partir du template de workflow. Le script `rebuild_pack.py` assure la cohérence entre :

- **Source humaine** : `AG1-V3-Portfolio manager/workflow/AG1_workflow_template_v3.json` + `workflow/nodes/*` + `workflow/docs/`
- **Exports dérivés** : `AG1-V3-Portfolio manager/nodes/*`, `AG1-V3-Portfolio manager/docs/POST_AGENT_DUCKDB_LEDGER.md`, variants par modèle (`workflow/variants/AG1_workflow_v3__*.json`)

## Quand l'exécuter

- Après toute modification du template `AG1_workflow_template_v3.json`.
- Après toute modification de `duckdb_writer.py` ou de `portfolio_ledger_schema_v2.sql` (le script mirrore ces fichiers entre `nodes/` et `workflow/nodes/`).
- Avant tout commit qui touche AG1.

## Usage

```bash
cd "AG1-V3-Portfolio manager"
python3 rebuild_pack.py
```

Le script :

1. Extrait les nœuds critiques du template (`pre_agent/`, `agent_input/`, `post_agent/`).
2. Produit les variants par modèle (GPT-5.2, Grok-4.1, Gemini-3) dans `workflow/variants/`.
3. Mirrore `workflow/nodes/` vers `nodes/` à la racine (avec préservation de `duckdb_writer.py` si déjà présent).
4. Écrit un `NODE_SUMMARY.tsv` pour lister les nœuds extraits.
5. Crée des placeholders docs s'ils n'existent pas (`docs/POST_AGENT_DUCKDB_LEDGER.md` à la racine AG1).

## Helpers

- `export_to_github.ps1` (généré par `rebuild_pack.py`) : helper PowerShell pour commit + push ciblé uniquement sur `AG1-V3-Portfolio manager/`.

## Notes

- Le script est **idempotent** — le relancer plusieurs fois produit le même résultat.
- Ne pas supprimer `workflow/AG1_workflow_template_v3.json` : c'est la source de vérité.
- Le script **préserve** un `duckdb_writer.py` modifié à la main s'il existe déjà dans `nodes/post_agent/`.
