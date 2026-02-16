# AG2-V2 - Workflow Final

## Version unique

Le workflow officiel est:

- `ag2-v2/AG2-V2-workflow.final-loop-vector-test.json`

Il n'y a plus de versions miroir.

## Scripts nodes synchronises

Les fichiers `ag2-v2/nodes/*` sont sync depuis le workflow final via `build_workflow.py`.

## Commandes utiles

Depuis `ag2-v2/`:

```bash
# Afficher/exporter le workflow canonique
python build_workflow.py > AG2-V2-workflow.final-loop-vector-test.json

# Resynchroniser les scripts nodes/* depuis le workflow canonique
python build_workflow.py --sync-nodes

# Alias pour resynchroniser les scripts
python build_workflow.py --write-files
```
