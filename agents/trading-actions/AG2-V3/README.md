# AG2-V3 - Analyse technique actions

## Workflow actif en n8n

Le workflow officiel de la partie trading actions est :

- `AG2-V3/AG2-V3 - Analyse technique actions ETF crypto.json` - univers actions / ETF / crypto.

## Agent technique

Le pipeline charge l'univers, recupere les donnees Yahoo Finance H1/D1, calcule les indicateurs, applique le prefiltre technique puis valide les candidats via le prompt ACTIONS/ETF.

Le schema DuckDB conserve les sorties utiles a AG1 :

- `universe`
- `technical_signals`
- `v_latest_signals`
- `v_ag1_summary`
- `ai_dedup_cache`
- `run_log`
- `batch_state`

## Scripts noeuds

Les fichiers dans `AG2-V3/nodes/` refletent le code embarque dans le workflow. Pour resynchroniser manuellement le contenu d'un noeud code : copier depuis n8n, coller dans le fichier correspondant, puis committer.
