# Fix échecs nocturnes AG3 (CHECKPOINT/fragmentation) + défrag DuckDB — 2026-07-05

**Symptôme :** 7 échecs AG3 sur 9 runs entre les nuits du 02→03/07 et du 04→05/07 (`AG3-V2 Fundamental Held+Core` : 2 err ; `Watchlist Nightly` : 5 err) + 1 crash `AG2 Held+Core` (03/07 13:00) + durées en dérive.

## Diagnostic (faits validés)

1. **Le métier AG3 réussissait** : `ag3_v2.run_log` = SUCCESS sur tous les runs, données complètes (snapshot/triage/consensus/metrics écrits). L'échec n8n survenait **après**, sur le node `AG3V2.09 - Finalize Run`.
2. Deux signatures d'échec : « Task execution timed out after **1200 seconds** » (HC) et « Node execution failed » via **déconnexion du task-runner** (WL) — corrélée à des **OOM kills** (`dmesg` : python 4,3 Go RSS tué le 05/07 03:34 ; `root-task-runners-3/4` `OOMKilled=true`).
3. Cause racine : dans `Finalize Run`, le helper `db_con` exécute **`CHECKPOINT` à la fermeture**. Sur une base massivement fragmentée, ce checkpoint (vacuum des row groups) dépasse 20 min et/ou consomme plusieurs Go de RAM.
4. Fragmentation mesurée (INSERT OR REPLACE sans compaction) : **ag3_v2 = 3,9 Go pour 48 Mo utiles (82×)** ; **ag2_v3 = 3,4 Go pour 13 Mo utiles (260×)**. La croissance quotidienne a fait franchir le seuil critique ces nuits-là — ça explique aussi la dérive de durée d'AG2 Held+Core notée dans l'audit du 02/07 (mêmes CHECKPOINT dans ses nodes).
5. Hors sujet vérifié : yfinance-api sain (200 en ~2 s, 2×500/j stables), pas de manque disque, pas de lock concurrent.

## Fixes déployés (2026-07-05, 12:20-12:35 UTC)

### a) Défragmentation immédiate (script `infra/maintenance/defrag_duckdb.py`)
- `ag2_v3.duckdb` : 3 380 Mo → **13 Mo** ; `ag3_v2.duckdb` : 3 931 Mo → **48 Mo**.
- Intégrité vérifiée : comptages **identiques** table par table vs `.old` (9/9 ag2, 6/6 ag3), PK restaurées, vues recréées et testées (`v_latest_triage` = 524 ; `v_ag2_fx_output` présente).
- Originaux conservés : `ag2_v3.duckdb.old` + `ag3_v2.duckdb.old` (~7,5 Go). **À supprimer après 24-48 h de runs sains** : `rm /local-files/duckdb/*.duckdb.old`.

### b) Deux bugs corrigés dans `defrag_duckdb.py` (repo, committé)
1. **Chunking `INSERT … LIMIT/OFFSET` sans ORDER BY** : non déterministe avec `preserve_insertion_order=false` → doublons/pertes silencieuses (échec PK `record_id` au 1ᵉʳ essai alors que la source était propre — vérifié : 214 445 record_id tous distincts). Remplacé par un `INSERT … SELECT` unique + **vérification du count** copié.
2. **Vues perdues au rebuild** : le script ne copiait que `duckdb_tables()` → `v_latest_*`, `news_analyzed` etc. auraient disparu au swap. Ajout de la recréation via `duckdb_views()`.
3. Bonus : option `--only <db...>` pour cibler des bases.
⚠️ Le 1ᵉʳ essai (chunking bugué) avait swappé ag2_v3 : intégrité **contrôlée et validée** a posteriori (aucun écart) ; ag3 relancée avec le script corrigé.

### c) `CHECKPOINT` retiré des `Finalize Run` AG3 HC + WL
- Workflows `AG3V2HELDCORE20260622` / `AG3V2WATCHNIGHT20260622` : le bloc `try: con.execute("CHECKPOINT")` du close est remplacé par un commentaire (le checkpoint implicite léger du `close()` suffit pour le WAL courant). Import + publish + restart n8n/runners/dashboard ; `active=1` et patch vérifiés dans la version publiée.
- Périmètre volontairement minimal : les CHECKPOINT d'AG2 (Held+Core/Watchlist/UHQ) sont **conservés** — redevenus rapides après défrag ; si la dérive de durée revient, appliquer le même retrait.
- Rollback : réimporter `/tmp/deploy_20260705/ag3hc.json` / `ag3wl.json` (exports pré-patch) + republier.

### d) Cron défrag hebdomadaire
```
30 7 * * 0 docker exec root-n8n-1 python3 /files/maintenance/defrag_duckdb.py --apply --only ag2_v3.duckdb ag3_v2.duckdb --tmp-dir /files/tmp_defrag >> /local-files/logs/duckdb_defrag_cron.log 2>&1
```
Dimanche 07:30 UTC = fenêtre sans écrivain (dernier writer : YF-ENRICH 04:15 Paris ; premier suivant : AG2 WL 22:00 Paris). Garde-fou intrinsèque : le script refuse si un `.wal` actif est présent. Script exécuté avec duckdb **1.4.3** (root-n8n-1) → format compatible avec tous les lecteurs.

## Validation attendue (J+1)
1. Cette nuit 22:00/23:00 UTC + 02:00 UTC : AG3 HC & WL en `success` n8n (déjà SUCCESS en run_log), durées ~15-20 min.
2. Plus d'OOM runner (`docker inspect root-task-runners-* → OOMKilled=false`, dmesg calme).
3. AG2 Held+Core lundi : durée revenue ≈ 23 min.
4. Dimanche prochain 07:30 UTC : log défrag hebdo OK, tailles stables.
5. Après 48 h : supprimer les `.old`.
