# Déploiement — AG2-V3 utile à AG1 V4 (option « LLM utile », hybride)

**Date** : 2026-06-19 ~11:35 UTC · **Décision** : Nicolas → option **(A) Rendre le LLM utile**, mode **hybride**, **WATCH éligible (pondéré)**.
Fait suite à l'audit `docs/audits/20260619_ag2_v3_analyse_pertinence_efficience.md`.

## Ce qui a été déployé (2 workflows live, version publiée mise à jour)

### AG2-V3 — `lUsgEdJODpYh5vt0dQdb2` (nœud « Extract AI + Write »)
- **Fix `ai_rr_theoretical`** : la colonne était 100 % NULL (calculée dans Snapshot Context mais jamais persistée). On mappe désormais `ai_context.rr_theoretical` → `technical_signals.ai_rr_theoretical` (+ `ALTER TABLE … ADD COLUMN IF NOT EXISTS`). Peuplée pour les lignes BUY passées au LLM.
- **Court-circuit SELL : NON retenu.** Sous l'option A, les appels `SELL → REJECT` ne sont plus du gaspillage : ce sont eux qui alimentent le filtre dur d'entrée d'AG1 (blocage des titres en repli). Coût négligeable (~0,3 $/mois). Le scan SELL est conservé.

### AG1 V4 — `AG1V4CONSENSUS` (nœuds « R8 — Data Prep » et « Calcul Matrice & Briefing »)
- **R8** : ajout de `ts.ai_decision`, `ts.ai_quality` au SELECT + exposition `AI_Decision`/`AI_Quality` dans la matrice. Univers : `asset_class <> 'CURRENCY'` remplacé par whitelist `IN ('EQUITY','ETF','CRYPTO')` → retire les **78 paires FX legacy** (Forex désactivé, dernier scan 24/04) qui polluaient le snapshot.
- **Matrice (hybride)** :
  - `REJECT` → **exclu d'office de « Entrer / Renforcer »** (filtre dur, `llm_reject_block`). **N'affecte PAS** « Reduire / Sortir » (gestion de sortie inchangée). Pas de pénalité sur `prob_score` (évite de polluer la logique de sortie/grade).
  - `APPROVE` → bonus `prob_score = +12 + (quality-5)*1.5`.
  - `WATCH` → bonus réduit `+4 + (quality-5)*1.0` (éligible mais pondéré sous APPROVE).
  - `SKIP`/inconnu → neutre (pas de blocage ni bonus). NB : la plupart des symboles ont `ai_decision='SKIP'` (pas de trigger H1 récent) → comportement inchangé pour eux.
  - Raisons tracées : `AG2_LLM_APPROVE` / `AG2_LLM_WATCH` / `AG2_LLM_REJECT`. Ligne de brief « AG2 LLM: APPROVE=… WATCH=… REJECT=… ». `ai_decision`/`ai_quality` exposés dans `opportunity_pack.rows`.

## Validation (shadow/replay sur données réelles, read_only)
- Syntaxe OK (wrap + compile) sur les 4 nœuds. Structure JSON intacte (id/active/nodes/connexions préservés).
- Replay R8+Matrice sur bases live : **baseline** enter=25 / watch=183 / exit=177 → **patché** enter=13 / watch=195 / exit=177. Les ~12 candidats `REJECT` sortent de l'entrée (→ Surveiller), **sorties inchangées** (filtre d'entrée pur), EV/pWin moyens stables. APPROVE (ex. CCN.PA q=7) conservé et boosté.

## Procédure de déploiement utilisée
`docker cp` → `chmod 644` (le conteneur tourne en user `node`) → `n8n import:workflow --input=` (désactive le wf) → `n8n update:workflow --id= --active=true` → `n8n publish:workflow --id=` → `docker restart root-n8n-1 root-task-runners-3/4/5`. Vérifié `versionId==activeVersionId` et marqueurs présents dans l'export live.

## Pièges rencontrés / à retenir
- **DuckDB mono-writer** : lire `ag2_v3.duckdb` en read_only pendant le run AG2 (cron :05, ~20 min) provoque un conflit de verrou et **fait échouer le run AG2** (runs 10:05 et 11:05 du 19/06 en erreur = lecture concurrente de l'analyse). Ne jamais requêter cette base pendant la fenêtre :05→:30.
- `n8n execute --id` (CLI) **inutilisable** ici : « Task Broker's port 5679 already in use » (archi task-runners externes). Pour tester un run : attendre le cron ou déclencher via l'UI.
- L'apostrophe dans un commentaire Python cassait la chaîne du patcher — commentaires ASCII sans apostrophe.

## Backups
- Live avant patch : `.codex-tmp/backups/ag2_v3_live_20260619_112005.json`, `.codex-tmp/backups/ag1v4_live_20260619_112005.json`.
- Patchés déployés : `.codex-tmp/ag2_v3_hybrid_patched_20260619_113252.json`, `.codex-tmp/ag1v4_hybrid_patched_20260619_113252.json`.

## Vérification post-déploiement
- Tâche planifiée one-shot **14:35 Paris / 12:35 UTC** : confirme le run AG2 12:05 `success` + taux `ai_rr_theoretical` non-NULL + absence d'erreur runner.
- **AG1 V4 hybride s'applique au run de 16:00 UTC (semaine).** ⚠️ Vérifier la session IBKR avant (broker `/health` montrait `authenticated=false` au moment du déploiement — relogin manuel via tunnel 5000 si toujours KO, sinon `auto_reauth`).

## Rollback
Réimporter le backup live correspondant (`n8n import:workflow --input=…live_20260619_112005.json`), `update:workflow --active=true`, `publish:workflow`, restart. (Les fichiers de backup sont des exports mono-workflow déjà au bon format.)
