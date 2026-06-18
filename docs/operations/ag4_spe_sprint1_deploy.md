# AG4_Spé-V2 — Sprint 1 : déploiement (go/no-go)

**Date** : 2026-06-17 · **Statut** : code implémenté + validé en shadow (lecture seule). **Rien n'est encore publié sur le n8n live.**
**Réf** : `docs/audits/20260617_ag4_spe_v2_analysis.md` (diagnostic), `…_remediation_plan.md` (plan).

⚠️ **AG1 V4 est en trading LIVE réel (`U25651155`).** D1 modifie la donnée news vue par le PM → influence des ordres réels. Déploiement = action de publication : à valider explicitement.

---

## Ce qui a été changé (sources, dans le repo)

| ID | Fichier | Changement | Validé |
|---|---|---|---|
| **D1** | `…/AG1-V4-Consensus…/workflow/nodes/pre_agent/R8_data_prep_matrix.code.py` | Fenêtre news : `published_at` n'est utilisé que s'il est dans `[now-730j ; now+7j]`, sinon repli sur `first_seen_at`. 2 requêtes (ag4_spe + fallback ag4_v3). | Shadow : `last_news_date` max 2031-12-25 → **2026-06-17** ; `count_7d` EURUSD 22→0. `ast.parse` OK. |
| **B1** | `…/AG4-SPE-V2/nodes/07_parse_article.js` | `normalizeDate()` réécrite : ISO d'abord, fallback FR ancré (année 4 chiffres), garde-fou plausibilité → `null` si aberrant. | 8 cas Node OK (futur→null, « 2031 » texte→null, FR réelle conservée). |
| **A1** | `…/AG4-SPE-V2/nodes/02_start_run.py` | Auto-réconciliation : tout `RUNNING` > 1 h passé en `STALE` au démarrage de chaque run. | Shadow : 47 zombies ciblés, tous > 1 h. |
| **B2** | `scripts/ag4_spe_backfill_published_at.py` (nouveau) | Neutralise (`NULL`) les `published_at` hors plage. Dry-run par défaut. | Dry-run : 12 922/19 518 (66,2 %) à neutraliser ; `first_seen_at` rempli à 100 % (repli sûr). |

Artefacts régénérés : `AG4-SPE-V2/AG4-SPE-V2-workflow.json` (contient B1+A1).

---

## Procédure de déploiement (à exécuter après go)

### Pré-requis
- Session IBKR OK (sinon aucun ordre ne part, mais n'empêche pas le déploiement news).
- Backup DuckDB avant B2.

### Étape 1 — AG4_Spé-V2 (B1 + A1) — faible risque
```bash
# (build déjà fait) importer le workflow régénéré dans n8n puis redémarrer pour réenregistrer le cron
#   AG4-SPE-V2/AG4-SPE-V2-workflow.json  -> import n8n (remplacer la version active)
ssh vps "docker restart root-n8n-1"   # ~60 s
# Vérif : prochain run (09/12/15) -> run_log : 47 RUNNING passent en STALE, nouvelles dates plausibles
```

### Étape 2 — B2 backfill (optionnel, hygiène ; D1 protège déjà AG1)
```bash
ssh vps "cp /local-files/duckdb/ag4_spe_v2.duckdb /local-files/duckdb/ag4_spe_v2.duckdb.bak_20260617"
# hors run AG4_Spé (éviter lock) :
docker cp scripts/ag4_spe_backfill_published_at.py yf-enrichment:/tmp/
ssh vps "docker exec yf-enrichment python3 /tmp/ag4_spe_backfill_published_at.py"          # dry-run
ssh vps "docker exec yf-enrichment python3 /tmp/ag4_spe_backfill_published_at.py --apply"  # apply
```

### Étape 3 — AG1 V4 (D1) — **trading LIVE, prudence maximale**
```bash
cd "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow"
python3 build_v4_workflow.py                 # régénère le JSON avec D1
# DIFF du JSON régénéré vs export n8n live -> confirmer que la SEULE diff fonctionnelle = R8/D1
# puis import n8n + docker restart root-n8n-1
# Replay : exécuter AG1 V4 en test, vérifier le brief PM (Symbol_News_Impact_7d, last_news_date <= aujourd'hui)
```

---

## Ordre recommandé
Étape 1 (sûre, gain immédiat sur la fraîcheur + zombies) → Étape 3 (D1, après replay) → Étape 2 (hygiène, quand pratique).

> Note : une fois B1 déployé, les **nouvelles** news auront des dates correctes ; B2 ne traite que l'historique. Et D1 rend AG1 robuste **même sans** B1/B2.
