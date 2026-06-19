# HANDOFF Codex — Commit + PR : AG2-V3 utile à AG1 V4 (hybride)

**Pour Codex.** Objectif : committer proprement et ouvrir une PR pour le changement « AG2→AG1 hybride » du 2026-06-19, **déjà déployé en live** sur le VPS (source de vérité = workflows publiés n8n). Ce commit ne fait que **synchroniser le repo** avec le live + ajouter la doc.

## Contexte (1 paragraphe)
Audit (`docs/audits/20260619_ag2_v3_analyse_pertinence_efficience.md`) : l'étage LLM d'AG2-V3 ne servait pas AG1 (champs nuls ou déterministes, `ai_decision`/`ai_quality` jamais lus). Décision de Nicolas : **option A — rendre le LLM utile**, mode **hybride**, **WATCH éligible (pondéré)**. Déployé live le 2026-06-19 ~11:35 UTC sur AG2-V3 (`lUsgEdJODpYh5vt0dQdb2`) et AG1 V4 (`AG1V4CONSENSUS`), vérifié (run AG2 patché OK + replay AG1). Détails/rollback : `docs/operations/20260619_ag2_hybrid_deploy_notes.md`.

## Changements fonctionnels (déjà dans le working tree, diffs propres)
1. **AG2-V3 — nœud `Extract AI + Write`** (`06_extract_ai.py` + workflow JSON) : persiste `ai_rr_theoretical` depuis `ai_context.rr_theoretical` (était 100 % NULL) + `ALTER TABLE … ADD COLUMN IF NOT EXISTS ai_rr_theoretical DOUBLE`.
2. **AG1 V4 — `R8 — Data Prep`** : SELECT `ts.ai_decision, ts.ai_quality` + sortie `AI_Decision`/`AI_Quality` ; univers `asset_class … IN ('EQUITY','ETF','CRYPTO')` (retire 78 paires FX legacy).
3. **AG1 V4 — `Calcul Matrice & Briefing`** : hybride — REJECT exclu de « Entrer/Renforcer » (filtre dur, sorties intactes) ; APPROVE `+12+(q-5)*1.5`, WATCH `+4+(q-5)*1` sur `prob_score` ; SKIP/inconnu neutre ; raisons `AG2_LLM_*` ; ligne brief + `ai_decision`/`ai_quality` dans `opportunity_pack.rows`.

## Fichiers à committer (tous diffs propres, vérifiés)
```
AGENTS.md                                                                                   (MAJ état + refs)
docs/audits/20260619_ag2_v3_analyse_pertinence_efficience.md                                (NOUVEAU)
docs/operations/20260619_ag2_hybrid_deploy_notes.md                                         (NOUVEAU)
docs/operations/HANDOFF_codex_PR_ag2_hybrid_20260619.md                                     (NOUVEAU, ce fichier)
agents/trading-actions/AG2-V3/AG2-V3 - Analyse technique actions ETF crypto.json            (1 ligne)
agents/trading-actions/AG2-V3/nodes/06_extract_ai.py                                        (+6)
agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/AG1_workflow_v4_consensus.json   (1 ligne)
agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/R8_data_prep_matrix.code.py       (+5/-1)
agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/calcul_matrice_briefing.code.py   (+35/-1)
```

## ⚠️ À NE PAS committer
- Le **bruit CRLF** (~200 fichiers `M` non liés : AG4-V3, AG1-PF-V1, AG1-V3, AG2-FX, AG3-FX, AG4-FX, etc.) et le **backlog AG4_Spé** (handoff séparé `HANDOFF_codex_PR_ag4_spe_v3_20260618.md`). **Stager uniquement les 9 chemins ci-dessus.**
- `.codex-tmp/` et `.ssh/` (gitignorés).
- **2 fichiers vides parasites à SUPPRIMER** (doublons créés par erreur, n'ont pas pu être rm dans l'env précédent) :
  `agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/r8_data_prep_for_matrix.code.py`
  `agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/calcul_matrice_briefing.code.py`
  → `rm` ces deux fichiers (0 octet) avant le commit. Ne pas les `git add`.

## Procédure git (chemins avec espaces → quoter ; `git add` explicite par chemin)
```bash
cd <repo>
# 0. supprimer les 2 doublons vides
rm -f "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/r8_data_prep_for_matrix.code.py" \
      "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/agent_input/calcul_matrice_briefing.code.py"

# 1. branche dédiée (isole du backlog AG4 non commité, qui reste non stagé)
git checkout -b feat/ag2-hybrid-ag1-20260619

# 2. stager UNIQUEMENT les 9 chemins (add explicite : contourne toute anomalie de statut sur AGENTS.md)
git add -- AGENTS.md \
  "docs/audits/20260619_ag2_v3_analyse_pertinence_efficience.md" \
  "docs/operations/20260619_ag2_hybrid_deploy_notes.md" \
  "docs/operations/HANDOFF_codex_PR_ag2_hybrid_20260619.md" \
  "agents/trading-actions/AG2-V3/AG2-V3 - Analyse technique actions ETF crypto.json" \
  "agents/trading-actions/AG2-V3/nodes/06_extract_ai.py" \
  "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/AG1_workflow_v4_consensus.json" \
  "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/R8_data_prep_matrix.code.py" \
  "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/calcul_matrice_briefing.code.py"

# 3. CONTRÔLE OBLIGATOIRE : le diff stagé ne doit contenir QUE le changement logique
git diff --cached --stat
# Attendu : 9 fichiers, ~+? lignes ; les 2 JSON = 1 ligne modifiée chacun (code de nœud sur une ligne).
# Vérifier qu'aucun fichier hors-périmètre n'est stagé, et qu'aucun JSON n'est reformaté en entier (sinon = bruit CRLF → unstage et re-cibler le hunk).
git diff --cached -- "agents/trading-actions/AG2-V3/nodes/06_extract_ai.py" | grep -E "ai_rr_theoretical|ALTER"   # doit montrer le fix RR
git diff --cached -- "agents/trading-actions/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/calcul_matrice_briefing.code.py" | grep -E "llm_adj|AG2_LLM_APPROVE|REJECT"  # doit montrer l'hybride

# 4. commit
git commit -F - <<'MSG'
feat(ag2/ag1): rendre l'analyse technique AG2-V3 utile à AG1 V4 (hybride)

Suite à l'audit 20260619, le verdict LLM d'AG2 est désormais exploité par
le Portfolio Manager AG1 V4 :
- AG2 Extract: persiste ai_rr_theoretical (ai_context.rr_theoretical), était 100% NULL.
- AG1 R8: lit ai_decision/ai_quality ; univers whitelist EQUITY/ETF/CRYPTO (retire 78 FX legacy).
- AG1 Matrice: hybride — REJECT exclu de "Entrer/Renforcer" (filtre dur, sorties intactes) ;
  APPROVE/WATCH pondérés par ai_quality (WATCH éligible, poids réduit) ; SKIP/inconnu neutre.
- SELL conservé au scan (REJECT alimente le filtre dur).

Déployé live le 2026-06-19 (AG2 lUsgEdJODpYh5vt0dQdb2, AG1 AG1V4CONSENSUS) et vérifié
(run AG2 patché OK + replay AG1: enter 25->13, sorties inchangées). Ce commit synchronise
le repo avec le live + ajoute audit/notes de déploiement.

Refs: docs/audits/20260619_ag2_v3_analyse_pertinence_efficience.md
      docs/operations/20260619_ag2_hybrid_deploy_notes.md
MSG

# 5. push + PR
git push -u origin feat/ag2-hybrid-ag1-20260619
gh pr create --base main --head feat/ag2-hybrid-ag1-20260619 \
  --title "feat(ag2/ag1): analyse technique AG2-V3 utile à AG1 V4 (hybride)" \
  --body-file docs/operations/HANDOFF_codex_PR_ag2_hybrid_20260619.md
```
(adapter `--base` au tronc réel du repo si ce n'est pas `main`.)

## PR — corps (résumé pour relecteur)
- **Quoi** : AG2-V3 ne servait pas AG1 ; on câble le verdict LLM (hybride) + on corrige `ai_rr_theoretical` + on nettoie l'univers FX.
- **Risque** : changement dans la **préparation** (pré-PM) d'AG1 V4 qui trade en LIVE réel (`U25651155`). Les gardes d'exécution (Risk Manager, consensus 2/3, approbation Telegram) sont **inchangées**. Effet : moins de candidats à l'entrée (REJECT exclus), sorties non modifiées.
- **Validation** : run AG2 patché `success` (ai_rr_theoretical peuplé) ; replay AG1 sur données réelles (0 REJECT en « Entrer/Renforcer », FX=0, sorties stables). Détails dans les notes de déploiement.
- **Déjà live** : oui (cette PR synchronise le repo). Rollback documenté.

## Checklist relecteur
- [ ] `git diff --cached --stat` = 9 fichiers, aucun hors-périmètre, aucun JSON reformaté intégralement.
- [ ] Fix `ai_rr_theoretical` présent dans `06_extract_ai.py`.
- [ ] Hybride (`llm_adj`, filtre `REJECT`) présent dans `calcul_matrice_briefing.code.py`.
- [ ] Univers `IN ('EQUITY','ETF','CRYPTO')` dans `R8_data_prep_matrix.code.py`.
- [ ] 2 doublons vides `agent_input/*` supprimés, non commités.
- [ ] Backlog AG4 / bruit CRLF **non** inclus.
