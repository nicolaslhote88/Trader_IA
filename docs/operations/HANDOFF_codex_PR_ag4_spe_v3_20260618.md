# Brief handoff Codex — Push GitHub + PR (AG4_Spé : audit, Sprint 1/2, V3 IBKR)

**Date** : 2026-06-18 · **Pour** : Codex (Windows, accès git) · **De** : session Claude/Cowork (analyse + déploiement VPS)
**Branche actuelle** : `claude/ag4-v3-dualbranch-calib-20260617`

---

## 0. ⚠️ AVERTISSEMENTS AVANT TOUT `git`

1. **NE PAS faire `git add -A` / `git add .`** Le working tree montre **~235 fichiers « modifiés »**, mais la quasi-totalité est du **bruit de fins de ligne (CRLF↔LF)** : un fichier non touché comme `agents/trading-forex/AG8-FX-Rates/nodes/04_log_run.py` ressort en `35 add / 35 del` (= fichier entier réécrit, 0 changement de contenu). Committer tout ça = PR illisible mêlant du bruit + du travail antérieur non commité.
2. **Stager UNIQUEMENT la liste §2** (les vrais changements de cette session). Vérifier chaque diff (`git diff --word-diff` ou ignorer les EOL : `git diff --ignore-all-space`) pour confirmer qu'il s'agit de contenu, pas d'EOL.
3. **Régler les fins de ligne** : envisager un `.gitattributes` (`* text=auto eol=lf`) + renormaliser, OU committer avec `core.autocrlf` cohérent, pour stopper ce bruit à l'avenir. À décider avec Nicolas (hors scope strict de cette PR, mais c'est la cause du bruit).
4. **Travail antérieur non commité sur la branche** (AG4-V3 dual-branch, FX, macro-data-api…) : il préexiste à cette session. Décider avec Nicolas s'il part dans cette PR ou une autre. Cette PR-ci ne concerne **que** l'audit AG4_Spé + Sprint 1/2 + V3 IBKR.
5. **`/opt/trader-ia` n'est PAS un clone git** : la source de build du broker y a été éditée en miroir du repo. Le repo est la référence à committer (sinon perdu au prochain rebuild d'image).

---

## 1. Ce qui a été fait (résumé fonctionnel)

Audit complet du pilier news single-stock **AG4_Spé-V2** (Boursorama → `ag4_spe_v2.duckdb`), puis remédiation **déployée et vérifiée en LIVE sur le VPS** :

**Sprint 1 (qualité/fiabilité, déployé)**
- **B1** — `normalizeDate()` réécrite (S16) : fin du bug de dates (années 2016→2031). ISO d'abord + garde-fou plausibilité.
- **A1** — auto-réconciliation des runs « zombies » RUNNING>1h → STALE (S02).
- **D1** — AG1 V4 (node R8) : fenêtre news sur `published_at` validé sinon `first_seen_at` (corrige `Symbol_News_Impact_7d`). **Touche AG1 V4 = trading live** (déployé après shadow).
- **Nettoyage base** (one-shot, déjà appliqué en prod) : 19 574 → 3 310 lignes (suppression placeholders, dates corrigées). Scripts fournis.

**Sprint 2 (couverture/observabilité, déployé)**
- **C1/C3** — `01_build_symbol_queue.py` : exclusion FX (`SKIP_FX`, univers 463→385 actions/ETF) + rotation **priorisée portefeuille** + retry×3 sur S04/S14 (502/503 Boursorama).
- **B3** — vue DuckDB `news_analyzed` (summary ∧ is_relevant).
- **A3** — nouveau workflow n8n d'alerte santé (Telegram) si pipeline stale/zombies/dates KO.

**V3 — news IBKR « portfolio » (déployé, opérationnel)**
- **Broker** : nouvelle route lecture seule `GET /news/portfolio` (+ `cpapi_client.get_portfolio_news()`), wrappe `/iserver/news/portfolio` (Reuters/Dow Jones/Trading Central… des valeurs **détenues**). L'endpoint par-contrat IBKR (`/iserver/news?conid=`) est indisponible sur ce build (503) → V3 = held-only, en **complément** de Boursorama.
- **Workflow** `AG4_Spé-IBKR-V1 — Portfolio News` : 1 appel/run → filtre positions AG1 (matching base `.PA`) → **même chaîne LLM que V2** (analyse) → écriture `news_history` (`source='ibkr'`, `provider`, `news_article_id`, `ibkr_sentiment`). Validé : run réel = 40 lignes sur DSY/PEUG/VIRP/LR.PA, analyse LLM OK, 0 date erronée.

Détails complets : `docs/audits/20260617_ag4_spe_v2_analysis.md`, `…_remediation_plan.md`, `docs/specs/ag4_spe_v3_ibkr_news.md`, `docs/operations/ag4_spe_sprint1_deploy.md`.

---

## 2. Fichiers à inclure dans la PR (et RIEN d'autre)

**Broker (image baked — committer impérativement)**
- `services/ibkr-broker/app.py` — route `/news/portfolio`
- `services/ibkr-broker/cpapi_client.py` — `get_portfolio_news()`

**Nodes AG4_Spé-V2 (sources)**
- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/01_build_symbol_queue.py` — C1/C3
- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/02_start_run.py` — A1
- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/07_parse_article.js` — B1
- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/nodes/A3_health_check.py` — A3 (source du Code node)

**AG1 V4 (D1)**
- `agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/R8_data_prep_matrix.code.py`

**Workflows n8n (JSON importables) — voir §3 pour la canonicalisation**
- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/AG4-SPE-V2-workflow.json` (B1+A1 ; **à régénérer**, cf §3)
- `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/AG4-SPE-IBKR-V1-workflow.json` (V3, nouveau)
- workflow A3 (alerte santé) : à exporter de n8n (cf §3)

**Scripts maintenance (nouveaux)**
- `outils/scripts/ag4_spe_backfill_published_at.py`
- `outils/scripts/ag4_spe_cleanup_history.py`

**Docs (nouveaux)**
- `docs/audits/20260617_ag4_spe_v2_analysis.md`
- `docs/audits/20260617_ag4_spe_v2_remediation_plan.md`
- `docs/operations/ag4_spe_sprint1_deploy.md`
- `docs/specs/ag4_spe_v3_ibkr_news.md`
- `docs/operations/HANDOFF_codex_PR_ag4_spe_v3_20260618.md` (ce fichier)

---

## 3. Points de réconciliation à trancher (Codex × Nicolas)

1. **Doublons de noms de workflow** dans `AG4-SPE-V2/` : il existe `AG4-SPE-IBKR-V1-portfolio-news.workflow.json` et `AG4-SPE-health-alert.workflow.json` (créés côté Codex) **et** `AG4-SPE-IBKR-V1-workflow.json` (créé cette session). **Garder une seule version canonique par workflow.** La **référence = ce qui tourne en prod sur le VPS** (voir §4). Recommandation : **exporter depuis n8n** (`n8n export:workflow --id=…`) les 3 workflows live et committer ces exports, pour que repo == prod.
   - IDs n8n live : AG4_Spé-V2 = `H0cfY1coMx8dvMuXScMc_` · AG4_Spé-IBKR-V1 = `hSqxVSb8YAO9Nc6A` · AG4_Spé Health Alert = `uXW5M4vc1imTExB4` · AG1 V4 = `AG1V4CONSENSUS`.
2. **`AG4-SPE-V2-workflow.json`** : régénéré pour B1+A1 mais C1/C3 (nouveau `01_build_symbol_queue.py`) a été déployé par patch live. Régénérer proprement : `python AG4-SPE-V2/build_workflow.py` (embarque les nodes à jour) **ou** exporter depuis n8n.
3. **A3** : seul `A3_health_check.py` (le code du Code node) est dans le repo ; le workflow complet doit être exporté de n8n.

---

## 4. État de déploiement (déjà LIVE — la PR ne déploie pas, elle versionne)

Tout est **déjà déployé et vérifié** sur le VPS (n8n + broker baked rebuild). La PR sert à **versionner** ce qui tourne. Détail :
- Broker rebuild OK (`/health` authenticated, `/news/portfolio` renvoie ~48 items session active).
- Workflows AG4_Spé-V2 / IBKR-V1 / Health Alert / AG1 V4 : actifs, versions publiées.
- Base nettoyée + vue `news_analyzed` recréée.

⚠️ **Garde-fous** : ne pas toucher les variables d'exécution IBKR (`IBKR_DRY_RUN`, `AG1_ACTIONS_LIVE_ORDERS_ENABLED`, etc.). AG1 V4 = trading réel.

---

## 5. PR proposée

**Titre** : `AG4_Spé: audit + Sprint 1/2 fixes (dates/zombies/rotation/D1) + V3 news IBKR portfolio`

**Description (corps)** :
> Audit complet du pilier news single-stock AG4_Spé et remédiation (déployée et vérifiée en prod) :
> - **Qualité** : fix bug de dates `normalizeDate` (B1), nettoyage base 19.5k→3.3k.
> - **Fiabilité** : réconciliation runs zombies (A1), retry HTTP Boursorama, alerte santé Telegram (A3).
> - **Couverture/efficience** : univers aligné AG1 (exclusion FX) + rotation priorisée portefeuille (C1/C3).
> - **Consommation AG1** : R8 fenêtre news robuste aux dates (D1) — *touche AG1 V4 live, validé shadow*.
> - **V3** : nouvelle source **news IBKR portfolio** (broker `/news/portfolio` + workflow dédié) en complément de Boursorama, analysée par la même chaîne LLM.
>
> ⚠️ Déjà déployé sur le VPS (n8n + broker baked). Cette PR versionne le code. Voir `docs/operations/HANDOFF_codex_PR_ag4_spe_v3_20260618.md`.

**Découpage de commits suggéré** (pour relecture) :
1. `broker: add read-only /news/portfolio endpoint` (services/ibkr-broker/*)
2. `ag4_spe: fix dates (B1) + zombie reconcile (A1) + queue FX-exclusion & portfolio priority (C1/C3) + health check (A3)`
3. `ag1_v4: R8 news recency fallback to first_seen_at (D1)`
4. `ag4_spe: V3 IBKR portfolio-news workflow + maintenance scripts`
5. `docs: AG4_Spé audit, remediation plan, V3 spec, deploy notes`

**Test plan (déjà exécuté en prod, à mentionner)** :
- Run réel AG4_Spé-V2 post-fix : dates plausibles, zombies réconciliés, batch sans FX + positions en tête.
- Run réel AG4_Spé-IBKR-V1 : 40 lignes `source='ibkr'` (Reuters/DJ), analyse LLM, 0 date future.
- `news_analyzed` : boursorama + ibkr.

---

## 6. Rappel — ce que la PR NE doit PAS embarquer
- Le bruit CRLF sur ~200 fichiers non liés.
- Les changements préexistants AG4-V3/FX/macro non commités (sauf décision explicite de les inclure).
- Tout fichier sous `.codex-tmp/` (backups/scratch).

---

## 7. AJOUTS (mise à jour 2026-06-18 fin de session) — D2 + fix approbation + AGENTS

Travaux supplémentaires **déployés et vérifiés en prod**, à inclure dans la même PR.

### Fichiers additionnels à committer
**D2 — exploitation news par AG1 V4 (node 20K)**
- `agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/pre_agent/20K_news_digest.code.py` (source du node)
- workflow AG1 V4 : exporter de n8n (`id=AG1V4CONSENSUS`) pour capter le node `20K — News Digest (Pack+Held)` + le rewire `Calcul Matrice → 20K → Merge7[1]`. (NE PAS committer un export brut qui réverserait D1 — vérifier que `RECENCY_SPE` ET `20K` sont présents.)
- `docs/specs/ag1_v4_d2_news_digest.md`

**Fix approbation ordres (broker)**
- `services/ibkr-broker/app.py` (déjà dans la liste §2 ; contient maintenant AUSSI le fix `approvals_approve` re-soumission fraîche + helper `_approval_decision_error`). Source host miroir : `/opt/trader-ia/services/ibkr-broker/app.py`.

**Doc projet**
- `AGENTS.md` (réécrit : pipeline news V2/V3/D2, fix approbation, pièges dev, issues MAJ). Backup `.codex-tmp/AGENTS.md.bak_20260618`.

### Vérifs prod (déjà passées)
- Run consensus 20:04 UTC : `opportunity_pack` enrichi (`news_legend`, `held_news`, news par row), +~1,4k tokens. D1 toujours présent.
- Approbation : approve → 200 (plus de 500), double-tap → idempotent. (Fill réel à revalider EN SÉANCE — hors séance l'ordre finit FAILED proprement.)

### Commits suggérés (compléments)
6. `ag1_v4: D2 news digest node (20K) enriching opportunity_pack (pack+held, 14d, compact)`
7. `broker: fix order-approval (fresh re-submit on approve, idempotent double-tap, no 500)`
8. `docs: update AGENTS.md (news pipeline V2/V3/D2, approval fix, dev pitfalls)`

### Issues résolues cette session (à retirer des bugs ouverts si listées ailleurs)
- ✅ Bug dates AG4_Spé (2016→2031) — corrigé (B1) + base nettoyée.
- ✅ Runs zombies AG4_Spé — auto-réconciliation (A1).
- ✅ Approche « approve → re-price → fill » : exercée et **corrigée** (plus de 500 ; échec propre hors séance).

---

## 8. AJOUT CODEX — Réconciliation durable IBKR → dashboard AG1-PF

Travail additionnel déployé et vérifié le 2026-06-18 après constat d'un écart entre le portefeuille IBKR live et le dashboard.

### Fichiers additionnels à committer
- `agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/build_workflow.py`
- `agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/AG1-PF-V1-workflow.json`
- `agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/nodes/00b_fetch_ibkr_state.js`
- `agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/nodes/00c_reconcile_ibkr_ledger.py`
- `outils/scripts/ag1_v4_reconcile_ibkr_live.py`
- `docs/operations/ag1_v4_ibkr_reconcile_20260618.md`

### Cause racine
Les ordres IBKR approuvés ou remplis hors-bande pouvaient arriver après le run AG1 V4 initial. Le dashboard AG1-PF lisait DuckDB uniquement ; les fills tardifs n'étaient donc pas réimportés dans `core.fills`, `core.position_lots` et `core.positions_snapshot`.

### Correctif
`AG1-PF-V1` exécute maintenant une réconciliation read-only IBKR avant de lire le portefeuille :
- fetch broker `/health`, `/positions`, `/fills`, `/account/ledger` ;
- si compte live `U25651155` authentifié/aligné : import idempotent des fills stock manquants, rebuild FIFO `core.position_lots`, snapshot `RUN_RECON_IBKR_PF_*` uniquement en cas d'écart ;
- si IBKR est déconnecté : no-op, le MTM existant continue.

### Vérifs prod
- Script maintenance appliqué une fois : import de 4 fills manquants (`NVDA`, `PEUG.PA`, `ELEC.PA` x2) + snapshot `RUN_RECON_IBKR_20260618_211130`.
- Tests sur copie DuckDB : `NO_DIFF` quand aligné ; fill `NVDA` supprimé artificiellement → réimport `WRITTEN`.
- Après relogin IBKR et run manuel AG1-PF : execution n8n `19189` `success`, `missing_fills=[]`, `position_diffs=[]`, MTM `PFMTM_20260618233554` `SUCCESS`.

### Commit suggéré
9. `ag1_pf: reconcile IBKR live state before dashboard MTM`
