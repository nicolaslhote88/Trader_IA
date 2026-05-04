# Plan de nettoyage & restructuration Trader_IA

**Rédigé par :** Claude (Cowork), à la demande de Nicolas
**Date :** 21 avril 2026
**Statut :** Proposition à valider avant exécution. Aucun fichier n'a été supprimé ni déplacé.

Ce plan est découpé en 4 catégories : **A = suppressions sûres**, **B = dédoublonnage / MAJ**, **C = documentation à refondre**, **D = choix structurels à trancher**.

Pour chaque item je précise : *quoi*, *pourquoi*, *risque de régression*, et *action proposée*. Tu valides / amendes / rejettes item par item si tu veux, ou en bloc par catégorie.

---

## Contexte : ce qui doit rester intouchable

Avant toute chose, voici les chemins **load-bearing** que j'ai croisés avec ton `docker-compose.yml` et les 13 workflows n8n de ta capture. Rien dans ce plan ne touche à ces éléments :

- `vps_hostinger_config/docker-compose.yml` — **le fichier va être mis à jour** (ton paste est plus récent), mais sa position ne bouge pas.
- `yfinance-api/` (context Docker build) → doit rester à la racine.
- `yf-enrichment-service/` (context Docker build) → doit rester à la racine.
- `yf-enrichment-v1/` (monté en volume dans `task-runners` et `yf-enrichment`) → doit rester à la racine avec ce nom (le chemin `/opt/yf-enrichment-v1` est hardcodé dans le compose).
- Tous les dossiers `AG*` → les chemins `AG1-V3-Portfolio manager/...` sont encodés en dur dans `09_upsert_run_bundle_duckdb.code.py` (liste `STATIC_WRITER_PATHS`) pour la résolution du writer sur le VPS. **Renommer ces dossiers casserait la résolution sur la VPS** tant qu'on n'a pas aussi mis à jour le code et redéployé. Proposition : on laisse les noms tels quels dans ce premier round.
- Workflows actifs n8n (13 au total, dont 3 inactifs dans ta capture) : AG0-METADATA-Univers (inactif), Boursorama Compartiments (inactif), extraction-base-duckdb (inactif). Les autres (10) sont `active`. **Aucun fichier des AG* ne sera supprimé.**

---

## Catégorie A — Suppressions sûres (aucun risque)

| # | Fichier / dossier | Taille | Justification | Action proposée |
|---|---|---|---|---|
| A1 | `h origin` (racine) | 4.8 KB | Sortie de `git pager` tombée dans un fichier par erreur de redirection. Contenu = diff d'un commit. | **Supprimer** |
| A2 | `tmp_ag1_extract/` | vide | Dossier tmp orphelin. | **Supprimer** |
| A3 | `tmp_ag1_patch/` | vide | Idem. | **Supprimer** |
| A4 | `AG1-PF-V1/.tmp_patch/` | à vérifier | Dossier tmp dans AG1-PF-V1. Probablement orphelin. | **Inspecter puis supprimer si vide** |
| A5 | Tous les `__pycache__/` | éparpillés | Dossiers de cache Python, déjà dans `.gitignore` mais présents sur disque. | **Supprimer récursivement** (`find . -name __pycache__ -exec rm -rf`) |

**Risque :** zéro. Ces éléments sont soit erreurs soit cache régénérables.

---

## Catégorie B — Dédoublonnage & mises à jour

### B1 — `docker-compose.yml` du repo est OBSOLÈTE vs. ce qui tourne en prod

Diff constaté (ton paste vs. le fichier du repo) :
- Ton paste ajoute `--accesslog=true` + `accesslog.filepath` + volume `/opt/traefik_logs` → non présent dans le repo.
- Ton paste ajoute `N8N_PROXY_HOPS=1`, `EXECUTIONS_DATA_MAX_AGE=72`, `EXECUTIONS_DATA_SAVE_ON_SUCCESS=none`, `EXECUTIONS_DATA_SAVE_MANUAL_EXECUTIONS=false`, `EXECUTIONS_DATA_PRUNE_MAX_COUNT=5000`, `DB_SQLITE_VACUUM_ON_STARTUP=true` → non présent dans le repo.

**Proposition :**
- Remplacer `vps_hostinger_config/docker-compose.yml` par ton paste à jour.

**Risque :** zéro. C'est un rapprochement vers la vérité prod, pas un changement prod.

### B2 — Doublon `POST_AGENT_DUCKDB_LEDGER.md`

Deux copies du même document, légèrement divergentes :
- `AG1-V3-Portfolio manager/docs/POST_AGENT_DUCKDB_LEDGER.md`
- `AG1-V3-Portfolio manager/workflow/docs/POST_AGENT_DUCKDB_LEDGER.md`

**Proposition :** garder **uniquement** `AG1-V3-Portfolio manager/workflow/docs/POST_AGENT_DUCKDB_LEDGER.md` (le dossier `workflow/` est la source de vérité du pack, reconstruit par `rebuild_pack.py`). Supprimer le duplicat parent. Ajouter un lien/renvoi depuis le README parent.

**Risque :** zéro. Pas référencé en code.

### B3 — Doublon `sql/README.md`

`AG1-V3-Portfolio manager/sql/README.md` et `AG1-V3-Portfolio manager/workflow/sql/README.md` sont **identiques** byte-à-byte — normal : `rebuild_pack.py` ligne ~395 écrit le même contenu dans les deux. Ce n'est pas cassé, juste du bruit.

**Proposition :** modifier `rebuild_pack.py` pour ne plus écrire le README dans la copie parent, OU laisser tel quel (pas urgent). À trancher D1.

### B4 — `.gitignore` anémique

Le `.gitignore` actuel fait 4 lignes :

```
__pycache__/
*.pyc
*.pyo
.env
```

**Proposition :** ajouter :

```
# IDE & OS
.vscode/
.idea/
.DS_Store
*.code-workspace

# Logs & tmp
*.log
tmp_*/
.tmp_*/
*.tmp

# Python
.venv/
venv/

# Livrables volatils Cowork / outputs
Etude_Comparative_Brokers_Trader_IA.docx  # → déplacé dans docs/
```

**Risque :** zéro.

### B5 — `MIGRATION.md` racine



**Risque :** zéro.

### B6 — `yfinance-api/MIGRATION.md`

Notes de migration yfinance-api v2.0.0 (168 lignes). Toujours utile comme release notes du service.

**Proposition :** renommer en `yfinance-api/CHANGELOG.md` (c'est sémantiquement plus proche).

**Risque :** zéro (pas référencé).

---

## Catégorie C — Refonte documentation

Le repo manque cruellement d'un **README principal**. Aujourd'hui, quelqu'un qui clone `nicolaslhote88/Trader_IA` atterrit sur `LICENSE` + 13 dossiers et deux .md assez verbeux (`ETAT_DES_LIEUX_FONCTIONNEL.md` 749 lignes, `ANALYSE_SYSTEME_AVANT_AGENT6.md`). Il faut un point d'entrée clair.

### C1 — Créer `README.md` à la racine

Contenu suggéré (sections ~15 lignes chacune) :

1. **Qu'est-ce que Trader_IA ?** — 1 paragraphe, en français, définit le système multi-agent et ses 3 classes d'actifs.
2. **Taxonomie 6 agents** — schéma textuel Portfolio Manager + 3 analystes + Risk Manager + Execution Trader, et mapping AG0→AG4.
4. **Arborescence du repo** — liste des dossiers et leur rôle en 1 ligne.
5. **Documentation** — pointeurs vers `docs/architecture/overview.md`, `docs/operations/deploy.md`, `ANALYSE_SYSTEME_AVANT_AGENT6.md`.
6. **Getting started dev** — comment régénérer le pack AG1 avec `rebuild_pack.py`, comment lancer le dashboard en local.
7. **Statut & roadmap** — phase actuelle (sandbox DuckDB validée), prochaine étape (agent 6 broker).
8. **Licence** — lien vers `LICENSE`.

### C2 — Créer dossier `docs/` et y déplacer :

- `docs/architecture/etat_des_lieux.md` ← depuis `./ETAT_DES_LIEUX_FONCTIONNEL.md` (renommé, minuscules)
- `docs/architecture/analyse_avant_agent6.md` ← depuis `./ANALYSE_SYSTEME_AVANT_AGENT6.md`
- `docs/studies/Etude_Comparative_Brokers_Trader_IA.docx` ← depuis racine
- `docs/operations/deploy.md` ← **nouveau**, documente les commandes `docker compose`, volumes, secrets, rollback.
- `docs/operations/env_vars.md` ← **nouveau**, liste toutes les variables d'env utilisées dans `docker-compose.yml` avec explication.
- `docs/dev/rebuild_pack.md` ← **nouveau**, explique le workflow `rebuild_pack.py` et la relation entre `workflow/` et les fichiers extraits.

### C3 — Nettoyer `ETAT_DES_LIEUX_FONCTIONNEL.md`

749 lignes dont une partie est devenue stale depuis mars. Proposition : **scinder** en deux sous-documents :
- `docs/architecture/etat_des_lieux.md` (vue d'ensemble qui reste vraie)
- `docs/architecture/historique_issues.md` (backlog de problèmes connus, dont certains déjà résolus — à marquer ✅ résolu ou ❌ encore ouvert)

### C4 — Compléter `.env.example`


**Proposition :** créer `vps_hostinger_config/.env.example` qui liste toutes ces variables avec valeurs factices + un commentaire sur chacune.

**Risque :** zéro.

### C5 — Renommer les `GUIDE.md` des sous-agents en `README.md`

Chaque `AGX-Vn/docs/GUIDE.md` serait plus visible sous forme de `AGX-Vn/README.md` à la racine du dossier de l'agent. GitHub affichera alors automatiquement ce README quand on navigue dans le dossier.

Ce n'est pas un déplacement purement cosmétique : ça réduit le nombre de clics pour tout nouveau lecteur.

**Concerne :** AG2-V3, AG3-V2, AG4-V3, AG4-SPE-V2 (AG1-V3 a déjà un README.md à sa racine).

**Risque :** zéro.

---

## Catégorie D — Choix structurels à trancher (je ne fais rien sans ton feu vert)

### D1 — Que faire du doublon `workflow/` dans `AG1-V3-Portfolio manager/` ?

Situation actuelle :
- `AG1-V3-Portfolio manager/nodes/` + `sql/` + `docs/` = extraction "flat" pour lecture humaine
- `AG1-V3-Portfolio manager/workflow/nodes/` + `sql/` + `docs/` = copie normalisée utilisée par `rebuild_pack.py` + `generate_model_variants.py`
- Les deux peuvent diverger (on a vu par exemple 08_build_duckdb_bundle : 362 lignes côté parent vs 187 lignes côté workflow/)

Options :
- **(a) Statu quo** : tolérer la double-écriture, ajouter un CI check qui lève une alerte si les deux trees divergent.
- **(b) Simplifier** : modifier `rebuild_pack.py` pour écrire **uniquement** dans l'arbre parent, et retirer `workflow/nodes/` + `workflow/sql/` + `workflow/docs/` (garder juste `workflow/AG1_workflow_template_v3.json` et `workflow/variants/` + les scripts de génération).
- **(c) Extrémité** : tout centraliser dans `workflow/` et vider le parent.

Ma recommandation : **(b)**. Simplifie de moitié le volume d'AG1-V3 et supprime la classe d'erreur "fichiers qui divergent silencieusement".

**Risque moyen** : il faut relancer `rebuild_pack.py` et vérifier que le résultat est cohérent avec ce qui tourne en prod sur `/files/AG1-V3-EXPORT/...`. Je recommande de faire ça dans une PR dédiée avec un commit "chore(ag1): dedup workflow/ tree" **après** avoir terminé les catégories A/B/C.

### D2 — Restructuration lourde (regrouper sous `agents/`, `services/`, `infra/`) ?

Proposé dans mon brouillon d'architecture mais **déconseillé pour maintenant** car :
- `docker-compose.yml` référence `context: ./yfinance-api` → renommer casse le build.
- `09_upsert_run_bundle_duckdb.code.py` contient une allow-list en dur `AG1-V3-Portfolio manager/...` → renommer casse la résolution.

Mon avis : on ne bouge PAS les dossiers pour cette itération. On pourra le faire dans 6-9 mois dans une vraie PR architecturale quand le broker sera branché et stable.

### D3 — `Trader_IA.code-workspace` et `TradingSim_GoogleSheet_Template.xlsx`

- `Trader_IA.code-workspace` : fichier de config VSCode. Personnel à chaque dev. **Proposition : le retirer du repo** et l'ajouter à `.gitignore`.
- `TradingSim_GoogleSheet_Template.xlsx` : template Google Sheets utilisé par le seed initial. **À garder** mais déplacer dans `docs/operations/` pour dégager la racine.

**Question pour toi :** OK pour retirer le `.code-workspace` ?

### D4 — Versions mortes dans les dossiers AG*

Les sous-dossiers contiennent des workflows anciens (exemple : `AG2-V3/AG2-V3 - Analyse technique (FX only).json`, `AG2-V3/AG2-V3 - Analyse technique (non-FX).json`, `AG2-V3/AG2-V3 - Analyse technique.json`). Ta capture n8n montre que (FX only) et (non-FX) sont actives, la version combinée "Analyse technique" sans suffixe est absente de n8n.

**Question pour toi :** la version sans suffixe est-elle obsolète (remplacée par les deux variantes spécialisées) ou est-ce la version "maître" qu'on garde par précaution ?

### D5 — Workflows n8n inactifs visibles dans ta capture

Trois workflows sont inactifs (icône "moins" grise) :
- AG0-METADATA-Univers
- Boursorama Compartiments A-B-C → Drive (CSV+XLSX)
- extraction base duckdb

**Question pour toi :**
- Veux-tu que je retire leurs JSON du repo (ils correspondent à `AG0-V1 - extraction universe/AG0-V1 - extraction universe.json` pour le premier), ou les garder comme artefacts historiques ?
- Les deux autres n'ont pas de fichier JSON dans le repo (je cherche : aucun `Boursorama*` ni `extraction base duckdb*`). Donc rien à faire de ce côté, juste confirmer.

---

## Synthèse : ordre d'exécution proposé

Si tu valides en bloc, je recommande :

1. **Phase 1 (10 min, zéro risque)** — Catégorie A + B1 + B4 + B5 + B6. Commit : `chore: remove tmp files, refresh docker-compose, expand .gitignore`.
2. **Phase 2 (15 min, zéro risque)** — B2 + B3 (dédup POST_AGENT et sql/README). Commit : `docs(ag1): deduplicate post-agent ledger doc`.
3. **Phase 3 (30 min, zéro risque)** — C1 + C2 + C5 + C4. Création `README.md` racine + `docs/` + renommage GUIDE.md → README.md. Commit : `docs: consolidate documentation under docs/`.
4. **Phase 4 (30 min, zéro risque)** — C3. Scinder et nettoyer `ETAT_DES_LIEUX`. Commit : `docs: split état des lieux into current/historical`.
5. **Phase 5 (hors scope ce round)** — D1/D2/D3/D4/D5. Après tes réponses aux questions.

Après chaque phase, je te ping avec le diff git et tu peux `git reset --hard HEAD~1` si quoi que ce soit te déplaît.

---

## Ce que je NE toucherai PAS dans ce round

- Aucun fichier `.js`, `.py`, `.sql`, `.json` de workflow ou de code d'agent.
- Aucun fichier dans `dashboard/`.
- Aucun fichier dans `yfinance-api/` (sauf renommer MIGRATION.md → CHANGELOG.md en B6 si tu le valides).
- Aucune réorganisation de `AG*` ni renommage.
- Aucune action sur le VPS ni sur les workflows n8n.

---

**À toi.** Un "GO A+B+C" me suffit pour lancer les phases 1-4. Pour D, j'attends tes réponses item par item.
