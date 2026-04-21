# Historique — écarts, risques et points d'attention

Ce document consolide les écarts identifiés au fil des analyses successives.
Chaque entrée est annotée avec son statut :

- ✅ **Résolu** — avec référence au commit / à la version qui a résolu le point.
- 🟡 **En cours** — travail démarré mais non clôturé.
- ❌ **À faire** — non traité à ce jour.

---

## Issues tracées depuis l'analyse 2026-03-02

### ✅ 1. AG4-SPE-V2 workflow JSON corrompu

**Constat 2026-03-02** : `agents/AG4-SPE-V2/AG4-SPE-V2-workflow.json` pesait 4 octets (BOM + newline).
**Workaround initial** : régénérer via `python agents/AG4-SPE-V2/build_workflow.py`.
**Statut 2026-04-21** : le fichier fait environ 112 KB et est à jour. La régénération via `build_workflow.py` est documentée dans le `README.md` d'AG4-SPE-V2. Issue clôturée.

### ❌ 2. Coexistence V2/V3 dans les paths n8n

**Constat 2026-03-02** : le compose `n8n` pointait `AG1_DUCKDB_PATH=/files/duckdb/ag1_v2.duckdb` tandis que le dashboard s'appuyait déjà sur `ag1_v3_*`.
**Statut 2026-04-21** : corrigé dans `infra/vps_hostinger_config/docker-compose.yml` — `n8n` pointe désormais sur `/files/duckdb/ag1_v3.duckdb`. Cependant `AG1-V2-EXPORT` reste monté en lecture seule (`/opt/trader-ia/AG1-V2-EXPORT:/files/AG1-V2-EXPORT:ro`) pour continuité. **À confirmer** : les workflows V2 sont-ils encore nécessaires à terme ?

### 🟡 3. Secrets dans le docker-compose

**Constat 2026-03-02** : `N8N_RUNNERS_AUTH_TOKEN`, `QDRANT_API_KEY`, `DASHBOARD_BASIC_AUTH` apparaissaient en clair dans le compose.
**Statut 2026-04-21** : le compose utilise désormais des interpolations `${VAR}` vers un `.env`. Un template `.env.example` a été publié. **Reste à faire** : chiffrer le `.env` ou migrer vers un vault (sops / docker secrets) avant mise en production live.

### ❌ 4. Duplication de `_news_pill_html` dans `services/dashboard/app.py`

**Constat 2026-03-02** : fonction `_news_pill_html` définie 2 fois (ligne 5021 et 5116). La seconde écrase la première.
**Statut 2026-04-21** : duplication toujours présente (vérifié via grep). **À faire** : supprimer la définition redondante et vérifier qu'aucun site d'appel ne dépend du comportement de la version écrasée.

### 🟡 5. Artefacts d'encodage cp1252/utf-8 dans les strings FR

**Constat 2026-03-02** : plusieurs textes affichent `e?` à la place de `é`, `a^` à la place de `à`, etc.
**Statut 2026-04-21** : la nouvelle documentation est écrite en UTF-8 propre. Le code Python du dashboard et des workflows n'a pas été rescané. **À faire** : passer un linter d'encodage sur l'ensemble des fichiers `.py` / `.js` / `.json` avant livraison finale.

### ❌ 6. Audit post-déploiement automatique

**Recommandation 2026-03-02** : ajouter un audit automatique post-deploy (présence DB/tables/views, dernier run status par workflow, couverture YF enrichment, disponibilité Qdrant).
**Statut 2026-04-21** : non réalisé. Un script `post_deploy_audit.py` serait naturellement hébergé dans `yfinance-api` ou dans un service `audit` dédié.

### ❌ 7. Matrice "workflow → DB → dashboard page"

**Recommandation 2026-03-02** : formaliser une matrice de traçabilité.
**Statut 2026-04-21** : pas de matrice centralisée, mais le `README.md` racine et `docs/architecture/etat_des_lieux.md` couvrent partiellement cette traçabilité. **À faire** : table unique dans `docs/architecture/`.

---

## Issues ouvertes depuis l'analyse broker (2026-04-20)

Ces points sont issus de `ANALYSE_SYSTEME_AVANT_AGENT6.md` (racine) et bloquent la mise en production live.

### ❌ 8. `client_order_id` absent côté broker

**Impact** : risque de double exécution sur timeout réseau entre n8n et le broker.
**Correction** : générer un `client_order_id` unique (ex. `<run_id>-<instrument_id>-<seq>`) et le propager dans `core.orders.broker_order_id` + dans l'appel HTTP broker. Vérifier que le broker retourne le même ID pour l'idempotence.

### ❌ 9. `kill_switch_active` non lu par le Risk Manager

**Impact** : la colonne `cfg.portfolio_config.kill_switch_active` existe mais n'est jamais interrogée par `07_validate_enforce_safety_v5.code.js`. Un kill-switch activé en base ne bloque rien.
**Correction** : ajouter en tête du node 7 une requête `SELECT kill_switch_active FROM cfg.portfolio_config` et shorter l'exécution si `true`.

### ❌ 10. Limites d'exposition non enforced

**Impact** : `max_pos_pct`, `max_sector_pct`, `max_daily_drawdown_pct` sont persistés en base mais jamais appliqués en code. Seules la normalisation FX, l'extraction d'actions et le garde-fou cash sont implémentés.
**Correction** : 3 checks supplémentaires dans `07_validate_enforce_safety_v5.code.js`, avant la construction du bundle d'ordres.

---

## Notes de méthode

- Les issues résolues restent visibles ici pour garder la traçabilité — ne pas les supprimer, seulement changer leur statut en ✅ et citer la PR / le commit / la version.
- Pour ajouter une nouvelle issue : créer une section `### ❌ N. Titre` avec **Constat** + **Impact** + **Correction envisagée**.
