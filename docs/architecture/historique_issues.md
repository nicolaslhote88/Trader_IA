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

## Issues ouvertes depuis l'audit valorisation (2026-04-23)

Ces points sont issus des rapports `docs/audits/20260423_audit_valorisation/report.md` et `report_segments.md`.

### ✅ 11. AG4 ne tagait pas l'impact géographique ni la classe d'actif

**Constat 2026-04-23** : `news_history` (12 026 lignes) contenait déjà un tagging macro riche (currencies_bullish/bearish, sectors_bullish/bearish, theme, regime) mais aucune étiquette explicite de zone géographique ni de classe d'actif. Conséquence : le Portfolio Manager mélangeait les signaux US/EU et les signaux Equity/FX, d'où une perf incohérente sur les actions US (cf `report_segments.md`) et aucun edge identifiable sur le forex.
**Correction 2026-04-24** (commits `53b4dd3`, `147f912`, `08cd363`) : ajout de 5 colonnes additives dans `news_history` (`impact_region`, `impact_asset_class`, `impact_magnitude`, `impact_fx_pairs`, `tagger_version`), taxonomie fermée dans le prompt LLM, sanitize côté n8n, backfill idempotent disponible sous `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py`. Spec complète dans `docs/specs/ag4_geo_tagging_and_forex_base_v1.md`. **Suivi** : lancer la validation 48 h (requêtes §9 du spec) avant tout passage à AG1_Forex.

### ✅ 12. Pas de base de news FX isolée pour un PM Forex dédié

**Constat 2026-04-23** : le Forex partageait la base news des actions, ce qui empêchait de construire un brief FX synthétique (mélange de signaux et de cadences incompatibles).
**Correction 2026-04-24** (commit `53b4dd3`) : nouvelle base `ag4_forex_v1.duckdb` avec `fx_news_history`, `fx_macro`, `fx_pairs`, `run_log`, `news_errors`. Alimentation double : (a) dual-write depuis AG4-V3 quand `impact_asset_class ∈ {FX, Mixed}` (origin `global_base`) ; (b) workflow `AG4-Forex` dédié ingérant `infra/config/sources/fx_sources.yaml` (origin `fx_channel`). **Suivi** : activer progressivement les sources FX dans le YAML (`enabled: true`) après validation qualité par Nicolas.

### ❌ 13. Divergence `core.position_lots.realized_pnl_eur` vs balance cash

**Constat 2026-04-23** : écart math entre `50000 − cost_basis − cash` et `Σ position_lots.realized_pnl_eur` (ChatGPT −1 109 €, Gemini −2 344 €, Grok +1 350 €). Drift de direction différente selon l'IA → pas un bug systématique mais un bug de séquence d'événements.
**Statut 2026-04-24** : non corrigé. À reprendre après stabilisation AG4 geo. Cf `docs/audits/20260423_audit_valorisation/report.md` §5.

### ❌ 14. `core.fills.fees_eur = 0`, `drawdown_pct = 0 %`, `cash_ledger` vide depuis 02/03

**Constat 2026-04-23** : trois bugs dashboard/instrumentation indépendants mais cumulatifs.
**Impact** : affichage dashboard faussé (notamment drawdown et coûts), impossibilité de reconstituer le cash historique.
**Statut 2026-04-24** : non corrigé. Chantier séparé à ouvrir après audit AG1 cross-LLM (tâche #27).

### ❌ 15. `source="unknown"` sur 8 681 lignes de `news_history`

**Constat 2026-04-24** : l'extraction du champ `source` à l'ingestion RSS échoue fréquemment. N'a pas d'impact sur le tagging LLM mais empêche le routage/filtrage par tier de source.
**Statut 2026-04-24** : non corrigé. À inclure dans un chantier de qualité d'ingestion AG4.

---

## Notes de méthode

- Les issues résolues restent visibles ici pour garder la traçabilité — ne pas les supprimer, seulement changer leur statut en ✅ et citer la PR / le commit / la version.
- Pour ajouter une nouvelle issue : créer une section `### ❌ N. Titre` avec **Constat** + **Impact** + **Correction envisagée**.
