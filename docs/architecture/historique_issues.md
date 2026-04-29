# Historique â€” Ã©carts, risques et points d'attention

Ce document consolide les Ã©carts identifiÃ©s au fil des analyses successives.
Chaque entrÃ©e est annotÃ©e avec son statut :

- âœ… **RÃ©solu** â€” avec rÃ©fÃ©rence au commit / Ã  la version qui a rÃ©solu le point.
- ðŸŸ¡ **En cours** â€” travail dÃ©marrÃ© mais non clÃ´turÃ©.
- âŒ **Ã€ faire** â€” non traitÃ© Ã  ce jour.

---

## Issues tracÃ©es depuis l'analyse 2026-03-02

### âœ… 1. AG4-SPE-V2 workflow JSON corrompu

**Constat 2026-03-02** : `agents/trading-actions/AG4-SPE-V2/AG4-SPE-V2-workflow.json` pesait 4 octets (BOM + newline).
**Workaround initial** : rÃ©gÃ©nÃ©rer via `python agents/trading-actions/AG4-SPE-V2/build_workflow.py`.
**Statut 2026-04-21** : le fichier fait environ 112 KB et est Ã  jour. La rÃ©gÃ©nÃ©ration via `build_workflow.py` est documentÃ©e dans le `README.md` d'AG4-SPE-V2. Issue clÃ´turÃ©e.

### âŒ 2. Coexistence V2/V3 dans les paths n8n

**Constat 2026-03-02** : le compose `n8n` pointait `AG1_DUCKDB_PATH=/files/duckdb/ag1_v2.duckdb` tandis que le dashboard s'appuyait dÃ©jÃ  sur `ag1_v3_*`.
**Statut 2026-04-21** : corrigÃ© dans `infra/vps_hostinger_config/docker-compose.yml` â€” `n8n` pointe dÃ©sormais sur `/files/duckdb/ag1_v3.duckdb`. Cependant `AG1-V2-EXPORT` reste montÃ© en lecture seule (`/opt/trader-ia/AG1-V2-EXPORT:/files/AG1-V2-EXPORT:ro`) pour continuitÃ©. **Ã€ confirmer** : les workflows V2 sont-ils encore nÃ©cessaires Ã  terme ?

### ðŸŸ¡ 3. Secrets dans le docker-compose

**Constat 2026-03-02** : `N8N_RUNNERS_AUTH_TOKEN`, `QDRANT_API_KEY`, `DASHBOARD_BASIC_AUTH` apparaissaient en clair dans le compose.
**Statut 2026-04-21** : le compose utilise dÃ©sormais des interpolations `${VAR}` vers un `.env`. Un template `.env.example` a Ã©tÃ© publiÃ©. **Reste Ã  faire** : chiffrer le `.env` ou migrer vers un vault (sops / docker secrets) avant mise en production live.

### âŒ 4. Duplication de `_news_pill_html` dans `services/dashboard/app.py`

**Constat 2026-03-02** : fonction `_news_pill_html` dÃ©finie 2 fois (ligne 5021 et 5116). La seconde Ã©crase la premiÃ¨re.
**Statut 2026-04-21** : duplication toujours prÃ©sente (vÃ©rifiÃ© via grep). **Ã€ faire** : supprimer la dÃ©finition redondante et vÃ©rifier qu'aucun site d'appel ne dÃ©pend du comportement de la version Ã©crasÃ©e.

### ðŸŸ¡ 5. Artefacts d'encodage cp1252/utf-8 dans les strings FR

**Constat 2026-03-02** : plusieurs textes affichent `e?` Ã  la place de `Ã©`, `a^` Ã  la place de `Ã `, etc.
**Statut 2026-04-21** : la nouvelle documentation est Ã©crite en UTF-8 propre. Le code Python du dashboard et des workflows n'a pas Ã©tÃ© rescanÃ©. **Ã€ faire** : passer un linter d'encodage sur l'ensemble des fichiers `.py` / `.js` / `.json` avant livraison finale.

### âŒ 6. Audit post-dÃ©ploiement automatique

**Recommandation 2026-03-02** : ajouter un audit automatique post-deploy (prÃ©sence DB/tables/views, dernier run status par workflow, couverture YF enrichment, disponibilitÃ© Qdrant).
**Statut 2026-04-21** : non rÃ©alisÃ©. Un script `post_deploy_audit.py` serait naturellement hÃ©bergÃ© dans `yfinance-api` ou dans un service `audit` dÃ©diÃ©.

### âŒ 7. Matrice "workflow â†’ DB â†’ dashboard page"

**Recommandation 2026-03-02** : formaliser une matrice de traÃ§abilitÃ©.
**Statut 2026-04-21** : pas de matrice centralisÃ©e, mais le `README.md` racine et `docs/architecture/etat_des_lieux.md` couvrent partiellement cette traÃ§abilitÃ©. **Ã€ faire** : table unique dans `docs/architecture/`.

---

## Issues ouvertes depuis l'analyse broker (2026-04-20)

Ces points sont issus de `ANALYSE_SYSTEME_AVANT_AGENT6.md` (racine) et bloquent la mise en production live.

### âŒ 8. `client_order_id` absent cÃ´tÃ© broker

**Impact** : risque de double exÃ©cution sur timeout rÃ©seau entre n8n et le broker.
**Correction** : gÃ©nÃ©rer un `client_order_id` unique (ex. `<run_id>-<instrument_id>-<seq>`) et le propager dans `core.orders.broker_order_id` + dans l'appel HTTP broker. VÃ©rifier que le broker retourne le mÃªme ID pour l'idempotence.

### âŒ 9. `kill_switch_active` non lu par le Risk Manager

**Impact** : la colonne `cfg.portfolio_config.kill_switch_active` existe mais n'est jamais interrogÃ©e par `07_validate_enforce_safety_v5.code.js`. Un kill-switch activÃ© en base ne bloque rien.
**Correction** : ajouter en tÃªte du node 7 une requÃªte `SELECT kill_switch_active FROM cfg.portfolio_config` et shorter l'exÃ©cution si `true`.

### âŒ 10. Limites d'exposition non enforced

**Impact** : `max_pos_pct`, `max_sector_pct`, `max_daily_drawdown_pct` sont persistÃ©s en base mais jamais appliquÃ©s en code. Seules la normalisation FX, l'extraction d'actions et le garde-fou cash sont implÃ©mentÃ©s.
**Correction** : 3 checks supplÃ©mentaires dans `07_validate_enforce_safety_v5.code.js`, avant la construction du bundle d'ordres.

---

## Issues ouvertes depuis l'audit valorisation (2026-04-23)

Ces points sont issus des rapports `docs/audits/20260423_audit_valorisation/report.md` et `report_segments.md`.

### âœ… 11. AG4 ne tagait pas l'impact gÃ©ographique ni la classe d'actif

**Constat 2026-04-23** : `news_history` (12 026 lignes) contenait dÃ©jÃ  un tagging macro riche (currencies_bullish/bearish, sectors_bullish/bearish, theme, regime) mais aucune Ã©tiquette explicite de zone gÃ©ographique ni de classe d'actif. ConsÃ©quence : le Portfolio Manager mÃ©langeait les signaux US/EU et les signaux Equity/FX, d'oÃ¹ une perf incohÃ©rente sur les actions US (cf `report_segments.md`) et aucun edge identifiable sur le forex.
**Correction 2026-04-24** (commits `53b4dd3`, `147f912`, `08cd363`) : ajout de 5 colonnes additives dans `news_history` (`impact_region`, `impact_asset_class`, `impact_magnitude`, `impact_fx_pairs`, `tagger_version`), taxonomie fermÃ©e dans le prompt LLM, sanitize cÃ´tÃ© n8n, backfill idempotent disponible sous `infra/maintenance/ag4_geo_backfill/backfill_geo_tags.py`. Spec complÃ¨te dans `docs/specs/ag4_geo_tagging_and_forex_base_v1.md`. **Suivi** : lancer la validation 48 h (requÃªtes Â§9 du spec) avant tout passage Ã  AG1_Forex.

### âœ… 12. Pas de base de news FX isolÃ©e pour un PM Forex dÃ©diÃ©

**Constat 2026-04-23** : le Forex partageait la base news des actions, ce qui empÃªchait de construire un brief FX synthÃ©tique (mÃ©lange de signaux et de cadences incompatibles).
**Correction 2026-04-24** (commit `53b4dd3`) : nouvelle base `ag4_forex_v1.duckdb` avec `fx_news_history`, `fx_macro`, `fx_pairs`, `run_log`, `news_errors`. Alimentation double : (a) dual-write depuis AG4-V3 quand `impact_asset_class âˆˆ {FX, Mixed}` (origin `global_base`) ; (b) workflow `AG4-Forex` dÃ©diÃ© ingÃ©rant `infra/config/sources/fx_sources.yaml` (origin `fx_channel`). **Suivi** : activer progressivement les sources FX dans le YAML (`enabled: true`) aprÃ¨s validation qualitÃ© par Nicolas.

### âŒ 13. Divergence `core.position_lots.realized_pnl_eur` vs balance cash

**Constat 2026-04-23** : Ã©cart math entre `50000 âˆ’ cost_basis âˆ’ cash` et `Î£ position_lots.realized_pnl_eur` (ChatGPT âˆ’1 109 â‚¬, Gemini âˆ’2 344 â‚¬, Grok +1 350 â‚¬). Drift de direction diffÃ©rente selon l'IA â†’ pas un bug systÃ©matique mais un bug de sÃ©quence d'Ã©vÃ©nements.
**Statut 2026-04-24** : non corrigÃ©. Ã€ reprendre aprÃ¨s stabilisation AG4 geo. Cf `docs/audits/20260423_audit_valorisation/report.md` Â§5.

### âŒ 14. `core.fills.fees_eur = 0`, `drawdown_pct = 0 %`, `cash_ledger` vide depuis 02/03

**Constat 2026-04-23** : trois bugs dashboard/instrumentation indÃ©pendants mais cumulatifs.
**Impact** : affichage dashboard faussÃ© (notamment drawdown et coÃ»ts), impossibilitÃ© de reconstituer le cash historique.
**Statut 2026-04-24** : non corrigÃ©. Chantier sÃ©parÃ© Ã  ouvrir aprÃ¨s audit AG1 cross-LLM (tÃ¢che #27).

### âŒ 15. `source="unknown"` sur 8 681 lignes de `news_history`

**Constat 2026-04-24** : l'extraction du champ `source` Ã  l'ingestion RSS Ã©choue frÃ©quemment. N'a pas d'impact sur le tagging LLM mais empÃªche le routage/filtrage par tier de source.
**Statut 2026-04-24** : non corrigÃ©. Ã€ inclure dans un chantier de qualitÃ© d'ingestion AG4.

---

## Notes de mÃ©thode

- Les issues rÃ©solues restent visibles ici pour garder la traÃ§abilitÃ© â€” ne pas les supprimer, seulement changer leur statut en âœ… et citer la PR / le commit / la version.
- Pour ajouter une nouvelle issue : crÃ©er une section `### âŒ N. Titre` avec **Constat** + **Impact** + **Correction envisagÃ©e**.
