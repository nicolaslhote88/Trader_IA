# Trader_IA

Plateforme de trading assistée par IA, orchestrée par n8n sur un VPS Hostinger.
Le système combine des Portfolio Managers LLM, des analystes spécialisés, un
Risk Manager déterministe, un broker FastAPI branché sur IBKR Client Portal et
un dashboard Streamlit.

**État opérationnel vérifié au 2026-08-06.** Le système Actions/ETF fonctionne en
**LIVE réel** sur le compte IBKR `U25651155` (`IBKR_DRY_RUN=false`,
`AG1_ACTIONS_LIVE_ORDERS_ENABLED=true`). Le Portfolio Manager actif est
**AG1 V4 consensus** : GPT-5.6 Sol, DeepSeek V4 Pro et Claude Opus 4.8 votent, puis le
workflow applique une règle de consensus 2/3 avant toute exécution. Gemini a été
retiré. Le Forex est entièrement désactivé (`fx_orders_enabled=false`, workflows
FX inactifs) ; les bases FX sont conservées mais figées.

**Attention : ordres réels.** Toute modification du chemin AG1 V4, du broker
IBKR ou des variables d'exécution doit être validée avec les garde-fous du projet.
Ne jamais déclencher ni confirmer un ordre manuellement depuis le code.

## 1. Architecture fonctionnelle

| # | Agent | Rôle | Implémentation principale |
|---|---|---|---|
| 1 | Univers | Extraction et maintenance de l'univers d'investissement : tickers, métadonnées, secteurs | `outils/AG0-V1 - extraction universe/` |
| 2 | Portfolio Manager | Allocation, cibles de position et ordres théoriques. En production Actions/ETF : AG1 V4 consensus GPT-5.6 Sol + DeepSeek V4 Pro + Claude Opus 4.8 | `agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/` |
| 3 | Analyste Technique | Indicateurs H1/D1, validation DeepSeek et rotation Held+Core/Watchlist vérifiée | `agents/trading-actions/AG2 - La technique/AG2-V3/` |
| 4 | Analyste Fondamental | Financials, valorisation, earnings | `agents/trading-actions/AG3 - Les fondamentaux/AG3-V2/` |
| 5 | Analyste Sentiment / News | News macro, sentiment marché et signaux par valeur ; les trois workflows AG4_Spé utilisent DeepSeek V4 Pro | `agents/common/AG4-V3/`, `agents/trading-actions/AG4 - Les news/AG4-SPE-V2/` |
| 6 | Risk Manager + Execution Trader | Validation déterministe, consensus, écriture DuckDB, envoi IBKR et approbation Telegram | `agents/trading-actions/AG1 - Portfolio manager/AG1-V4-Consensus Portfolio manager/workflow/nodes/post_agent/`, `services/ibkr-broker/` |

## 2. Workflows actifs

Workflows actifs côté Actions/ETF :

- `AG1V4CONSENSUS` : Portfolio Manager Actions/ETF live, consensus 2/3.
- `AG1-PF-V1` : mark-to-market horaire V4.
- `AG2V3HELDCORE20260619` et `AG2V3WATCHNIGHT20260619` : analyse technique
  split avec curseur transactionnel ; `AG2UHQ20260619` maintient la quarantaine.
- `AG3-V2` : analyse fondamentale (split 2026-06-22 en `Fundamental Held+Core` + `Fundamental Watchlist Nightly`).
- `AG4-V3` : News Watcher macro, dual-branch `reduced/full`.
- `AG4_Spé-V2`, `AG4SPEFINNHUBV1` et `AG4_Spé-IBKR-V1` : analyse news par
  valeur via DeepSeek V4 Pro et parseur structuré.
- `YF-ENRICH-V1` : enrichissement Yahoo Finance.

Workflows d'approbation :

- `AG1 V4 — Order Approval Request` : notification Telegram quand un ordre doit
  être validé hors bande.
- `AG1 V4 — Order Approval Decide` : webhook appelé par les boutons
  Approuver/Rejeter.

Forex : workflows FX inactifs. Ne pas les réactiver sans décision explicite.

Le contexte commun est live depuis le 2026-08-05 pour AG5 macro, AG6
valorisation relative FX, AG7 positionnement et AG8 régime taux/liquidité. Sa
synthèse atomique est strictement consultative pour AG1. AG9 World Monitor reste
en sommeil et est exclu des poids (`GLOBAL_CONTEXT_ENABLED_COMPONENTS=AG5,AG6,AG7,AG8`).
Architecture et statut :
`docs/architecture/global_context_architecture.md` et
`docs/operations/20260805_ag5_ag8_global_context_live_deploy.md`.

La qualité des sources AG5–AG8 a été remédiée et validée le 2026-08-06 : le
pack représentatif est `OK`, `use_policy=CAUTION`, couverture `0,908` et
confiance `0,685`. La sortie LLM est compactée à 4 000 caractères maximum et
reste strictement consultative. La rotation AG2 a également été réparée : le
premier run Held+Core post-correction a traité 27/27 symboles et avancé
son curseur `0 → 18`.

## 3. Exécution IBKR et approbation

AG1 V4 utilise des prix frais avant d'envoyer les packs aux LLM, avec un
préflight de liquidité qui interroge IBKR pour les symboles retenus. L'objectif
est d'éviter de consommer trois appels LLM sur une réflexion inexécutable.

Chemin d'exécution Actions/ETF :

1. Construction du portefeuille, des opportunités et du brief compact.
2. Préflight IBKR/yfinance : résolution contrat, snapshot, historique de secours.
3. Raisonnement des trois LLM.
4. Consensus 2/3 sur symbole + intention.
5. Safety deterministic checks.
6. Envoi IBKR via `services/ibkr-broker`.
7. Écriture ledger DuckDB et health check.

Garde-fous broker :

- Déviation prix limite vs référence <= 5 % : confirmation automatique si le
  prompt IBKR est qualifié comme prompt prix.
- Déviation 5 % à 15 % : ordre parqué, notification Telegram, revalidation au clic.
- Déviation > 15 % : rejet.
- Prix non vérifiable (`QUOTE_TOO_OLD`, `NO_REFERENCE_PRICE`,
  `QUOTE_FETCH_FAILED`) : parking Telegram.
- Prompt IBKR `without market data` : auto-confirmation si la garde prix
  yfinance valide une déviation ≤5 % ; sinon parking/rejet selon les bandes
  précédentes. Une approbation re-soumet un ordre frais et traite la nouvelle
  chaîne de prompts, sans rejouer un `reply_id` périmé.

Détails : `docs/operations/order_approval_deploy_notes.md`.

## 4. Stack technique

- **n8n** : orchestration des workflows. Instance partagée : Trader_IA, SIGA et
  templates. Filtrer les opérations sur `AG*` / `YF*`.
- **DuckDB** : vérité métier et ledger V4 dans `/local-files/duckdb/` sur le VPS.
- **IBKR Client Portal Gateway** : passerelle compte réel `U25651155`.
- **ibkr-broker** : service FastAPI, image Docker rebuildée depuis
  `/opt/trader-ia/services/ibkr-broker/` sur le VPS.
- **yfinance-api** : quotes et données Yahoo Finance.
- **yf-enrichment** : enrichissement quotidien.
- **macro-data-api** : données macro et marché complémentaires.
- **worldmonitor-adapter** : découverte/normalisation AG9, sans code World
  Monitor copié ; conteneur arrêté tant qu'AG9 reste dormant.
- **global-context-synthesizer** : snapshot atomique et pack consultatif AG1.
- **Streamlit dashboard** : dashboard V4-only.
- **Traefik** : reverse proxy TLS.

Deux stacks Docker Compose sont utilisées sur le VPS :

- n8n : `/docker/root`
- IBKR/yfinance : `/docker/yfinance`

## 5. Structure du dépôt

```text
Trader_IA/
├── agents/
│   ├── common/
│   │   ├── AG4-V3/
│   │   ├── global-context/
│   │   └── yf-enrichment-v1/
│   ├── trading-actions/
│   │   ├── AG1 - Portfolio manager/
│   │   │   ├── AG1-PF-V1/
│   │   │   ├── AG1-V3-Portfolio manager/
│   │   │   └── AG1-V4-Consensus Portfolio manager/
│   │   ├── AG2 - La technique/
│   │   │   └── AG2-V3/
│   │   ├── AG3 - Les fondamentaux/
│   │   │   └── AG3-V2/
│   │   └── AG4 - Les news/
│   │       └── AG4-SPE-V2/
│   └── trading-forex/
├── docs/
│   ├── archives/
│   ├── audits/
│   ├── operations/
│   ├── studies/
│   └── specs/
├── infra/
├── outils/
│   ├── AG0-V1 - extraction universe/
│   └── scripts/
├── services/
│   ├── dashboard/
│   ├── ibkr-broker/
│   ├── macro-data-api/
│   └── yfinance-api/
└── snapshots/
```

Le layout GitHub n'est pas identique au layout VPS. En particulier,
`/opt/trader-ia` sur le VPS n'est pas un clone Git : toute modification broker
déployée doit aussi être commitée dans ce dépôt.

## 6. Documentation utile

| Sujet | Fichier |
|---|---|
| Instructions projet pour Codex | `AGENTS.md` |
| Investigation n8n | `docs/operations/runbook_n8n_investigation.md` |
| Déploiement VPS | `docs/operations/deploy.md` |
| Accès VPS | `docs/operations/vps-access.md` |
| Variables d'environnement | `docs/operations/env_vars.md` |
| Exécution IBKR | `docs/operations/ibkr_execution.md` |
| Approbation ordres | `docs/operations/order_approval_deploy_notes.md` |
| Audit brief LLM AG1 V4 | `docs/audits/20260615_ag1_v4_prompt_audit.md` |
| Audit AG4 V3 | `docs/audits/20260617_ag4_v3_news_watcher_audit.md` |
| Plans AG4 SPE | `docs/audits/20260617_ag4_spe_v2_analysis.md`, `docs/audits/20260617_ag4_spe_v2_remediation_plan.md` |
| Contexte global AG5–AG9 | `docs/architecture/global_context_architecture.md` |
| Runbook AG5–AG9 | `docs/operations/ag5_ag9_runbook.md` |
| État fonctionnel courant | `docs/architecture/etat_des_lieux.md` |
| Correctif qualité AG5–AG8 | `docs/operations/20260806_ag5_ag8_data_quality_remediation.md` |
| Correctif rotation AG2 | `docs/operations/20260806_ag2_batch_rotation_cursor_fix.md` |
| Index de la documentation | `docs/README.md` |

## 7. Conventions de sécurité

- Les lectures DuckDB doivent être faites en `read_only=True`.
- Ne jamais afficher ni committer de secret, clé privée ou fichier `.env` réel.
- Ne pas modifier les variables live IBKR sans décision explicite.
- Ne jamais confirmer un ordre depuis le code ou la console.
- Toute nouvelle version d'un workflow live doit être validée en shadow/replay
  avant publication.

## 8. Licence

MIT. Voir `LICENSE`.
