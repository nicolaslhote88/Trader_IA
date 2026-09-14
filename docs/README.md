# Documentation Trader_IA

État documentaire consolidé au 2026-08-10. Les documents datés dans `audits/`
et `operations/` conservent volontairement les faits observés à leur date ; les
documents ci-dessous décrivent l'état courant.

## Points d'entrée

| Besoin | Référence |
|---|---|
| État fonctionnel du système | `architecture/etat_des_lieux.md` |
| Instructions et garde-fous Codex | `../AGENTS.md` |
| Architecture du contexte global | `architecture/global_context_architecture.md` |
| Contrats AG5–AG9 | `architecture/ag5_ag9_data_contracts.md` |
| Scoring du contexte global | `architecture/global_context_scoring.md` |
| Liens AG1 ↔ dashboard | `operations/SYSTEM_LINKS_AND_PARITY.md` |
| Crons et contention DuckDB | `operations/SCHEDULING_AND_LOAD.md` |
| Déploiement VPS | `operations/deploy.md` |
| Investigation n8n | `operations/runbook_n8n_investigation.md` |
| Accès VPS | `operations/vps-access.md` |
| Variables d'environnement | `operations/env_vars.md` |
| Exécution et approbation IBKR | `operations/ibkr_execution.md`, `operations/order_approval_deploy_notes.md` |

## Derniers changements live

- Contrat de performance, positions, préflight, comparaison EUR et horaires US :
  [corrections du 14 septembre 2026](operations/20260914_performance_contract_remediation.md).

- AG5–AG8, synthèse atomique et pack consultatif AG1 :
  `operations/20260805_ag5_ag8_global_context_live_deploy.md`.
- Remédiation de la qualité des sources AG5–AG8 :
  `operations/20260806_ag5_ag8_data_quality_remediation.md`.
- AG9 dormant sans abonnement payant :
  `operations/20260805_ag9_dormant_free_tier.md`.
- Contrat multi-modèles AG1 V4 :
  `operations/20260730_ag1_v4_deepseek_output_contract_fix.md`.
- Migrations DeepSeek AG2 et AG4_Spé : documents `operations/20260730_*deepseek*`.
- Migration DeepSeek V4 Flash hors AG1 :
  `operations/20260810_deepseek_v4_flash_migration.md`.
- Navigation métier simplifiée du dashboard :
  `operations/20260810_dashboard_navigation_reorganization.md`.
- Vue unique et visuelle du contexte de marché AG5–AG8 :
  `operations/20260810_global_context_single_view_redesign.md`.
- Contrat d'exécution AG1, ticks IBKR et alertes structurées :
  `operations/20260810_ag1_execution_contract_remediation.md`.
- Rotation AG2 et faux succès n8n :
  `operations/20260806_ag2_batch_rotation_cursor_fix.md`.
- Statuts H1 AG2 et restauration du funnel pré-tradable :
  `operations/20260806_ag2_h1_soft_stale_fix.md`.

## Statut synthétique

- Actions/ETF : live réel via AG1 V4, consensus GPT-5.6 Sol / DeepSeek V4 Pro /
  Claude Opus 4.8.
- AG2 : split Held+Core/Watchlist, rotation transactionnelle active et seuil
  dur H1/D1 aligné à 96 h avec AG1/dashboard.
- AG3 : split Held+Core/Watchlist, yfinance sans LLM.
- AG4_Spé : Boursorama, Finnhub et IBKR analysés avec DeepSeek V4 Flash.
- AG5–AG8 : actifs et consultatifs ; AG9 : dormant.
- Forex trading : désactivé.

Les rapports antérieurs restent utiles pour la traçabilité, mais leur section
« reste à faire » ne prévaut jamais sur `AGENTS.md`, le présent index et les
notes d'opération plus récentes.
