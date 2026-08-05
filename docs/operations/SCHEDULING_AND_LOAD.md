# Ordonnancement & charge système — workflows n8n Trader_IA

**MAJ 2026-07-22.** Vue d'ensemble de tous les workflows actifs : crons, durées moyennes observées, bases DuckDB touchées, et stratégie de déconfliction.
Frise visuelle : [`system_load_gantt.html`](system_load_gantt.html) (à ouvrir dans un navigateur).
Pour les **liens logiques inter-systèmes** (dashboard↔AG1, parité scoring/gates) : voir [`SYSTEM_LINKS_AND_PARITY.md`](SYSTEM_LINKS_AND_PARITY.md).

**Contexte global live depuis le 2026-08-05 :** AG5 07:20, AG6 07:40,
AG7 08:00, AG8 08:20 et synthèse 10:05/13:05/16:05 Paris, L–V. AG9 reste
inactif/en sommeil et son cron 09:35/12:35/15:35 n'est pas enregistré. Les
producteurs macro partagent `macro_data.duckdb`; la synthèse écrit
`global_context_v1.duckdb`. Détail :
[`ag5_ag9_scheduling.md`](ag5_ag9_scheduling.md) et
[`20260805_ag5_ag8_global_context_live_deploy.md`](20260805_ag5_ag8_global_context_live_deploy.md).

## Contexte technique

- **n8n applique les crons en fuseau `Europe/Paris`** (`GENERIC_TIMEZONE`). Les `run_log.started_at` DuckDB sont en **UTC** → toujours convertir (Paris = UTC+2 en été).
- **DuckDB = un seul écrivain par fichier.** Un lecteur ne peut pas s'attacher pendant qu'un écrivain tient le verrou (et inversement) → `IO Error: Could not set lock … Conflicting lock is held`.
- **Timeout par tâche n8n = `N8N_RUNNERS_TASK_TIMEOUT=1200` s (20 min)** → un node ne doit pas dépasser 20 min ; garder les batches modérés.
  ⚠️ **MAJ 2026-07-02 (F3)** : cette variable doit être posée **aussi côté `task-runners`** dans `/docker/root/docker-compose.yml` (le défaut du runner est 60 s ; l'oubli faisait échouer AG2UHQ « timed out after 60 seconds » depuis l'expansion à 563 symboles). Corrigé + runners recréés.
- Base goulot = **`ag2_v3.duckdb`** (univers + signaux techniques) : écrite par AG2, lue par AG3, AG4 et AG1.

## Tableau des workflows actifs

| Workflow | Cron (Paris) | Fréq. | Durée moy. | Max | Base principale (rôle) |
|---|---|---|---|---|---|
| AG2-V3 Technical Watchlist | `0 22,2 * * *` | 7j/7 | ~41 min | 59 | ag2_v3 (écrivain) |
| AG2-V3 Technical Held+Core | `0 9,13,15 * * 1-5` | L-V | ~23 min | 27 | ag2_v3 (écrivain) |
| AG2 Universe Health Quarantine | `0 20 * * 1-5` | L-V | ~8 min | 12 | ag2_v3 (écrivain) |
| AG3-V2 Fundamental Held+Core | `0 0 * * *` | 7j/7 | ~18 min | 28 | ag3_v2 (écrit) / ag2_v3 (lit au start) |
| AG3-V2 Fundamental Watchlist | `0 1,4 * * *` | 7j/7 | ~17 min | 20 | ag3_v2 (écrit) / ag2_v3 (lit au start) |
| AG4-V3 News Watcher | `45 6,10,18 * * 1-5` | L-V | ~89 min | 92 | ag4_v3 (écrit) / ag2_v3 (lit brièvement au start) |
| AG4_Spé-V2 News symbole | `0 5 8,11,14,17 * * 1-5` | L-V | ~24 min | 34 | ag4_spe (écrit) / ag2_v3 (lit au start) |
| AG1 V4 Consensus PM | `0 0 14 * * 1-5` + `0 30 16 * * 1-5` | L-V | ~6 min | 12 | ag1_v4 (écrit) / lit ag2_v3·ag3_v2·ag4 — **2 créneaux** : 14:00 (Euronext) + **16:30 (US ouvert depuis 15:30 + Euronext jusqu'à 17:30)**. Le 16:30 rend les actions US réellement tradables (à 14:00 le marché US est fermé → cotations "C"/figées → gate liquidité). Ne pas déplacer sans raison. |
| AG1-PF MTM | `0 15 9-17 * * 1-5` | L-V | <1 min | 3 | ag1_v4 (MTM horaire) — **H+15 depuis 2026-07-02** (F4 : locks avec AG1 V4 14:00/16:30 + recon qui écrivent la même base) |
| AG4_Spé-Finnhub Global News | `0 0 10,13,16 * * 1-5` | L-V | ~20 min | 30 | ag4_spe |
| AG4_Spé-IBKR Portfolio News | `0 0 10,13,16 * * 1-5` | L-V | ~9 min | 13 | ag4_spe |
| AG4_Spé Health Alert | `0 30 16 * * 1-5` | L-V | <1 min | 2 | ag4_spe |
| YF-ENRICH Daily Refresh | `15 6 * * *` | 7j/7 | ~14 min | 14 | yf_enrichment |

*(Durées = moyenne/max des exécutions `success` sur 8 jours, via `execution_entity`. AG1 V4 : la durée capturée est courte ; le gros du temps LLM est dans des sous-nœuds. AG4-V3 dure ~89 min mais ne tient `ag2_v3` qu'au démarrage.)*

## Stratégie de déconfliction (déployée 2026-06-28)

### Nuit — accès `ag2_v3` / `ag3_v2` sérialisés
```
22:00  AG2 Watchlist   (écrit ag2_v3, ~41min)
00:00  AG3 Held+Core   (lit ag2_v3, écrit ag3_v2, ~18min)
01:00  AG3 Watchlist   (lit ag2_v3, écrit ag3_v2, ~17min)
02:00  AG2 Watchlist   (écrit ag2_v3, ~41min)
04:00  AG3 Watchlist   (lit ag2_v3, écrit ag3_v2, ~17min)
```
Écarts ≥ 33 min, runs ≤ 59 min → aucun chevauchement. AG2 Watchlist = 2 slots × 40 symboles = 80/j → cycle ~3,5 j (< seuil H1 96 h).

### Jour — écrivains `ag2_v3` à l'écart des lecteurs
- **AG2 Held+Core** : `10 8,12,14` → **`0 9,13,15`** (s'écarte d'AG4-Spé 08:05/14:05 et d'AG1 V4 14:00 ; écrit après les lecteurs).
- **AG2UHQ** : `35 18` → **`0 20`** (après les lecteurs du soir, ex. AG4-V3 18:45).
- **AG1 V4 (14:00) inchangé** : c'est un lecteur ; AG4-Spé 14:05 est aussi un lecteur (lecteurs concurrents = OK). Seul l'écrivain AG2 Held+Core a été éloigné de ce créneau.

## Principes pour toute évolution future
1. Avant d'ajouter/déplacer un cron, vérifier les **écrivains** de la même base et garantir un écart ≥ durée_max + marge.
2. Plusieurs **lecteurs** d'une même base peuvent coexister ; seul un écrivain bloque.
3. Garder chaque node < 20 min (timeout tâche) → préférer +de slots à +gros batch.
4. Ne jamais déplacer l'heure d'**AG1 V4 (14:00)** sans décision explicite (run de trading).

## Maintenance DuckDB hors n8n

Les crons système du VPS sont exprimés en **UTC** et s'exécutent le dimanche,
hors des fenêtres d'écriture concernées :

| Heure UTC | Maintenance | Bases |
|---|---|---|
| 07:30 | reconstruction via `defrag_duckdb.py --apply` | `ag1_v4_consensus`, `ag2_v3`, `ag3_v2`, `ag4_spe_v2`, `yf_enrichment_v1` |
| 11:00 | rétention AG4 V3 (news 60 j, erreurs 30 j, zombies 6 h) + `--rebuild` | `ag4_v3` |

Le reconstructeur refuse une base avec WAL actif, conserve le fichier précédent
en `.old` et vérifie colonnes, contraintes, index, vues, nombres de lignes et
permissions avant swap. Ne pas remplacer ces reconstructions offline par un
`CHECKPOINT` explicite dans un node n8n.

## Reste à durcir (proposé, non déployé)
**Retry-hardening** : porter le budget de reconnexion DuckDB des `db_con` (tous les nodes) de ~15 s à ~2-3 min (backoff), pour absorber tout chevauchement transitoire résiduel sans faire échouer le run. Robustesse générale, mais touche de nombreux nodes.
