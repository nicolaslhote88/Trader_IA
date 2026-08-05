# Rapport post-implémentation AG5–AG9 — 2026-08-05

> **Addendum live :** ce rapport fige l'état de qualification shadow. La
> promotion AG5–AG8/synthèse/AG1 consultatif a ensuite été autorisée et réalisée,
> tandis qu'AG9 a été mis en sommeil. Voir
> [`../operations/20260805_ag5_ag8_global_context_live_deploy.md`](../operations/20260805_ag5_ag8_global_context_live_deploy.md).

## 1. Résumé exécutif

Le code complet du contexte global a été construit : réhabilitation AG5–AG8,
adaptateur AG9 World Monitor, synthèse atomique, pack consultatif identique avant
les trois branches AG1, audit ledger, dashboard commun, fixtures, contrats et
replay sans ordre.

Un shadow isolé tourne sur le VPS. Il a créé un snapshot `DEGRADED` cohérent à
partir des quatre composants macro historiques et d'AG9 indisponible. Les sept
workflows ont été importés, tous inactifs/non publiés. Le workflow AG1 live est
resté actif sur la même version; broker et Forex sont inchangés.

La publication des producteurs et d'AG1 enrichi n'est pas autorisée : aucune clé
World Monitor n'était disponible, les données macro de la copie sont périmées,
aucun cycle naturel n'a été observé et le shadow LLM manual-only n'a pas été
exécuté. Le code est prêt, le déploiement final reste conditionnel.

## 2. Faits vérifiés

### Repo/local

- branche : `codex/ag5-ag9-global-context-20260805` ;
- tests ciblés : 45/45 passés ;
- `compileall` Python et parsing de 77+ JSON passés ;
- compose parsé avec PyYAML local et `docker compose config --quiet` sur VPS ;
- DuckDB : macro 1.1.3, nouveaux writers 1.4.3 ;
- consensus, safety et transport broker protégés par hashes de non-régression ;
- builder AG1 reproductible : candidat 43 nœuds, shadow 37 nœuds.

Le working tree contenait avant mission des modifications AG2/AG4/AGENTS du
2026-07-30. Elles ont été préservées et doivent rester hors des commits de ce
chantier, hormis le hunk AGENTS propre à AG5–AG9.

### VPS shadow

Répertoire : `/opt/trader-ia-shadow/ag5-ag9-20260805`.

| Élément | Preuve |
|---|---|
| services | `ag5ag9-macro-shadow`, `ag5ag9-worldmonitor-shadow`, `ag5ag9-synth-shadow`, `ag5ag9-dashboard-shadow` actifs |
| exposition | localhost `18081`, `18082`, `18083`, `18502` uniquement |
| bases | `macro_data_ag5ag9_shadow.duckdb`, `worldmonitor_v1_shadow.duckdb`, `global_context_v1_shadow.duckdb` |
| catalogue WM | 19 capacités ciblées, 19 résolues, 0 manquante, mode `CATALOG_ONLY` |
| AG5 | 12 lignes, couverture 0, `DEGRADED` — données anciennes correctement non scorées |
| AG6 | 12 lignes, couverture 0,125, `DEGRADED`, durée 11,944 s |
| AG7 | 9 lignes, couverture 1, `DEGRADED`, millésimes CFTC conservés |
| AG8 | 12 lignes, couverture 0,8375, `DEGRADED` |
| synthèse | 4 composants disponibles, AG9 absent, 24 lignes atomiques |
| pack | 7 901 caractères, ~1 975 tokens, hash SHA-256, `advisory_only=true` |
| locks | 0 erreur de lock dans les trois services |
| dashboard | `DASHBOARD_GLOBAL_CONTEXT_SMOKE_OK` via `streamlit.testing.v1` |
| replay | 20 runs historiques lus, mode `READ_ONLY_NO_ORDER_NO_LLM` |

Snapshot shadow publié : `GC_20260805T165651Z_7ba52b15`; couverture 0,18,
confiance 0,158936, fraîcheur `missing`, warning `AG9_UNAVAILABLE`.

### n8n et trading après import

- n8n 2.3.5 ;
- AG5, AG6, AG7, AG8, AG9, synthèse et AG1 shadow : `active=false`,
  `activeVersionId=null` ;
- AG1 live : `active=true`, version active inchangée
  `e5b3f226-1db2-40a8-bdf6-64d209bde1b4` ;
- sauvegarde pré-import :
  `/opt/trader-ia-shadow/ag5-ag9-20260805/backups/n8n-before-import/` ;
- broker : authentifié, `dry_run=false`, `fx_orders_enabled=false`, 0 approbation
  en attente ;
- aucun workflow Forex de PM/collecte réactivé et aucun ordre envoyé.

## 3. Architecture finale

Voir `docs/architecture/global_context_architecture.md`. Chaque base possède un
writer unique. Le synthétiseur lit les snapshots exacts, refuse zéro composant,
publie en transaction et garde les horizons séparés. Dashboard/replay sont
read-only. AG1 obtient une seule référence de pack avant fan-out.

## 4. Changements par fichier/groupe

| Chemin | Changement | Raison | Test |
|---|---|---|---|
| `services/macro-data-api/scoring.py` | formules pures canoniques | supprimer biais/0 artificiels/duplications | `test_scoring.py` |
| `components.py`, `market_client.py`, `world_bank_client.py` | collecte, lineage, millésimes | contrats AG5–AG8 complets | `test_components_contract.py` |
| `macro_db.py`, `main.py`, `config/*.json` | schémas, writer/endpoints, configs | writer unique, zéro ligne explicite | tests macro + shadow |
| anciens `AG5…AG8/nodes/*` | supprimés | retirer writers/formules concurrents | workflow safety |
| `build_three_pillars_workflows.py` + 10 JSON miroirs | workflows HTTP minces inactifs | orchestration sans logique métier | JSON + import n8n |
| `services/worldmonitor-adapter/*` | transport, registry, raw, AG9, mappings | intégration faible couplage | 13 tests + catalogue |
| `tests/fixtures/*.json` | 14 cas contractuels | CI sans service réel | tests normalizer |
| `services/global-context-synthesizer/*` | transaction et pack | snapshot atomique | 3 tests + shadow |
| `ag1_gc_attach_advisory_pack.code.js` | fetch/fallback/hash commun | un pack identique | test contrat AG1 |
| `build_v4_workflow.py`, deux workflows AG1 | point d'injection + shadow sans broker | replay avant live | hashes/graph tests |
| writer/schema AG1 | colonnes audit additives | reproductibilité des décisions | contrat ledger |
| `services/dashboard/global_context_tab.py`, `app.py` | page commune complète | sortir le contexte du Forex | smoke local/VPS |
| `replay_ag1_global_context.py` | trois variantes read-only | comparaison sans ordre | test sécurité + VPS |
| `infra/...compose.yml`, env example | services/variables sûres | déploiement reproductible | YAML/compose VPS |
| `docs/architecture/*global*`, `docs/operations/ag5_ag9_*` | architecture, contrats, runbooks | exploitation/rollback | revue documentaire |
| README, AGENTS, état des lieux, historique, env, scheduling | navigation et statut | mémoire durable | diff ciblé |

Le manifeste Git exact est celui de la draft PR; aucun `.duckdb`, `.env` réel ou
payload distant n'y est inclus.

## 5. Formules corrigées — avant/après

| Domaine | Avant | Après |
|---|---|---|
| macro | manquant→0, CA absolue, taux neutre certain, biais USD | null, CA `%PIB`, estimation versionnée/incertaine, aucun prior |
| valorisation | inflation différentielle nommée PPP, carry dominant | PPP seulement fair+spot, REER distinct, renormalisation |
| positionnement | interprétation normative, date run | `clamp(-z/2)`, vraie date CFTC, USD proxy ≤0,60 |
| taux | steepener sans direction, ordre long/short | direction des jambes obligatoire, régime descriptif |
| AG9 | inexistant | severity×confidence×diversity×decay×relevance, agrégat borné |
| synthèse | composites concurrents | dimensions séparées; agrégat qualité seulement |

Détails et poids : `docs/architecture/global_context_scoring.md`.

## 6. AG1 V4

Point d'injection : après `Assemble Input Packs`, avant le preflight inchangé.
`Attach Advisory Pack` produit un objet immuable dont le même hash est reçu par
les trois agents. Les prompts distinguent explicitement AG4 news et AG9 risque
structuré. Les positions détenues sont évaluées avant les opportunités.

Le ledger persiste snapshot ID, hash, versions, âge, statut et JSON exact. Le
fallback `DISABLED/UNAVAILABLE/STALE` reste consultatif. Aucune donnée de contexte
ne touche les quantités déterministes.

Hashes sources invariants : consensus `c39434c3…`, safety `d658f005…`, broker
`060d6494…`. La version live n'a pas changé. Le shadow importé ne contient ni
schedule, preflight, safety, writer, broker ni approbation.

## 7. Dashboard

Navigation `Commun → Contexte global`, puis neuf onglets communs. AG9 a ses neuf
sous-onglets : synthèse, événements, pays, chokepoints/supply, énergie, cyber,
source health, runs, méthode. Les requêtes ciblent uniquement les vues
canoniques des trois bases avec `read_only=True`. Bases absente/vide/verrouillée,
volumes, erreurs et secret sont testés. Le smoke VPS a exécuté la page sans
exception; aucune carte n'a été ajoutée sans dépendance cartographique fiable.

## 8. Tests et limites

Commandes : voir `ag5_ag9_runbook.md`. Le replay VPS a comparé 250 caractères
baseline, 7 865 AG5–AG8 et 7 901 AG5–AG9. Les variations de propositions,
consensus, pertinence AG9, faux positifs et latence LLM sont volontairement
`NOT_RUN` : elles nécessitent une clé World Monitor et des runs shadow capturés.

Le runtime JS n'était pas installé localement; l'import n8n 2.3.5 des sept JSON
valide leur structure et les quatre Code nodes modifiés ont passé `node --check`
dans le container n8n du VPS (`NODE_CHECK_4_OK`).

## 9. Risques résiduels et statut

| Lot | Statut | Motif |
|---|---|---|
| AG5–AG8 code/contrats | COMPLET | tests et shadow manuels passés |
| AG5–AG8 activation | SHADOW UNIQUEMENT | sources historiques périmées; cycles naturels requis |
| AG9 mocks/registry | COMPLET | fixtures + catalogue 19/19 |
| AG9 données réelles | BLOQUÉ | credential/abonnement World Monitor absent |
| Synthèse atomique | SHADOW UNIQUEMENT | snapshot démontré; AG9 réel absent |
| Dashboard | SHADOW UNIQUEMENT | smoke complet; live non modifié |
| AG1 candidat | COMPLET MAIS NON DÉPLOYÉ | code/tests prêts, publication interdite |
| AG1 shadow LLM | À VALIDER | workflow importé mais non exécuté |
| Production trading | COMPLET — INCHANGÉ | version AG1/broker/Forex inchangée |
| PR/merge | À VALIDER | draft seulement; ne pas fusionner avant contrôles live |
