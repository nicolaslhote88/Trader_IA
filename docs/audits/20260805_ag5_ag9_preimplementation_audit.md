# Audit préalable AG5–AG9 — 2026-08-05

## 1. Périmètre et méthode

Audit effectué avant toute modification fonctionnelle du chantier AG5–AG9.

Sources vérifiées :

- dépôt local, branche initiale `codex/dashboard-ag1-20260721`, commit
  `ae652dacb50922049fda4a3df36a1ba51aa93e2b` ;
- documents obligatoires listés dans la mission ;
- code AG5–AG8, `macro-data-api`, AG3-FX, AG1-FX, AG1 V4 et dashboard ;
- VPS `srv961978`, le 2026-08-05 entre 15:47 et 16:30 UTC, en lecture seule ;
- SQLite n8n, DuckDB et fichiers déployés ;
- documentation, dépôt et catalogue MCP publics de World Monitor.

Aucun workflow n'a été exécuté et aucun ordre n'a été placé ou confirmé.

## 2. Faits vérifiés — dépôt

- Branche de travail créée : `codex/ag5-ag9-global-context-20260805`.
- Le working tree contenait avant le chantier des modifications AG2/AG4 et
  `AGENTS.md` du 2026-07-30. Elles appartiennent à un chantier antérieur et ne
  doivent pas être écrasées ni mélangées aux commits AG5–AG9.
- `services/dashboard/app.py` et `three_pillars_tab.py` sont identiques au live :
  SHA-256 respectifs `ee93fbe0…` et `3c5cf420…`.
- Les six fichiers principaux de `services/macro-data-api/` sont identiques à
  ceux de l'image live (SHA-256 vérifiés fichier par fichier).
- Le workflow AG1 V4 versionné a 40 nœuds. Ses connexions, son consensus, son
  Risk Manager, son nœud d'envoi IBKR et son bundle DuckDB ont les mêmes hashes
  de code que le workflow live publié.
- Divergence ciblée AG1 : les trois extracteurs live ont des constantes de nom
  internes différentes du JSON repo, mais le bundle/consensus persiste bien les
  clés historiques `chatgpt52`, `grok41_reasoning`, `claude_sonnet46`. Les douze
  dernières propositions DuckDB portent les modèles réels attendus et
  `parse_ok=true`.

## 3. Faits vérifiés — VPS et trading

Au 2026-08-05 15:47 UTC :

- broker `healthy`, `authenticated=true`, `connected=true` ;
- compte live `U25651155` aligné ;
- `dry_run=false` pour Actions et `fx_orders_enabled=false` ;
- aucune approbation en attente ;
- AG1 V4 publié : `versionId=activeVersionId`, deux crons 14:00 et 16:30 Paris ;
- AG1-FX, AG1-FX-PF, AG2-FX, AG3-FX, AG4-FX et AG5–AG8 historiques :
  `active=false`, `activeVersionId=null` ;
- quinze workflows Trader_IA actifs, tous hors Forex ;
- derniers runs naturels AG1 V4 du jour : succès à 14:00 et 16:30 Paris ;
- aucune base `worldmonitor_v1.duckdb` ni `global_context_v1.duckdb` présente.

Conclusion de sûreté : le chantier doit rester fail-open et consultatif. Toute
publication d'AG1 avant replay et shadow violerait les critères de la mission.

## 4. État AG5–AG8 et données

### 4.1 Workflows historiques

| Workflow | État live | Cron historique Paris | Dernière publication |
|---|---:|---|---|
| AG5-FX-Macro | inactif/non publié | `5 3 * * 1-5` | 2026-06-11 |
| AG6-FX-Valuation | inactif/non publié | `15 3 * * 1-5` | 2026-06-11 |
| AG7-FX-Positioning | inactif/non publié | `25 3 * * 1-5` | 2026-06-11 |
| AG8-FX-Rates | inactif/non publié | `35 3 * * 1-5` | 2026-06-11 |

Le builder lance AG5 puis AG6/AG7/AG8, alors qu'AG5 appelle déjà
`/pillars/compute`. Le snapshot historique n'est donc pas atomique.

### 4.2 `macro_data.duckdb`

Base de 3,5 MiB, écrite par `macro-data-api` (DuckDB 1.1.3 dans l'image) et lue
en 1.4.x par les autres composants.

| Table | Lignes | Dernière observation réelle |
|---|---:|---|
| `macro.policy_rates` | 119 | 2026-06-10 |
| `macro.country_indicators` | 44 | 2026-05-01 |
| `cot.speculative_positions` | 1 016 | rapport 2026-06-02 |
| `rates.yield_curve` | 258 | 2026-06-11 |
| `pillars.currency_scores` | 251 | 2026-06-11 |
| `pillars.run_log` | 99 | 2026-06-11 |

Le service live répond `/health`, mais aucune collecte AG5–AG8 n'est active :
les données sont périmées et ne doivent pas être présentées comme courantes.

### 4.3 Défauts méthodologiques confirmés

AG5 :

- valeurs manquantes converties en scores `0.0` ;
- balance courante scorée en milliards USD absolus, sans PIB ;
- taux neutres codés en dur et traités comme certitudes ;
- conclusion normative « USD très négatif / JPY et EUR positifs » dans le README ;
- fréquences et millésimes non publiés dans le contrat ;
- composite AG5–AG8 déclenché depuis AG5 ;
- erreurs de log masquées par `except: pass`.

AG6 :

- double calcul dans le nœud AG6 et `scoring.py` ;
- absence de valeur transformée en zéro ;
- « PPP » calculée comme simple écart d'inflation sans spot ni ancre ;
- REER absent ;
- carry utilisé comme majorité de la valorisation ;
- écriture directe concurrente dans `macro_data.duckdb` depuis n8n.

AG7 :

- rapport TFF CFTC et redirections corrigés dans le code actuel ;
- garde `zero rows` déjà ajoutée mais dates/fraîcheur non contractuelles ;
- COT hebdomadaire encore enveloppé dans un run quotidien ;
- proxy USD et sources non directes insuffisamment exposés dans les vues legacy ;
- commentaire « hated = opportunité » transformant un signal contrarian en
  conclusion normative.

AG8 :

- sortie principale contient `long_2y_short_10y`/`short_2y_long_10y` ;
- « courbe inversée = récession imminente » et biais USD normatif ;
- jambes 2Y/10Y pouvant avoir des dates différentes ;
- variation 30 jours choisie sans tolérance de proximité ni contrôle de fréquence ;
- absence de distinction observation/proxy ;
- erreurs de lecture et de log masquées.

## 5. Deux synthèses concurrentes

- `macro-data-api/scoring.py` construit un composite trois piliers.
- AG6 et AG8 recalculent des composantes déjà produites par le service.
- AG3-FX reconstruit un autre score fondamental et une cible d'équilibre.
- AG1-FX relit les piliers et assemble encore un autre brief.
- `three_pillars_tab.py` recalcule fraîcheur, interprétations et plusieurs
  représentations métier.

Architecture retenue :

1. observations et composants AG5–AG8 dans `macro_data.duckdb`, writer unique
   `macro-data-api` ;
2. données et AG9 dans `worldmonitor_v1.duckdb`, writer unique
   `worldmonitor-adapter` ;
3. snapshot canonique dans `global_context_v1.duckdb`, writer unique
   `global-context-synthesizer` ;
4. dashboard et AG1 lecteurs read-only de vues canoniques ;
5. AG3-FX reste historique/hors exécution et devient consommateur éventuel, pas
   source de vérité commune.

## 6. Dashboard

- `Three Pillars Monitor` est sous le groupe `Forex`.
- AG6 n'est pas toujours distingué visuellement d'une valorisation cross-asset.
- le module recalcule fraîcheur, seuils, couleurs, agrégats et interprétations ;
- les absences sont parfois converties par `fillna(0)` dans les graphiques ;
- AG8 affiche encore une stratégie d'ordre obligataire et le biais USD historique.

La nouvelle section `Contexte global` devra lire uniquement les vues versionnées.
La page Forex historique restera disponible séparément.

## 7. World Monitor — vérifications 2026-08-05

### 7.1 Catalogue et protocole

- serveur MCP public : `https://worldmonitor.app/mcp` ;
- handshake réussi, protocole `2025-03-26` ;
- serveur annoncé : `worldmonitor` version `1.15.0` ;
- `tools/list` public retourne **59 outils**, contre 39 dans une documentation
  encore indexée ;
- capacités pertinentes présentes : conflits, risque pays, sanctions, posture
  militaire, convergence, anomalies temporelles, focal points, news, cyber,
  chokepoints, maritime, airspace, énergie, supply chain, catastrophes,
  infrastructures, tarifs, macro et marchés ;
- les appels `tools/call`, y compris `describe_tool`, exigent OAuth ou une clé au
  moment de l'audit. L'absence de credential empêche un test de données live,
  mais pas la découverte ni les tests sur fixtures.

### 7.2 Licence et décision d'intégration

Le dépôt `koala73/worldmonitor` est sous **AGPL-3.0-only**. Le README autorise
l'usage commercial sous respect des obligations AGPL et prévoit une licence
commerciale séparée pour un usage propriétaire hors AGPL.

Décision : ne copier aucun code World Monitor. Utiliser MCP/REST via un adaptateur
indépendant et conserver seulement des contrats, mappings et normalisations
propres à Trader_IA. Cette séparation évite de contaminer le dépôt MIT par une
copie de code AGPL. La conformité finale d'un usage professionnel/commercial
reste une décision juridique/opérationnelle, pas une conclusion technique.

## 8. Risques de migration

1. Lock DuckDB si plusieurs writers historiques restent actifs.
2. Upgrade de format si une image utilise DuckDB > version du lecteur n8n.
3. Faux succès à zéro ligne dans les anciens nœuds.
4. Snapshot composé de millésimes incompatibles.
5. Changement de schéma World Monitor ou disparition d'un outil.
6. Double comptage AG4/AG9.
7. Pack AG1 trop volumineux ou différent entre les trois branches.
8. Import n8n qui désactive involontairement un workflow live.
9. Réactivation accidentelle du Forex.
10. Modification indirecte du consensus/Risk Manager/broker.

Réduction de risque : nouvelles bases dédiées, workflows shadow inactifs,
configurations fail-open, fixtures, hashes de contrat, transactions, replay sans
broker et comparaison des hashes des nœuds d'exécution.

## 9. Fichiers réellement concernés (prévision)

- `services/macro-data-api/` : schéma, méthodes AG5–AG8, endpoints et tests ;
- `services/worldmonitor-adapter/` : nouveau service ;
- `services/global-context-synthesizer/` : nouveau writer de snapshot ;
- `agents/common/global-context/` : contrats, mappings, configs, workflows,
  fixtures et replay ;
- AG1 V4 : uniquement chargement/pack/persistance avant fan-out et migration
  additive du ledger ;
- `services/dashboard/` : nouvelle page commune, sans recalcul métier ;
- `infra/` : variables exemples/migrations/configuration shadow ;
- documentation demandée par la mission.

Exclus du périmètre : consensus 2/3, calcul de quantité, Risk Manager, preflight
liquidité, envoi broker, approbations Telegram, règles de stop/cash/exposition et
variables IBKR live.

## 10. Hypothèses et validations restantes

Hypothèses :

- le mode MCP sera le mode privilégié, REST et self-hosted restant supportés ;
- les credentials World Monitor seront fournis hors repo au moment du shadow ;
- DuckDB 1.4.3 est la borne maximale retenue pour les nouvelles bases afin de
  rester compatible avec les lecteurs n8n actuels ;
- le contexte initial restera `GLOBAL_CONTEXT_ENABLED=false` sur AG1 live.

À valider après implémentation :

- appels World Monitor authentifiés sur service réel ;
- qualité et coût/quota des outils retenus ;
- plusieurs cycles AG5–AG9 cohérents sur VPS ;
- replay AG1 trois scénarios et identité de pack ;
- shadow AG1 sans aucune connexion vers le broker ;
- absence de collision dans le scheduling ;
- activation finale explicitement autorisée.
