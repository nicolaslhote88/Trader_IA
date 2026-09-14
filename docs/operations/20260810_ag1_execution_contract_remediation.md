# Remédiation du contrat d'exécution AG1 et des `agent_warning`

Date de déploiement live : **2026-08-10**.

## Décision

Les cinq remédiations validées ont été déployées sur AG1 V4, le broker IBKR et
le dashboard. Aucun ordre n'a été placé ou confirmé manuellement et aucun
garde-fou d'exécution n'a été modifié.

## Faits validés avant correction

- Au dernier état juste avant publication : 33 alertes plates `AGENT_WARNING`
  sur 12 runs, pour 42 décisions `CONSENSUS_APPROVED`. Le run de 14:00 a encore
  ajouté `MIN_ORDER_VALUE_EUR:TSM` et deux `STALE_D1`, confirmant la récidive.
- Le consensus divisait une cible EUR par un prix natif, notamment sur les
  actions USD, puis la safety étiquetait le notionnel natif comme EUR.
- Le ticket minimal de 1 000 EUR n'était contrôlé qu'après le consensus.
- TKO.PA utilise un pas IBKR de 0,02 EUR entre 10 et 20 EUR ; 16,99 EUR n'est
  donc pas un prix valide, alors que 16,98 EUR l'est.
- R8 produisait les flags durs de fraîcheur/fermeture des barres, mais la matrice
  ne les utilisait pas comme condition bloquante.
- Toutes les alertes étaient persistées avec le même code `AGENT_WARNING`, ce
  qui empêchait un suivi fiable par cause.

## Changements déployés

### 1. Devise native et normalisation EUR

Le préflight résout désormais la devise du contrat, lit le ledger IBKR et, si
nécessaire, complète par un snapshot FX. Il transmet `currency`,
`fx_rate_to_eur` et `price_eur`. Le consensus, la safety, les plafonds de
concentration, le cash et les snapshots du ledger travaillent en EUR.

Un BUY/INCREASE sans conversion fiable est bloqué par
`FX_RATE_UNAVAILABLE`. Les ordres SELL restent surveillés à partir des valeurs
EUR du portefeuille.

### 2. Faisabilité avant consensus

`AG1.V4 — Liquidity Preflight` calcule `execution_constraints` : ticket minimal,
quantité additionnelle minimale, quantité finale min/max, poids cible minimal,
cash et plafond de position. Une ligne impossible reçoit
`MIN_ORDER_UNFEASIBLE`. Chaque vote BUY est aussi revalidé : une cible dont le
delta final est inférieur à 1 000 EUR devient non exécutable et ne peut plus
former un consensus 2/3.

### 3. Pas de cotation IBKR

Le broker expose les métadonnées de `/iserver/contract/rules`. Juste avant un
ordre LIMIT réel, il sélectionne le palier actif puis arrondit :

- BUY : vers le bas ;
- SELL : vers le haut.

Le prix effectivement soumis est renvoyé au workflow et devient le prix du
ledger. Une impossibilité de lire les règles échoue fermée avec
`IBKR_PRICE_RULE_PREFLIGHT_FAILED`.

### 4. Gates durs de données

Les flags `MISSING_TECH`, `TECH_BARS_NOT_CLOSED`, `TECH_STATUS_NOT_OK`,
`STALE_H1`, `STALE_D1`, `MISSING_YF` et `STALE_YF` interdisent maintenant
`Entrer / Renforcer` dans la matrice. Le dashboard applique exactement la même
règle et calcule l'âge effectif comme le maximum entre l'âge stocké et l'âge
réel du dernier bar. `MISSING_FUNDA`/`STALE_FUNDA` restent neutralisés, sans gel
dur du trading, conformément au contrat existant.

### 5. Alertes structurées et récidives

Le writer persiste désormais le code cause réel (`MIN_ORDER_VALUE_EUR`,
`IBKR_MIN_PRICE_VARIATION`, `STALE_H1`, etc.), la catégorie, le symbole, l'étape,
le détail et, si applicable, la valeur observée/le seuil. `core.runs.risk_gate_json`
contient les nombres de consensus approuvés, rejets safety/broker et ordres
exécutables.

Après chaque run, les occurrences des huit derniers jours sont normalisées, y
compris les anciens `AGENT_WARNING`. Un couple code/symbole présent sur au moins
trois runs, ou un code système présent sur au moins cinq runs, crée une alerte
`CHRONIC_AGENT_WARNING`.

## Validation

- Syntaxe des cinq nodes JavaScript : OK dans le conteneur n8n.
- Smoke BUY + SELL existant : 2 ordres validés en dry-run local, aucun fill.
- Replay CRM USD : cible 11 % → 7 actions, notionnel **1 000,03 EUR** ; l'ancien
  calcul natif aurait produit 6 actions.
- Replay cible CRM 8 % : deux votes non exécutables avec
  `MIN_ORDER_VALUE_EUR`, donc aucun consensus tradable.
- Replay matrice : setup frais fort → `Entrer / Renforcer` ; même setup avec
  `STALE_H1` → `Surveiller|HARD_DATA_GATE`.
- Test broker : 16,99 BUY → 16,98 ; 17,15 SELL → 17,16 ; changement de palier à
  20 EUR validé.
- Probe live read-only : TKO.PA = EUR avec pas 0,02 dans ce palier ; TSM = USD
  avec pas 0,01.
- Test récidive sur DuckDB temporaire : trois anciens `AGENT_WARNING` identiques
  produisent `CHRONIC_AGENT_WARNING` avec le code normalisé.
- Après déploiement : n8n `/healthz` 200, dashboard `/_stcore/health` OK, broker
  authentifié et aligné sur le compte réel, zéro approbation en attente.

Incident de déploiement : le premier redémarrage n8n a marqué `crashed`
l'exécution AG4_Spé-V2 `20925`, commencée à 14:05 Paris et encore active à
14:30. Aucun run AG1 ni ordre n'a été interrompu. Cette collecte news sera
reprise par son prochain cron normal de 17:05 Paris ; aucun replay manuel n'a
été déclenché. Le second redémarrage a été effectué après contrôle explicite
qu'aucune exécution n'était active.

## Versions live

- Workflow `AG1V4CONSENSUS` : `active=1`, `versionId == activeVersionId ==`
  `86c464a6-d58d-4aaa-a5b0-2aa706df1b0a`.
- Image broker : `sha256:66ddb5257fb7133dcf6a321d5a16aaff5f236f67100ef874608806329fbdbb14`.
- Dashboard `app.py` : SHA256
  `fdff1d7291825a9737e34c599dca6ba27c349b0ba87961c1c3e6d1f5591a76a0`.
- AG1 V4 conserve volontairement `deepseek-v4-pro`, conformément à l'exception
  « AG1 V4 reste avec le plus gros modèle ».

## Sauvegarde et rollback

Manifest :
`/opt/trader-ia/.codex-tmp/agent_warning_fix_20260810/SHA256SUMS` (rollback)
et `DEPLOYED_SHA256SUMS` (fichiers actifs, vérifié avec `sha256sum -c`).

Workflow précédent :
`/local-files/.codex-tmp/agent_warning_fix_20260810/AG1V4CONSENSUS_pre.json`.

Broker et dashboard précédents :
`/opt/trader-ia/.codex-tmp/agent_warning_fix_20260810/{broker,dashboard}/`.

Rollback workflow : importer le JSON précédent, republier
`AG1V4CONSENSUS`, puis redémarrer n8n et les trois task-runners. Rollback broker :
restaurer les deux fichiers sauvegardés, reconstruire `ibkr-broker`, puis le
recréer. Rollback dashboard : restaurer `app.py`, puis redémarrer
`root-trading-dashboard-1`.

## Vérification différée

Le prochain run naturel de 16:30 Paris doit être contrôlé sans déclenchement
manuel : succès du run, codes structurés, `risk_gate_json`, absence de nouveaux
rejets de pas IBKR et cohérence consensus → safety → broker.
