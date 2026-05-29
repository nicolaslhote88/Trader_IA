# Intervention AG1-FX - durcissement performance portefeuille

Date: 2026-05-29

## Constat

L'audit des executions AG1-FX a confirme que la sous-performance recente ne
venait pas principalement d'un manque de donnees. Les sources AG2, AG3, AG4,
AG5 et le cube a 3 piliers etaient presentes, mais AG1-FX pouvait encore
produire des decisions economiquement mauvaises:

- micro-trades trop petits face aux frais IBKR observes;
- prises de profit brutes positives mais nettes negatives apres frais;
- churn de sortie sans invalidation decisive du signal long terme;
- incoherence entre l'univers complet disponible et le wording du prompt;
- cas de prefunding qui pouvait demander une conversion sur la meme paire dans
  le sens oppose de l'ordre cible.

Les frais realises observes sur le ledger etaient proches de 1.6 EUR par fill,
avec plusieurs ordres de faible notionnel incapables de couvrir le cout aller /
retour. Le probleme principal etait donc l'alignement decisionnel et economique
d'AG1-FX, pas uniquement la qualite des donnees amont.

## Correctifs appliques

- Ajout d'une section `execution_economics` dans le brief AG1-FX:
  frais estimes par fill, notionnel minimum, objectif de notionnel, gain brut
  minimum attendu, reward-to-fee minimum et seuil de profit net de cloture.
- Enrichissement des lots ouverts avec une estimation de frais de sortie et de
  PnL net si cloture immediate.
- Mise a jour du prompt systeme AG1-FX:
  l'agent doit raisonner en profit net apres frais, eviter les micro-trades,
  exiger un take-profit exploitable, et ne plus fermer un lot sur un petit gain
  brut si le net attendu est negatif.
- Mise a jour du validateur risque:
  - rejet `TRADE_ECONOMICS_MISSING_TAKE_PROFIT`;
  - rejet `TRADE_ECONOMICS_NOTIONAL_TOO_SMALL`;
  - rejet `TRADE_ECONOMICS_REWARD_TOO_SMALL`;
  - rejet `CLOSE_ECONOMICS_NEGATIVE_NET`;
  - rejet `DUPLICATE_CLOSE_LOT`;
  - rejet `PREFUNDING_SELF_PAIR_CONFLICT`.
- Passage du plafond `REDUCED_SIZE_ONLY` par defaut de 10% a 15% du portefeuille
  afin que les trades autorises restent assez grands pour absorber les frais,
  tout en restant sous le plafond global par paire.
- Correction du wording "27 paires" en wording dynamique base sur
  `universe_pairs`.
- Ajout d'un replay de maintenance:
  `infra/maintenance/ag1_fx_v1_smoke/risk_guard_replay.js`.

## Parametres par defaut

- `AG1_FX_ESTIMATED_FEE_PER_FILL_EUR=1.75`
- `AG1_FX_MIN_NEW_TRADE_NOTIONAL_EUR=1200`
- `AG1_FX_TARGET_NEW_TRADE_NOTIONAL_EUR=1700`
- `AG1_FX_MIN_EXPECTED_GROSS_PROFIT_EUR=8`
- `AG1_FX_MIN_REWARD_TO_FEE=2.0`
- `AG1_FX_MIN_CLOSE_NET_PROFIT_EUR=2.0`
- `AG1_FX_MIN_DISCRETIONARY_HOLD_HOURS=24`
- `AG1_FX_REDUCED_SIZE_MAX_PAIR_PCT=0.15`

Ces valeurs sont volontairement conservatrices pour le compte paper actuel.
Elles pourront etre ajustees apres observation des prochains runs si le
notionnel moyen, le cout IBKR ou le regime de volatilite changent.

## Validation

Validation locale / VPS effectuee sans execution live du workflow:

- syntaxe JavaScript OK dans le runtime Node du conteneur `root-n8n-1`;
- replay `risk_guard_replay.js` OK, avec `failures=[]`;
- cas controles:
  - micro-trade rejete par `TRADE_ECONOMICS_NOTIONAL_TOO_SMALL`;
  - reward insuffisant rejete par `TRADE_ECONOMICS_REWARD_TOO_SMALL`;
  - prefunding contradictoire rejete par `PREFUNDING_SELF_PAIR_CONFLICT`;
  - cloture a net negatif rejetee par `CLOSE_ECONOMICS_NEGATIVE_NET`;
  - double cloture d'un meme lot rejetee par `DUPLICATE_CLOSE_LOT`.

## Deploiement VPS

Workflow actif:

- id: `3IiaEQTYEHgMh_6aLo6H9`
- nom: `AG1-FX-V1 Portfolio Manager - chatgpt52`
- nouvelle version n8n: `2635e12d-550c-4a68-8e78-3651878fc7a2`
- sauvegarde SQLite:
  `/var/lib/docker/volumes/n8n_data/_data/database.sqlite.backup_ag1fx_perf_20260529070841`

La version active n8n contient les marqueurs:

- `TRADE_ECONOMICS_REWARD_TOO_SMALL`
- `PREFUNDING_SELF_PAIR_CONFLICT`
- `CLOSE_ECONOMICS_NEGATIVE_NET`
- `universe_pairs`

Le workflow a ete publie via `n8n publish:workflow`, puis `root-n8n-1` et les
trois `root-task-runners-*` ont ete redemarres.

## Suivi attendu

Sur les prochains runs AG1-FX, le bon comportement attendu est:

- moins d'ordres de tres faible notionnel;
- moins de clotures de micro-gains net-negatifs;
- davantage de `hold` lorsque les donnees sont neutres ou peu convergentes;
- rejets explicites `TRADE_ECONOMICS_*` avant broker lorsque l'ordre n'a pas
  d'esperance nette suffisante;
- aucun retour des erreurs de prefunding auto-contradictoire.

Le KPI a surveiller n'est pas uniquement le nombre d'ordres acceptes, mais la
qualite economique moyenne des ordres acceptes: notionnel, reward-to-fee, frais
par ordre, PnL net realise et turnover du portefeuille.
