# 2026-07-16 — Bandeau P&L : refonte présentation (arrêt du mélange de bases)

Dashboard VPS `root-trading-dashboard-1`, `/opt/trading-dashboard/app/app.py` (backups `app.py.bak_pnl_reconcile_*`, `app.py.bak_banner_*`).

## Problème
Le bandeau « AG1 V4 Consensus » juxtaposait, comme si elles formaient une seule équation, deux comptabilités incompatibles :
- **Vérité IBKR (NetLiq)** : P&L total net, P&L latent. Réalisé net cumulé = Total − Latent.
- **Ledger interne** (`position_lots` / `fill_costs`) : « P&L réalisé brut » (cumul prix des trades fermés) et « Fees ».

Ex. 16/07 : Total −19,42 ; Latent 11,28 ⟹ réalisé net **−30,70** (IBKR). Mais brut 144,50 − fees 101,37 = **+43,13**. Écart ~73,8 €. Une rustine précédente (13/07) forçait « réalisé net = total − latent » (correct) mais laissait une légende fausse (« ~175 € de frais sur trades fermés » alors que total frais = 101 €).

## Cause de fond (irréconciliable, pas un simple bug de frais)
Trois « réalisés » cohabitent sans coïncider : `position_lots` 144,50 (cumul prix, **clôtures USD stockées en devise native, non converties EUR** — bug FX réel), `base_ledger.realizedpnl` IBKR 26,55 (réalisé « session », repart pas du capital), et Total − Latent −30,70 (seul réalisé net cumulé fiable). Même en convertissant l'USD, le ledger interne ne peut pas égaler le réalisé net IBKR (change sur cash, période, conventions). Le **−30,70 € est la seule vérité**.

## Correctif (présentation)
Refonte en deux blocs distincts + suppression de la légende fausse :
- **Bloc 1 — Décomposition P&L source IBKR (boucle exactement)** : `P&L réalisé net (= Total − Latent)` + `P&L latent` = `P&L total net`. Légende : dérivé de la réconciliation IBKR (NetLiq − capital − latent), agrège frais des trades fermés + change + dividendes.
- **Bloc 2 — Ledger interne (indicatif)** : `Trades fermés (P&L prix brut)`, `Frais bruts payés`, `Couverture frais`. Légende explicite : **ne boucle PAS** avec le bloc 1 (clôtures USD en natif, base/période différentes), à usage indicatif seulement.

Vérifié live : Bloc 1 boucle (−30,70 + 11,28 = −19,42). Dashboard sain.

## MAJ 2026-07-16 (suite) — VRAIE cause : le bandeau prenait le latent « prix seul » d'IBKR
Le tableau Positions montrait P&L total **−58,67 €** (= P&L prix **+11,28** + Impact FX **−69,95**) alors que le bandeau « P&L latent » affichait **+11,28**. Cause : le bandeau utilisait `base_ledger.unrealizedpnl` d'IBKR = **prix seul** (performance du cours au taux courant), qui **ignore l'impact du change** sur le coût des titres USD. Pour un investisseur EUR, le vrai latent en euros est **Valeur_EUR − Coût_EUR = −58,67** (ce que montre le tableau).

Conséquence : avec le mauvais latent (11,28), le réalisé net sortait à −30,70 et ne recoupait pas le ledger (écart 74 €). Avec le **bon** latent économique (−58,67), tout se recale :
- Réalisé net = Total − Latent = −19,42 − (−58,67) = **+39,25 €**
- Vérif indépendante (cash + coût − capital) = **+39** ✓
- Ledger brut − frais = 144,50 − 101,37 = **+43,13** → écart résiduel **~4 €** (bug FX clôtures USD natif dans `position_lots`).

**Correctif (app.py, backup `app.py.bak_latent_eco_*`).** Le loader calcule le latent économique EUR = Σ `pnl_total_eur` des positions (même source que le tableau Positions) et l'expose (`latent_economic_eur`, `latent_prix_eur`, `latent_fx_eur`). Le bandeau « P&L latent » pointe désormais dessus (au lieu du prix-seul IBKR) → **identique au total du tableau Positions**. Réalisé net = Total − Latent. Légende bloc 1 : « latent = prix + Impact FX ». Légende bloc 2 : recoupe le bloc 1 à ~quelques € près (résiduel = FX clôtures USD natif). Vérifié : boucle exacte.

## MAJ 2026-07-22 — Onglet Rendement : pont tuiles ↔ cascade (fin de l'« écart résiduel » opaque)

**Problème.** Dans l'onglet Rendement Financier (portefeuille AG1 V4), les tuiles (réalisé clos 166,94 / partiel −18,22 / latent 107,22) sont en base **brute prix**, « Gain total » 89,07 est **net** (NAV IBKR − capital). Le caption affichait « Écart résiduel de −166,87 EUR … flux hors P&L (coûts IA, dividendes, ajustements cash) » — faux : l'écart se décompose **exactement** en frais broker −127,90 + ajustement base clôtures USD +105,41 + impact change latent −144,38 (résidu 0,00). La cascade juste en dessous portait déjà ces postes, mais le caption était calculé sans elle (et rendu avant son calcul).

**Correctif (app.py, backup `app.py.bak_bridge_20260722_*`).**
- Calcul de `compute_financial_waterfall` **déplacé avant** le bandeau (try/except, `_wf=None` si échec).
- Caption remplacé par un **pont exact** utilisant les mêmes variables que la cascade : `real_fees_total`, `recon_adjustment`, `latent_fx_eur`, + terme `latent_price_eur − latent_pnl` (écart snapshot vs table, absorbe le cas trust_snapshot). Résidu inexpliqué = residual_other − somme des postes ; « RACCORD ✓ » si < 0,50 €, sinon seulement là le message « flux hors position » (à investiguer).
- Nouvel expander « Pont de reconciliation : tuiles (brut prix) → gain total (net) » avec tableau des postes.
- Le bloc cascade réutilise `_wf` (plus de double calcul) ; fallback ancien message si cascade indisponible.

**Vérifié** : identité résidu=0 prouvée par construction (le plug `recon_adjustment` aligne sur la NAV quand `total_val` fourni) + test synthétique (cas nominal et cas écart snapshot/table → 0,000000). Compile Python 3.12 du container OK (⚠ py_compile 3.10 échoue sur un backslash f-string **préexistant** l.12612 — non bloquant, prod en 3.12). Déployé + restart `root-trading-dashboard-1`, health 200. Local `services/dashboard/app.py` = VPS (md5 vérifié avant/après). Non commité (modifs live 13-16/07 déjà en attente dans le working tree).

## Reste (optionnel)
- **Nettoyage FX `position_lots`** : convertir en EUR le `realized_pnl` des clôtures USD (stockées en natif) — hygiène de données ; ne réconciliera pas au réalisé net IBKR mais rendra le ledger interne cohérent en EUR. Non fait (le bloc est désormais étiqueté « non réconcilié »).
- **Accès** : après restructuration `D:\IA`, la clé VPS est pontée dans le `~/.ssh` du sandbox via `scripts/bootstrap-ssh-sandbox.sh` (clés physiques dans `.ssh/`, gitignorées). Le mount bash gardait l'ancien symlink en cache → pont via dossier frais.
