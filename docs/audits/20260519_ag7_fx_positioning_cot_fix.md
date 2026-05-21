# Audit AG7-FX Positioning COT

Date: 2026-05-19

## Constat

- Le dashboard Three Pillars affichait: `Aucune donnée COT`.
- Le workflow n8n `AG7-FX-Positioning` etait actif et execute avec succes.
- Les runs AG7 recents dans `pillars.run_log` etaient `ok`, mais avec
  `records_written=0`.
- La table lue par le dashboard, `cot.speculative_positions`, etait vide.

## Cause

Le client COT utilisait le rapport CFTC `Disaggregated Futures Only`
(`f_disagg.zip`). Ce flux ne fournit pas les futures FX G10 principaux sous les
libelles attendus par AG7. En plus, l'URL courante redirigeait vers un chemin
404 et le client ne suivait pas les redirections.

Resultat: `COTClient` retournait une liste vide sans erreur bloquante, puis AG7
loguait un succes a zero ligne.

## Correctif

- Bascule du client COT vers les fichiers CFTC annuels
  `fut_fin_txt_YYYY.zip` du rapport `Traders in Financial Futures`.
- Support des colonnes CFTC underscorees (`Market_and_Exchange_Names`,
  `Lev_Money_Positions_Long_All`, etc.).
- Normalisation des noms de marche avec suffixe exchange, par exemple
  `EURO FX - CHICAGO MERCANTILE EXCHANGE` -> `EURO FX`.
- Ajout du mapping `NZ DOLLAR -> NZD`.
- `POST /macro/cot/refresh` echoue maintenant explicitement si CFTC retourne
  zero ligne ou si aucune devise recente n'est ecrite.
- AG7 marque le run en erreur si le refresh ou le chargement des scores COT est
  vide.

## Verification VPS

- `POST /macro/cot/refresh`: `records_total=992`, `currencies_updated=8`.
- `cot.speculative_positions`: 992 lignes.
- Dernier rapport COT: `2026-05-12`.
- Devises disponibles: `AUD`, `CAD`, `CHF`, `EUR`, `GBP`, `JPY`, `MXN`, `NZD`.
- `POST /pillars/compute`: scores P3 recalcules pour les devises couvertes.
- Workflow n8n `AG7FXPositioningPillarsV1` republie avec la version contenant
  `COT_REFRESH_EMPTY` et `COT_POSITIONING_EMPTY`.

Note: il n'existe pas de future COT direct pour `USD` dans ce flux; `USD` reste
donc sans score COT direct, sauf modelisation separee via Dollar Index.
