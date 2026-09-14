# Correction sémantique du funnel AG2 — 2026-08-13

## Problème

La page **Analyse technique** affichait un funnel `Analyses → Actionnables →
Appels IA → IA approuvées`. Ces étapes n'étaient pas un pipeline monotone :

- le total mélangeait les symboles en rotation et les lignes historiques hors
  rotation ;
- « Actionnables » désignait seulement une direction D1 BUY/SELL brute ;
- `APPROVE` était présenté comme une validation technique nécessaire alors
  qu'AG1 bloque uniquement `REJECT` ;
- la fraîcheur affichait l'âge de la ligne la plus récente au lieu de la
  couverture réelle de l'univers.

## Contrat corrigé

Le funnel global est désormais :

1. **Univers configuré** : symboles actifs de `ag2_v3.universe` ;
2. **Rotation AG2 active** : symbole segmenté, hors quarantaine et classe
   d'actif supportée par AG1 ;
3. **Technique prête AG1** : H1/D1 `OK`, bougies clôturées et âge effectif
   `max(âge stocké, âge réel du signal) ≤96 h` ;
4. **Non bloquée par l'IA** : étape précédente et décision différente de
   `REJECT`.

Le funnel ne dépend pas des filtres graphiques. Les directions BUY/SELL, appels
IA, `APPROVE` et `REJECT` sont affichés séparément. Le champ `ai_quality` est
libellé **score de setup**, car il ne mesure pas la fiabilité du modèle.

## Validation

- tests unitaires : `services/dashboard/tests/test_ag2_operational_funnel.py` ;
- contrôle syntaxique : `python -m py_compile services/dashboard/app.py
  services/dashboard/app_modules/ag2_funnel.py` ;
- contrôle live attendu au déploiement : `563 → 361 → 310 → 251`, avec 62
  appels IA exploitables et 3 `APPROVE` frais au moment du diagnostic.

## Déploiement live vérifié

- release : `/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard` ;
- sauvegarde : `.codex-tmp/ag2-funnel-20260813T205458Z/` dans la release ;
- redémarrage limité à `root-trading-dashboard-1` ;
- endpoint Streamlit `/_stcore/health` : `ok` ;
- 14 tests dashboard ciblés/connexes : `OK` ;
- calcul DuckDB post-déploiement : `563 → 361 → 310 → 251`, 62 appels IA
  exploitables, 3 `APPROVE` frais et 59 `REJECT` frais.

## Rollback

Le dashboard est monté en lecture seule depuis
`/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard`.
Pour revenir en arrière, restaurer `app.py` depuis
`.codex-tmp/ag2-funnel-20260813T205458Z/`, supprimer le nouveau module
`app_modules/ag2_funnel.py`, puis redémarrer uniquement
`root-trading-dashboard-1`.
