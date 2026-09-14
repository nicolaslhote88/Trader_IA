# Réorganisation de la navigation du dashboard (2026-08-10)

## Problème

La barre latérale mélangeait univers d'investissement, fonctions métier et
noms techniques de workflows. Les radios horizontales `Commun / Actions /
Forex` étaient serrées et les pages exposaient directement des libellés comme
`AG2`, `AG3`, `AG4`, `V2` ou `Monitoring`.

## Organisation retenue

La navigation conserve deux niveaux, mais leur rôle devient explicite :

1. un sélecteur **Espace** stable ;
2. une liste verticale **Vues** propre à cet espace.

| Espace | Vues |
|---|---|
| Pilotage global | État du système ; Décisions & opportunités ; Contexte de marché ; Macro & actualités |
| Actions / ETF | Portefeuille actions ; Analyse technique ; Analyse fondamentale |
| Forex | Portefeuille Forex ; Contexte & piliers |

Les identifiants historiques des pages restent inchangés. Le routage et les
chargements DuckDB ne sont donc pas modifiés. Chaque espace mémorise sa dernière
vue dans une clé Streamlit distincte.

## Implémentation

- configuration et rendu : `services/dashboard/app_modules/navigation.py` ;
- branchement : `services/dashboard/app.py` ;
- titres métier harmonisés dans `app.py` et `three_pillars_tab.py` ;
- contrat : `services/dashboard/tests/test_navigation.py` ;
- style : lignes verticales pleine largeur, sélection mise en évidence, radios
  techniques masquées, descriptions courtes par espace.

## Validation

- `py_compile` sur `app.py`, `navigation.py` et `three_pillars_tab.py` : succès ;
- 4 tests unitaires du contrat de navigation : succès ;
- démarrage Streamlit shadow dans le conteneur live sur le port 8502 :
  `health=ok` ;
- déploiement sur la source réellement montée sur `/app`, puis redémarrage du
  seul conteneur `root-trading-dashboard-1` ;
- tests Streamlit `AppTest` avec les données live : aucune exception pour les
  espaces Actions / ETF, Pilotage global et Forex ; titres et options attendus
  confirmés ;
- trading Forex toujours indiqué comme désactivé ; aucun scoring, gate, ordre
  ou workflow n8n modifié.

## Déploiement live

Le `docker inspect` a montré que `/app` est actuellement monté en lecture seule
depuis :

```text
/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard
```

L'ancien miroir `/opt/trading-dashboard/app` n'est pas la source exécutée et
n'a pas été utilisé pour la publication.

## Rollback

Sauvegardes locales :

```text
.codex-tmp/dashboard_navigation_20260810_predeploy_live/
```

Sauvegardes VPS :

```text
/tmp/dashboard_navigation_20260810_predeploy_live/
```

Restaurer `app.py` et `three_pillars_tab.py`, supprimer
`app_modules/navigation.py`, puis redémarrer `trading-dashboard` depuis
`/docker/root`.
