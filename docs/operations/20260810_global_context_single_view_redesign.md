# Refonte « Contexte de marché » en vue unique (2026-08-10)

## Problème traité

La page `Pilotage global → Contexte de marché` exposait neuf onglets et de
nombreuses tables techniques larges. La lecture métier des points de vue AG5 à
AG8 était noyée dans les colonnes de diagnostic.

## Vue retenue

`services/dashboard/global_context_tab.py` rend désormais une seule page :

1. un verdict global avec statut, fraîcheur, couverture et confiance ;
2. quatre cartes AG5–AG8 avec une phrase de synthèse, les signaux principaux et
   la qualité du composant ;
3. un guide visible qui explique le sens des couleurs, la couverture et la
   confiance ;
4. le dernier brief réellement journalisé par AG1 V4 : politique d'utilisation,
   devises pertinentes et composants inclus/omis ;
5. une explication de l'influence possible de chaque agent sur le raisonnement
   des trois LLM, avec les frontières consensus/gates/Risk Manager ;
6. une matrice visuelle sur les 12 devises publiées pour comparer macro,
   valorisation, positionnement et pression de taux.

Les tables, sous-onglets AG9, historiques et écrans de méthodologie ont été
retirés de l'interface. Les requêtes de diagnostic restent disponibles dans le
chargeur pour les tests et l'exploitation, mais ne sont plus exécutées à chaque
affichage de la page.

## Contrat fonctionnel

- aucune règle de scoring, aucun gate et aucun seuil métier n'ont été modifiés ;
- les scores sont ceux des vues DuckDB persistées et du `ag1_pack_json`
  canonique ; le dashboard ne fait que les trier et les représenter ;
- AG5–AG7 utilisent une échelle signée ; pour AG8 la couleur représente la
  pression sur la duration, dont une valeur haute est un risque ;
- les cartes ne montrent que les extrêmes les plus informatifs ; la matrice
  montre tout l'univers et marque `Non couvert` au lieu de confondre absence et
  neutralité ;
- les devises étiquetées `Brief AG1` proviennent du dernier
  `global_context_pack_json` de `core.runs`. Elles sont filtrées à chaque run à
  partir du portefeuille et des opportunités éligibles ;
- les textes parlent d'influence possible : le pack peut nuancer sélection,
  conviction et poids proposé, mais l'attribution causale exacte demanderait
  un replay A/B ;
- la page reste strictement consultative et n'agit sur aucun ordre ni workflow.

## Validation

- compilation Python et `git diff --check` : succès ;
- anciens tests de lecture DuckDB et de masquage des secrets : succès ;
- nouveaux tests de contrat : quatre cartes, matrice, absence de dépendance à
  `st.tabs` et `st.dataframe` ;
- validation dans `root-trading-dashboard-1` avec les bases live : 7 requêtes
  sans erreur en environ 0,32 s, 4 agents alimentés et 12 devises affichées ;
- snapshot vérifié après publication : `GC_20260810T140500Z_0d3d8b3f`, statut
  `OK`, couverture `90,8 %`, confiance `68,3 %`, fraîcheur `aging` ;
- dernier run AG1 vérifié : `RUN_20260810_163006_20933`, politique `CAUTION`,
  devises EUR/JPY/USD et détails AG5–AG8 tous `INCLUDED` ;
- tests unitaires, ancien jeu de régression, AppTest de la page candidate et
  AppTest du dashboard live complet : succès, sans exception ni alerte ;
- healthcheck Streamlit : `200 ok`.

## Déploiement live

Sources montées dans `/app` :

```text
/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/global_context_tab.py
/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app.py
```

SHA256 déployés :

```text
global_context_tab.py  56ec227105d14698c8192b4df1dccaebb14fefeb127eba9c6305f9c61a6b10b5
app.py                 312e2b8fea4b1e93a98bdee73c3bc447aa3b25e5386eec3b446efbb5f4ab5eea
```

Seul `root-trading-dashboard-1` a été redémarré. Aucun conteneur n8n, broker ou
producteur AG5–AG8 n'a été touché.

## Rollback

Sauvegardes VPS de la version précédente :

```text
/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/global_context_tab.py.bak_explained_20260810T144257Z
/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app.py.bak_global_context_explained_20260810T144257Z
```

Restaurer les deux fichiers, vérifier leur compilation, puis redémarrer le
dashboard :

```bash
cp /opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/global_context_tab.py.bak_explained_20260810T144257Z \
  /opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/global_context_tab.py
cp /opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app.py.bak_global_context_explained_20260810T144257Z \
  /opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app.py
python3 -m py_compile \
  /opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/global_context_tab.py \
  /opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/app.py
docker restart root-trading-dashboard-1
```

La sauvegarde locale préalable se trouve dans
`.codex-tmp/global_context_explanations_20260810_pre/`.
