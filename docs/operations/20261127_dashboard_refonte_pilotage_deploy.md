# Dashboard de pilotage du 27 novembre 2026

Le VPS sert la révision `36e2ce40bea11054282a9e829caa7a12d62bac99` du dashboard. Le Cockpit est la page d’accueil. La décision inscrite au registre AG1, la matrice des opportunités, la comptabilité officielle et le mandat simulé ont chacun une destination distincte dans les cinq espaces.

## Version et montage

La branche de départ est `codex/lots13-15-pilotage`, révision `b339c6b`. La branche de livraison est `codex/dashboard-refonte-pilotage-20261127`. Le conteneur `root-trading-dashboard-1` monte `/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard` sur `/app`. Le dernier redémarrage de ce conteneur date du 27 novembre à 9 h 24, heure de Paris.

Les étapes de construction sont repérées par les révisions suivantes. Chaque révision restitue un état cohérent de l’interface. Pour retirer la dernière étape d’une livraison intermédiaire, préparer la révision précédente en shadow et vérifier ses routes avant la bascule.

| Étape | Révision | Contenu |
| --- | --- | --- |
| 1 | `6fe64b7` | Règles de lecture et tests des voyants |
| 2 | `f138c45` | Cinq espaces, Cockpit et accès aux anciennes vues dans les preuves |
| 3 | `adf46cc` | Compte, décomposition en euros et coûts |
| 4 | `3af4d1` | Laboratoire, chaîne de traitement et adaptation des recettes |
| Recette finale | `36e2ce4` | Étiquettes des courbes, cartes cliquables, retour par l’alerte, vérification des volets et captures |

## Sources vérifiées

Les six JSON de référence ont été copiés en lecture seule et comparés par empreinte avec leurs originaux. Le contrôle nominatif des champs, des dates, des montants et des empreintes se trouve dans `.codex-tmp/dashboard-refonte-20261127/controle_sources.json` sur le poste du projet.

| Sujet | Donnée effectivement disponible | Conséquence à l’écran |
| --- | --- | --- |
| Compte | La sonde vérifie l’identité et l’alignement du courtier. La collecte datée du pilote distingue le compte de simulation. La dernière valorisation rapprochée se trouve dans `core.portfolio_snapshot` pour un run de rapprochement. | Le bandeau indique la disponibilité du compte réel. Le montant rapproché et la clôture officielle portent chacun leur date. |
| Rendement | Les taux des séries sont des fractions. Le panier conservé commence à `performance.freeze`. Les commissions sont agrégées par mois dans `months[].fees`. | Le graphique convertit les fractions en pourcentages. La cascade compare son total à `after_documented` et affiche l’écart pour des fenêtres différentes. |
| Coûts | `provider_costs` fournit les montants mensuels. `provider_ranges` décrit la couverture des justificatifs. Le coût historique des ordres s’appuie sur un échantillon distinct. | Les mois partiels sont signalés. L’affichage de la comparaison des barèmes vérifie l’effectif et le total de l’ancienne estimation de référence. |
| Mandat | `reviews[].results[].nav` appartient aux portefeuilles exploratoires. Le mandat publie ses séances et ses résultats par fenêtre dans `formal`. Les NAV quotidiennes nettes comparables et certaines mesures cumulées de risque et de coût manquent. | Les mesures de qualification indisponibles ou immatures restent grises. Les rendements de fenêtres achevées et complètes peuvent être affichés. L’exploration garde son propre encart. |
| Votes | `historical_votes.rows` publie les moyennes par groupe. Les bornes statistiques de l’écart entre les deux groupes ne sont pas publiées. | Le point d’écart est affiché avec la mention de l’intervalle indisponible. |
| Sources et analyses | Les empreintes des publications, les dates d’écriture des actualités et les dates de mise à jour des signaux techniques et fondamentaux sont distinctes. Les pays de l’univers sont dans le fichier référencé par `census[].file`. | Les voyants utilisent les écritures datées. Le graphique par pays utilise les titres de la photographie sélectionnée. |
| Collectes sur 72 heures | Le recensement contient des observations datées. Le registre contient les valorisations du rapprochement. L’historique complet des écritures du collecteur d’expériences manque sur le montage du dashboard. | Les créneaux sont rapprochés une fois chacun avec les écritures disponibles. Le nombre prévu et l’absence de numérateur sont indiqués pour les expériences. Le calendrier correspond aux horaires vérifiés le 27 novembre. |

L’écran des sources couvre les publications d’actualités générales et de cotations qu’il nomme. Les achats simulables et la présélection de recherche utilisent les comptes du dernier recensement. L’entonnoir historique de tradabilité se consulte dans **Santé du système → Tradabilité**.

## Recette et exploitation

La recette a utilisé Streamlit 1.61.1 dans le conteneur du dashboard. Le shadow a servi sur le port 8502, puis la production a été vérifiée sur 8501. Le point `/_stcore/health` répond `ok` en interne. L’accès HTTPS externe sans authentification répond 401.

- La suite du dépôt compte 34 tests pytest réussis, dont 22 exécutables par découverte unittest. Le seul avertissement pytest concerne une dépréciation du greffon anyio.
- Le parcours AppTest ouvre les 17 pages. Le premier rendu du Cockpit en production a pris 3,90 s lors de la recette. L’ancienne page Forex utilise un message rouge Streamlit pour un signal chiffré de positionnement CFTC. Le contrôle reconnaît ce format précis et rejette les erreurs applicatives.
- Les recettes `ops/{verified-accounting,research-sim,experiments,pilotage}/check_dashboard.py` parcourent les vues détaillées. La recette économique de la nouvelle navigation est `ops/economic-monitor/check_dashboard.py`. L’ancien `check_app.py` cible l’interface antérieure. Les panneaux « Positions et ventes » et « Barème et taille des ordres » ont aussi été ouverts en production et leurs graphiques contrôlés.
- Le contrôle dégradé parcourt les huit écrans avec les publications absentes et un compte non confirmé. La suite de voyants couvre aussi un rapprochement en retard, une séance fériée, les limites de risque et un mandat à zéro séance.
- Le contrôle sur les fichiers réels retrouve le résultat économique publié à moins d’un centime. Le contrôle navigateur ouvre et referme les preuves de chacun des six écrans, vérifie la navigation par la carte du compte et le retour par le bandeau rouge.

Les six captures de production et les preuves de recette sont conservées sur le poste du projet dans `snapshots/pilotage_prive/dashboard-refonte-20261127/`. La galerie privée `CAPTURES.md` donne accès aux écrans Cockpit, Compte, Coûts, Test de 6 mois, Enseignements et Chaîne de bout en bout. Les copies financières et les captures sont exclues du dépôt public.

## Retour arrière

La sauvegarde du montage antérieur et le manifeste des fichiers livrés se trouvent sur le VPS dans `/opt/trader-ia/deploy_backups/dashboard-refonte-20261127/`. La commande de contrôle vérifie le montage courant et détecte toute édition intervenue après livraison.

```bash
python3 /opt/trader-ia/deploy_backups/dashboard-refonte-20261127/rollback.py --check
```

Pour rétablir la version antérieure, exécuter sur le VPS la commande suivante. Elle restaure les fichiers sauvegardés, retire les fichiers ajoutés à cette livraison et redémarre le seul conteneur dashboard.

```bash
python3 /opt/trader-ia/deploy_backups/dashboard-refonte-20261127/rollback.py --restore
```

[Spécification et emplacement des maquettes privées](../specs/dashboard_refonte_pilotage_v1/README.md)
