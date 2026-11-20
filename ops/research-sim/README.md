# Recherche et portefeuilles simulés

Le collecteur lit les bases AG2, AG3 et Yahoo, le journal des décisions, les éléments Flex rapprochés et les publications des lots 3 à 6. Une collecte réussie lance un calcul hors réseau. Le dashboard lit uniquement les publications du calcul.

## Données et calculs

- Chaque photographie privée conserve les instruments, les signaux datés, les contrats, les observations de cotation et les références aux décisions. Les noms des fichiers incluent une empreinte du contenu. Les fichiers JSON compressés se conservent pour rejouer les revues.
- L’archive du 18 novembre fournit la première appartenance datée. Deux photographies successives donnent les entrées, sorties et changements observés. Les radiations et l’appartenance avant cette archive restent inconnues.
- Un achat Flex d’une séance antérieure atteste un accès historique au contrat. La permission actuelle reste inconnue. Les actions japonaises domestiques peuvent avoir un nominal indicatif pour 100 titres, même si le tarif de la place manque. [Unité de négociation des actions japonaises, JPX](https://www.jpx.co.jp/english/equities/trading/domestic/03.html)
- Les tarifs proviennent de `ops/economic-monitor/costs.py`. Pour un achat, le modèle croise l’ask. Pour une vente, il croise le bid. La convention réserve 0,5 % du nominal si la taxe d’achat reste inconnue et impute 3 points de base à chaque conversion. La liquidité consommée est plafonnée par le volume et, lorsqu’elle est connue, la taille affichée. Le délai de règlement compte deux jours de semaine. Ce délai ne modélise pas les jours fériés de règlement.
- La revue de 17 h 18 utilise la dernière photographie de l’heure précédente. Les observations de cotation du lot 4 sont jointes selon l’instant de **réception** et l’horodatage du prix. Les décisions et leurs limites sont fixées avant de consulter l’arrivée deux secondes plus tard. Une revue dépourvue de photographie amont apparaît comme manquante. Le taux de change provient d’une clôture antérieure, avec priorité au relevé Flex pour une même date.
- Les quatre portefeuilles partent du même capital fictif. Le classement neutre dépend de la semaine, du symbole et de la graine fixée. Le veto AG2 est appliqué dans deux variantes. Les frais, la rotation et la répartition sectorielle proviennent du livre de chaque variante. Un excédent sur un témoin requiert un prix de clôture dans la bonne devise pour chaque position, un change documenté et le témoin du même jour. Les premiers cours quotidiens clos et observés après la séance sont conservés. Les prix du jour de revue servent à la décision, les clôtures documentées au calcul comparatif. Les dividendes des positions fictives, les opérations sur titres et les coûts de fonctionnement restent hors du rendement publié.
- Les calendriers de place embarqués couvrent 2026 à 2028, y compris les demi-séances prévues et la pause de Tokyo. Régénérer les calendriers avant leur échéance ou après un changement officiel. Le moteur écarte les horaires inconnus.

## Installation et vérification

Dans la version exécutée, les modules Python de ce dossier et une copie de `ops/economic-monitor/costs.py` figurent sous `/opt/trader-ia/research-sim/current`. Le collecteur utilise le Python DuckDB du VPS. Le moteur utilise le Python système, un compte dédié et la racine isolée `/var/lib/trader-research-root`. Ses seules écritures métier vont dans `/var/lib/trader-research-sim` et `/local-files/duckdb/research_reporting`. Les unités systemd sont versionnées à côté du code.

`seed.py` initialise la photographie d’audit, les taux de change initiaux et les séances depuis les preuves privées, le rapprochement du lot 2 et `exchange-calendars`. Les fichiers produits `audit.json` et `sessions.json` sont en lecture seule pour le simulateur dans `/var/lib/trader-research-feed`. Le producteur conserve ensuite ses lots dans `batches/`.

```sh
systemctl status trader-research-feed.timer
systemctl show trader-research-feed.service trader-research-sim.service -p Result -p ExecMainStatus
journalctl -u trader-research-feed.service -u trader-research-sim.service --since today
```

`test_forward.py /var/lib/trader-research-feed` exerce une revue complète, un deuxième passage et la suspension d’une variante, dans un répertoire temporaire. Depuis un checkout, ajouter `ops/economic-monitor` au `PYTHONPATH` pour charger le barème. `check_dashboard.py /app/app.py` contrôle les trois vues depuis le conteneur Streamlit. Le champ `checks` de `report.json` conserve les tests sur les archives et sur les cas contrôlés. Le moteur teste l’interdiction des sockets IPv4 et Unix à chaque lancement.

## Suspension et retour

- L’historique des photographies reste consultable via le sélecteur de date. Le fichier de chaque révision figure dans `report.json`.
- Pour suspendre une variante à partir d’une date, publier atomiquement un objet JSON dans `/var/lib/trader-research-feed/pauses.json`, par exemple `{"ag3_veto":"2026-12-01"}`. L’historique des revues terminées reste immuable. Les avoirs d’une variante suspendue sont figés. Une reprise après suspension nécessite une nouvelle version de scénario pour comparer à nouveau des périodes continues.
- Pour suspendre l’ensemble, arrêter et désactiver `trader-research-feed.timer`, puis arrêter les unités de collecte et de simulation. Les bases du journal et les fichiers publiés restent en place.
- Le dashboard dispose d’une copie de son point d’entrée antérieur dans `/opt/trader-ia/deploy_backups/lots07-09-20261120/app.py`. Avant une restauration, vérifier les modifications apportées depuis cette version afin de conserver les autres livraisons.
