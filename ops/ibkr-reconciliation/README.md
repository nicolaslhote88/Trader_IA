# Rapprochement privé des relevés IBKR

Le lot 02 importe les relevés **Flex Activité au format XML** sur le poste. Le programme conserve le document original dans SQLite, son SHA-256, les lignes de chaque section, les périodes et les décisions de retrait ou de réintégration. Chaque compte est lié à l’empreinte de l’identité vérifiée sur la Gateway. Les lectures du VPS fournissent des références. Elles utilisent le grand livre du courtier, des copies stables en lecture de DuckDB et la requête de consultation PortfolioAnalyst. Le service d’archive du lot 01 fournit la lecture des copies stables.

## Export depuis le portail IBKR

Dans **Performance & Reports → Flex Queries**, créer ou exécuter une requête **Activity Flex Query** pour le compte concerné. Choisir le format XML et des dates YYYYMMDD. Exporter du 1er mai au 18 novembre 2026 pour traiter le résidu de l’audit, puis compléter avec les dates ultérieures disponibles. Plusieurs relevés par périodes successives conviennent. Une période qui s’arrête après le 18 ne fournit pas automatiquement le pont de NAV à cette date. [Guide IBKR des requêtes Flex](https://www.ibkrguides.com/clientportal/performanceandstatements/flex.htm)

- Dans **Account Information**, inclure l’identifiant du compte et la devise de base. Dans **Change in NAV**, inclure toutes les composantes et les deux valeurs de borne. Conserver un seul mode de présentation du résultat. [Variation de NAV selon IBKR](https://www.ibkrguides.com/reportingreference/reportguide/changeinnav_fq.htm)
- Inclure **Cash Report** par devise, **Statement of Funds** au détail des mouvements, **Open Positions** au niveau Summary et **Trades** au niveau Executions. Sélectionner conid, sens, quantité, currency, tradeDate, settleDateTarget, tradeID, ibExecID, transactionID, commission, devise de commission, taux de change, codes et origTradeID. [Cash Report](https://www.ibkrguides.com/reportingreference/reportguide/cash%20reportfq.htm) · [Positions ouvertes](https://www.ibkrguides.com/reportingreference/reportguide/open%20positionsfq.htm)
- Inclure **Cash Transactions**, **Corporate Actions**, **Transfers**, les conversions de devises et les cours de conversion vers l’EUR. Garder les identifiants source, la date comptable et de règlement, les montants signés en devise native, le taux de change et les champs de correction. [Cash Transactions](https://www.ibkrguides.com/reportingreference/reportguide/cash%20transactionsfq.htm) · [Codes de correction](https://ibkrguides.com/reportingreference/reportguide/codes_flex.htm)

Le dossier privé `entrant` reçoit les XML. Les dépôts bruts, la base, les références et les résultats restent hors Git. Un PDF ou un CSV déposé est signalé dans le rapport. Aucune clé Flex Web Service n’est nécessaire.

## Commandes sur le poste

Avec Python 3.11 ou plus récent, passer le dossier privé à `run.py --state DOSSIER`. Pour l’initialisation, ajouter `--audit ARCHIVE_ZIP --ssh CONFIG_SSH`. L’option `--refresh` demande des observations fraîches au VPS. Un fichier individuel peut être passé avec `--file EXPORT_XML`. Le programme produit `rapprochement.html`, `etat.json` et les détails CSV de la génération. La copie des références précédentes reste consultable.

Pour retirer une version du calcul, exécuter `python run.py --state DOSSIER --withdraw SHA256`. Pour la remettre en service, remplacer `--withdraw` par `--restore`. La présence du fichier retiré dans le dossier entrant ne le réactive pas. Le document, ses identifiants et l’historique demeurent dans la base. Exécuter `python test_reconciliation.py` pour la réception isolée avec des données fictives.

## Lecture des contrôles

Le pont de NAV est présenté séparément pour chaque période. Les relevés dont les dates se recouvrent ne sont jamais additionnés. Pour un solde, le calcul choisit une présentation de la section pour la période. Pour un mouvement, il relie les identifiants stables et ordonne les révisions selon la génération du relevé. Les versions incompatibles sans chronologie certaine restent ouvertes. Un mouvement sans identifiant reste dans les pièces et dans l’export des mouvements ouverts. Les mouvements de fonds, les transactions et les rubriques de variation de NAV représentent plusieurs vues de la même activité.

Les quantités sont comparées exactement, contrat par contrat. Le pont de stock exige un inventaire d’ouverture et de clôture, les identifiants d’exécution et la couverture des transferts et opérations sur titres. Une photographie d’espèces ou de positions observée en séance affiche l’écart indicatif avec le relevé. Une clôture commune doit être établie pour conclure. Un frais en USD et un frais en EUR ne donnent aucun écart en devise native. Pour PortfolioAnalyst uniquement, un écart est qualifié d’arrondi lorsque l’arrondi au centime reproduit exactement la valeur du relevé.

Le résidu historique est attribué lorsqu’un pont Flex couvre la période jusqu’à la date de l’audit, en une pièce ou par périodes adjacentes avec continuité des soldes. La différence entre valeur instantanée et clôture est isolée. La contribution de marché est alors comparée au réalisé interne et au latent du diagnostic. Une donnée insuffisante conserve son montant ouvert.
