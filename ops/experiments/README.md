# Expériences sur les sorties, les nouvelles et les avis IA

Ces trois expériences utilisent les données des lots 1 à 9. Elles ont leurs propres publications, leurs preuves et un budget de test distinct. La règle de vente peut être étudiée dans les décisions détaillées même après épuisement du budget des appels.

## Sorties

Le jeu de départ est l’audit arrêté le 18 novembre 2026. La convention d’évaluation emploie la première clôture ajustée après la proposition, puis la vingtième clôture suivante sur le titre. Le calcul reproduit les rendements relatifs publiés pour chacune des 64 propositions. Les comptes théoriques sont des comparaisons individuelles à partir de la position de la veille. Les prix intrajournaliers, les positions exactes pendant la séance et le spread de l’époque ne sont pas disponibles pour la plupart de ces propositions.

La première raison applicable est retenue selon l’ordre risque, liquidités, faiblesse fondamentale, remplacement. Il s’agit du motif de la politique testée. Les données insuffisantes pour justifier une sortie apparaissent dans la fiche. Si la quantité d’une réduction manque, le modèle teste la moitié de la position documentée de la veille, arrondie à l’unité inférieure. La conservation et la bande de 3 % de la NAV respectent toutes deux les sorties obligatoires. Le point bas mesure l’écart au capital initial sur la position considérée. Les observations se recouvrent parfois, y compris sur un même titre.

Le remplaçant provient des achats approuvés au même passage, départagés par nombre de votes puis par symbole. Il doit entrer dans le cash disponible après deux jours de semaine, dans les plafonds de ligne et de secteur et dans la limite de six positions. Une preuve antérieure du contrat et du lot, ou d’un ordre accepté de taille identique, est nécessaire pour chiffrer un remplacement historique. Les sessions détaillées empruntent le moteur d’exécution du lot 8 avec sa règle de 60 secondes.

## Nouvelles

Le collecteur archive les lignes de source, leurs versions, les titres, les contenus et les heures de publication, de première observation et d’analyse. Le dossier privé du VPS conserve les matières brutes. Les fichiers immuables sont découpés par journée et empreinte. La publication contient les événements, les liens aux sources et les motifs de sélection. Une mise à jour postérieure à la date évaluée prive la reconstitution de cette version. Une correction n’efface pas une photographie déjà archivée.

La reconstitution du canal quantitatif reprend le plafond de 6 000 lignes de R8 avec compteurs et sommes d’impact à 7 et 30 jours. Le digest qualitatif de référence montre les trois entrées analysées les plus récentes, et le pack historique des trois modèles conserve le texte effectivement transmis. La reconstitution applique une borne de disponibilité à la date de décision et une exclusion des publications futures. Les dates antérieures à la conservation des versions peuvent ainsi présenter des écarts avec R8. Le pack resserré sélectionne des événements sur 14 jours puis au plus trois par titre. Les identifiants distinctifs dans les URL de syndication sont conservés. Les résultats sur l’échantillon annoté s’appliquent aux titres examinés.

## Avis IA et dépenses

Les paramètres et les annotations initiaux figurent dans `protocol.json` et `labels.json`. Le protocole d’appels autorisé par le propriétaire est dans `paid_protocol.json`. Un avis archivé et le consensus ne sont appariés que si l’empreinte du contenu économique est identique et si les trois réponses sont valides. Les instructions propres à chaque fournisseur et les empreintes complètes de ses entrées restent identifiées séparément.

Les variantes repartent à chaque session des mêmes positions et liquidités. Les gardes communes précèdent la comparaison. Les avis et les instructions exécutables restent consultables séparément. Les P&L à vingt séances requièrent des clôtures dans la devise exacte. Les frais du modèle sont imputés au niveau de la session, car un appel peut traiter simultanément achats et ventes. Une facture globale ou une date sans barème attribuable laisse le résultat après IA ouvert. Les coûts de fonctionnement mutualisés restent dans le suivi du lot 5.

Les essais appariés sur les sources et le contexte utilisent un dossier compact, le même modèle et quatre conditions, dont un deuxième passage sur le dossier identique. L’identifiant d’article Finnhub a changé le contenu de trois entrées, enregistrées séparément. Les contrôles sont réutilisés uniquement si leur empreinte concorde. Le coût est issu de l’usage retourné par Anthropic et du tarif standard documenté. [Tarifs Anthropic](https://platform.claude.com/docs/en/about-claude/pricing)

Le processus de calcul systemd est hors réseau. Les appels payants utilisent un workflow n8n manuel inactif, avec un déclencheur manuel et uniquement des requêtes HTTPS vers l’API Messages d’Anthropic. Le budget enregistré dans `/var/lib/trader-experiment-budget/budget.sqlite` réserve les appels avant lancement, y compris en cas d’interruption. Une réservation incertaine empêche une nouvelle facturation automatique. Le minuteur de collecte ne lance aucun appel IA.

## Exploitation et retour

Les deux services sont `trader-experiments-feed.service` et `trader-experiments.service`. Le minuteur `trader-experiments-feed.timer` collecte à 12 h 32, 14 h 32 et 20 h 32 chaque jour, et à 17 h 28 du lundi au vendredi, heure de Paris. Les rapports se trouvent dans `/local-files/duckdb/research_reporting/experiments`. Le registre de recherches et les quatre portefeuilles du lot 9 ont leurs propres unités.

```sh
systemctl list-timers trader-experiments-feed.timer
systemctl show trader-experiments-feed.service trader-experiments.service -p Result -p ExecMainStatus
journalctl -u trader-experiments-feed.service -u trader-experiments.service --since today
```

Pour suspendre les trois expériences, arrêter et désactiver leur minuteur, puis arrêter les deux unités. Le sélecteur du dashboard permet de relire les dossiers antérieurs et le pack d’origine. Avant de restaurer une version de son point d’entrée, comparer les changements livrés depuis la réception. Les preuves privées, les sources et les réservations restent conservées.
