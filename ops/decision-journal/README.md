# Journal privé des décisions AG1

Le collecteur archive les exécutions terminées d’AG1 et les enregistrements du registre. Le service systemd a un utilisateur dynamique, un réseau privé et deux entrées locales en lecture seule. Il interroge les tables n8n avec SQLite en mode lecture. Une copie temporaire stable de DuckDB est vérifiée avant ouverture. Un WAL DuckDB non vide ou une modification du fichier pendant la copie diffère le rapprochement au passage suivant. Les messages n8n déjà acquis restent archivés.

## Exploitation

Le code installé est sous `/opt/trader-ia/decision-journal/current`. La révision Git du collecteur figure dans `release.conf` et dans le statut. La référence publiée de chaque graphe comprend son identifiant de version, sa topologie et les empreintes des paramètres de ses nœuds. Les paramètres exécutés sont comparés aux paramètres publiés en tenant compte des valeurs par défaut ajoutées par n8n. La révision du collecteur et la version n8n sont deux références distinctes.

Le répertoire privé `/var/lib/trader-ag1-journal` contient la base `journal.sqlite`, les fiches consultables depuis `fiches/index.html` et le dernier contrôle réussi dans `status.json`. Chaque objet est compressé et identifié par SHA-256. Une exécution partielle supplémentaire ou une correction du registre crée une nouvelle révision, accessible dans les tables `refs`, `revisions` et `objects`. Pour vérifier si une collecte a échoué depuis ce statut, consulter aussi systemd.

```sh
systemctl status trader-ag1-journal.timer trader-ag1-journal.service
journalctl -u trader-ag1-journal.service --since today
sudo cat /var/lib/trader-ag1-journal/status.json
```

La collecte passe aux minutes 04 et 34. Une relecture est permise à tout moment avec `systemctl start trader-ag1-journal.service`. Une version déjà connue occupe une seule entrée. Lors du premier passage naturel après la livraison, le collecteur relit la trace à la source et reconstruit le registre sur une deuxième copie. Le contrôle requiert des empreintes identiques et aucun doublon. Il consigne les écarts de version entre le modèle demandé et le modèle déclaré. La fiche donne la provenance du texte ou de la sortie structurée lorsqu’un fournisseur renvoie une réponse par outil.

Les relevés d’entrées ont un SHA-256 calculé avant masquage des identifiants sensibles. Les clés d’accès, les en-têtes HTTP et les blocs de raisonnement du fournisseur sont exclus. Les messages et réponses retenus restent des données financières privées. Les vues HTML contiennent ces données et doivent suivre les mêmes règles d’accès que la base.

## Conservation et reprise

Conserver les preuves au moins **400 jours à compter de leur acquisition**, afin de couvrir les essais à six mois et une revue annuelle. Aucune purge automatique. Réexaminer le volume à l’issue de cette période et lors d’une alerte de capacité. Le budget SQLite est de 1 Gio. Le statut demande une revue dès 768 Mio de base, 1,5 Gio de fiches ou moins de 2 Gio libres. Garder de la place pour une copie temporaire de DuckDB. Les fiches sont régénérables. Les preuves n8n expirées à la source exigent une sauvegarde privée du journal pour être restaurées.

Pour exporter la base en service, employer l’API de sauvegarde SQLite, vérifier `PRAGMA integrity_check` et le SHA-256 de la copie. Conserver les exports et la référence d’audit hors Git. Pour vérifier un audit gelé, `audit_baseline.py archive.zip reference.json` contrôle les empreintes des deux pièces utilisées avant de rapprocher le triplet séance, titre et sens. Le total historique obtenu suivant cette règle peut différer d’un décompte prospectif par identifiant broker.

Pour interrompre la collecte, exécuter la commande suivante sur le VPS. La base et les fiches restent consultables. Pour reprendre, réactiver le minuteur avec `systemctl enable --now trader-ag1-journal.timer` et démarrer le service une fois. Sa reprise récupère les traces encore présentes dans n8n et les modifications du registre.

```sh
systemctl disable --now trader-ag1-journal.timer
systemctl stop trader-ag1-journal.service
```

## Vérification avant installation

Exécuter `python test_journal.py` puis lancer deux collectes en environnement isolé avec un même répertoire privé et la seconde option `--replay`. Vérifier les compteurs de références, de révisions et d’objets. La simulation d’une réception historique utilise une copie jetable de la base d’archive. La date d’installation enregistrée en production marque le début de l’observation prospective.
