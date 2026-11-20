# Suivi du compte et des lots FIFO

La page **Actions / ETF → Portefeuille actions → Compte IBKR** lit le dernier relevé Flex validé, les positions et les exécutions rapprochées. La page **Pilotage global → État du système** affiche la date du relevé, la fraîcheur du registre et l’état de la passerelle. Le suivi interroge le broker en lecture seule et contrôle l’identité du compte avant et après la lecture de la valeur et des positions.

## Actualisation

La tâche Windows `TraderIA-Lot03-Publication` lit les imports actifs du lot 2 et vérifie les empreintes des références. Elle transfère le jeu de preuves réduit lorsque son contenu change. Sa source installée se trouve dans `snapshots/pilotage_prive/lot03/application`. Les nouveaux relevés continuent d’entrer par le dossier privé du lot 2.

Le timer VPS `trader-lot03-report.timer` lance le calcul aux minutes 08 et 38 UTC. Le service exploite une copie stable du registre DuckDB. Il remplace la publication dans `/local-files/duckdb/lot03_reporting/compte.json` après un calcul complet. En cas d’échec, le dernier rapport garde sa date originale. Le dashboard signale une publication de plus de 75 minutes et un relevé de plus de trois jours calendaires. Une session de dashboard ouverte relit les données toutes les minutes.

Chaque transaction comporte les devises de cotation, du compte et de la commission, le nominal natif, le change daté et la ligne Flex. Une transaction PortfolioAnalyst peut préciser le taux journalier lorsque l’identité, la date, le sens, la devise, le prix, la quantité et le montant signé concordent. Les frais sont convertis dans leur propre devise. Le FIFO répartit les commissions au prorata des titres cédés sans arrondir les montants intermédiaires. Les taxes sur transactions figurent séparément. Une vente sans stock couvert interrompt la publication. Un taux manquant laisse le montant en euros indéterminé. Une exécution du registre en attente d’un relevé figure dans une liste dédiée.

Le normaliseur FX historique du VPS et le réconciliateur PF alimentent toujours le registre opérationnel. Le normaliseur utilise notamment un taux de secours et PF peut reprendre le change courant et supposer la devise d’une commission. Le suivi comptable établit chaque correction sur justificatif. Les publications de ce lot servent à la lecture du dashboard.

## Contrôles et retour arrière

Sur le VPS, `systemctl status trader-lot03-report.timer` donne la prochaine collecte. `journalctl -u trader-lot03-report.service -n 12` donne le résultat du dernier calcul. Sur Windows, `Get-ScheduledTaskInfo TraderIA-Lot03-Publication` donne le résultat de la synchronisation. Le test `check_dashboard.py /app/app.py` s’exécute dans le conteneur Streamlit avec `PYTHONPATH=/app`.

Le sélecteur **Décisions des modèles et registre historique** ouvre immédiatement la lecture précédente. Pour un retour à la version logicielle antérieure, restaurer atomiquement la copie sauvegardée de `app.py` dans le bind mount actif du dashboard, puis redémarrer uniquement `root-trading-dashboard-1`. La réception privée indique le chemin exact de la sauvegarde et les empreintes. La collecte passive peut continuer pendant ce retour arrière.
