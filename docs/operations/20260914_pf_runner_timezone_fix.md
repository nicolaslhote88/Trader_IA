# PF : dépendance implicite aux fuseaux horaires — 14 septembre 2026

## Incident et impact validés

Les exécutions PF `22059` (11:15 Paris) et `22060` (12:15 Paris) échouent dans `PF.00C - Reconcile IBKR Ledger`, fonction `measured_snapshot_risk`, après le déploiement du contrat de performance. Le nouveau `SELECT total_value_eur, ts` récupérait un `TIMESTAMPTZ` : DuckDB 1.4.3 importe implicitement `pytz` pour construire l’objet Python correspondant. Ce module est absent des trois environnements Python 3.13 des task runners.

Le rejeu initial utilisait Python dans `root-n8n-1`, où cette dépendance était disponible. Ce test n’était pas représentatif de l’environnement réel d’exécution du nœud. La publication active et le test du dashboard ne pouvaient pas détecter ce défaut.

La transaction du nœud a été annulée à chaque échec. Aucun run ni snapshot de réconciliation des créneaux défaillants n’a été écrit ; le dernier relevé disponible lors du diagnostic était celui de 10:15 Paris (`22056`). Le workflow de valorisation ne transmet aucun ordre. Il s’agit d’un arrêt du rafraîchissement, sans écriture partielle du portefeuille.

## Correction déployée

La date est retournée comme `CAST(ts AS VARCHAR)` ; les comparaisons de dates et l’agrégation des flux restent dans DuckDB. Cette représentation conserve le fuseau et évite la conversion Python qui exigeait `pytz`. Les formules, seuils et crons sont inchangés.

Fichiers synchronisés :

- `agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/nodes/00c_reconcile_ibkr_ledger.py`
- `agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/AG1-PF-V1-workflow.json`

Le builder lit déjà ce fichier source. Aucun ajout de paquet, changement de configuration des runners ou modification des autres workflows n’a été nécessaire.

## Validation

- Rejeu de l’entrée exacte de `22060` sur une copie de la base, dans **chacun des trois runners**, avec leur `TaskExecutor`, leur sandbox et leur configuration d’imports : ancienne version en erreur `pytz` trois fois ; version corrigée `WRITTEN`, dix positions, trois fois.
- Dix tests PF passent. Les nouveaux cas vérifient le snapshot avec fuseau, les flux comptabilisés, l’absence de référence et la NAV précédente nulle. Un sous-processus interdit explicitement l’import de `pytz` pour que la régression soit détectée même sur une machine où il est installé.
- Un seul nœud modifié dans la version publiée. Workflow actif, `versionId=activeVersionId=3fc5b6bd-1571-4373-9b91-26282be72fab`, contenu publié vérifié ; les 179 autres workflows sont inchangés.

À la clôture de cette correction, le prochain cron PF de production est **13:15 Paris**. Son résultat n’a pas encore été observé ; les succès ci-dessus sont les rejeux sur copie dans les runners réels. Les créneaux manqués ne sont pas reconstruits avec des prix ultérieurs.

Commande locale des tests :

```powershell
& .codex-tmp/performance_audit_20260914/venv/Scripts/python.exe -m pytest 'agents/trading-actions/AG1 - Portfolio manager/AG1-PF-V1/tests' -q --disable-warnings
```

## Sauvegardes et retour arrière

Exports, entrée en erreur et reçus de validation restent privés sous `/opt/trader-ia/.codex-tmp/pf-timezone-20260914/`. Copie avant rejeu : `/local-files/.codex-tmp/pf-timezone-20260914/ag1_before.duckdb` ; aucune restauration de base n’a été nécessaire.

Le fichier `pf_export_before.json` contient la version précédente. Un retour arrière de ce correctif réintroduirait l’erreur : ne le faire que si une autre correction est prête. Pour restaurer un workflow, attendre la fin des exécutions partagées, importer seulement cet ID, republier et redémarrer n8n/runners ; vérifier ensuite la version active. Ne jamais remplacer une base courante par la copie de rejeu.
