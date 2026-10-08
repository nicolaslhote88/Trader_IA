# AG1 — historiques comptables intégrés en réel, 4 octobre 2026

## Décision et état validé

Nicolas a explicitement demandé le déploiement réel malgré l'absence de gain
de performance démontré par les simulations. Cette promotion porte sur les faits
comptables datés : évolution des marges, cash-flow et solidité financière.
Les probabilités du modèle prédictif restent expérimentales et non consommées.

Vérification finale le **2026-10-04 à 09:07 Paris** : `AG1V4CONSENSUS` actif,
`versionId = activeVersionId = 736b0a41-2f55-4153-aa97-3babbcbcab43`.
Le graphe publié correspond exactement au candidat rejoué. L'import n8n a attribué
ce nouvel identifiant de version ; la comparaison porte donc sur les nœuds et liens.
Export publié reflété dans `AG1_workflow_v4_consensus.json`.

## Circuit réellement publié

Après `AG1.V4 — Liquidity Preflight`, deux nouveaux nœuds précèdent les trois
modèles et la branche de contexte de `Merge Model Proposals` :

1. `AG1.HISTORY — Fetch Dated Accounts` appelle
   `POST http://ag3-predictive:8084/ag1/historical-evidence`, avec les symboles
   opportunités/détentions et `run.timestampUtc`. Aucun compte ou ordre n'est envoyé.
2. `AG1.HISTORY — Attach Dated Accounts` valide le contrat, les symboles et dates,
   puis ajoute les fiches aux entrées communes des modèles.

Champs : `opportunity_pack.rows[].fundamentals.historical_accounts`,
`held_historical_accounts`, `historical_accounts_legend` et
`historical_accounts_delivery`. Les faits restent consultatifs ; les modèles
peuvent les utiliser dans leurs propositions. Aucun poids, score, gate, prompt,
modèle, horaire ou garde broker existant n'a changé. Les quatre liaisons sortantes
du preflight sont conservées derrière l'attachement. Le contexte fondamental
`AG3_EVIDENCE_V1` existant est préservé.

API limitée à 100 symboles et dates avec fuseau non futures. Trois exercices
comparables au maximum, publication qualifiée obligatoire, données non datées
et révisions futures exclues. Timeout HTTP de 8 secondes : erreur ou réponse
invalide donne `UNAVAILABLE_OR_INVALID` et des fiches `SOURCE_UNAVAILABLE` vides,
sans altérer le contexte de trading initial ni inventer des faits.

Image : `trader-ag3-predictive:20261004-history-live`. La santé distingue
`historical_evidence_live_enabled=true` de `decision_enabled=false` pour le
modèle prédictif. `AG3_HISTORY_LIVE_ENABLED=1` dans le `.env` privé est une
métadonnée de déploiement/affichage, **pas un interrupteur de la route batch**.
Le retrait effectif de l'intégration exige le retour arrière du workflow.
Le dashboard affiche la fiche comme intégrée aux trois modèles, avec gain non démontré.

## Preuves et limites

- 21 tests Python et 7 cas JavaScript réussis : dates, absences, validation du
  contrat, préservation du contexte et rejet d'injections de configuration/ordres.
- API isolée sur copie SQLite : trois périodes GOOGL, données DSY non datées
  exclues, dates invalides/futures rejetées avec HTTP 422.
- Replay n8n isolé des deux nœuds exacts et des trois modèles : trois sorties
  `OK_OBJECT`, même contexte enrichi, couverture 11/18 sur le contexte du 2 octobre.
  Erreur HTTP réelle également rejouée, avec contexte indisponible explicite.
- Test dashboard AppTest réussi après déploiement ; sources locales/déployées
  identiques par SHA256. Six runners JS/Python enregistrés après redémarrage.
- 179 autres workflows inchangés. Comptages et empreintes des tables métier
  inchangés : 783 runs, 173 ordres, 124 fills, 7 585 positions_snapshot,
  783 portfolio_snapshot. Broker authentifié et aligné, zéro approbation en attente.
- Aucun ordre envoyé et aucun lancement manuel du workflow de trading.

Les mesures détaillées et empreintes sont dans
[`20261004_ag1_historical_evidence_live_proof.json`](20261004_ag1_historical_evidence_live_proof.json).
La simulation antérieure reste décrite dans
[`20261004_ag1_historical_evidence_shadow.md`](20261004_ag1_historical_evidence_shadow.md).
Elle ne démontre pas un meilleur rendement. Le replay vérifie le fonctionnement
du raccordement, pas sa rentabilité.

**Observation restante :** premier cycle automatique prévu lundi **5 octobre 2026
à 17:10 Paris**. Vérifier dans l'exécution publiée le statut de livraison, la
couverture et les trois entrées modèles. Ce cycle n'a pas encore été observé.

## Sources et retour arrière

Builder : `outils/scripts/build_ag1_history_live.py`. Code d'attachement :
`workflow/nodes/agent_input/historical_evidence_attach.code.js` dans le dossier
AG1-V4-Consensus Portfolio manager. Service : `services/ag3-predictive/`.
Dashboard : `services/dashboard/predictive_detail.py`.

Sauvegardes sur VPS : `/local-files/.codex-tmp/ag1-history-live-20261004/`.
`published.json` contient l'ancien graphe publié, version
`87de669d-ae38-4c50-9247-492bbf11f0a3` ; `backup/` contient les sources et
`research.env` privé. Copie locale du graphe sous `.codex-tmp/`.

Après vérification qu'aucune exécution n'est active, retour arrière du workflow
sur le VPS, sans exécuter celui-ci :

```bash
docker exec -u root root-n8n-1 chmod 644 /files/.codex-tmp/ag1-history-live-20261004/published.json
docker exec root-n8n-1 n8n import:workflow --input=/files/.codex-tmp/ag1-history-live-20261004/published.json
docker exec root-n8n-1 n8n publish:workflow --id=AG1V4CONSENSUS
docker restart root-n8n-1 root-task-runners-3 root-task-runners-4 root-task-runners-5
```

Vérifier `active=1`, `versionId=activeVersionId` et l'ancien graphe dans
`workflow_history` (l'import peut générer un autre UUID). Pour revenir aussi aux
sources précédentes, restaurer `api.py`, `historical_evidence.py`, `compose.yml`
et le `.env` privé depuis `backup/` vers `/opt/trader-ia/services/ag3-predictive/`,
avec mode 600 pour `.env`. L'image précédente `trader-ag3-predictive:20261004-history`
est conservée. Recréer le service avec le compose restauré :

```bash
docker compose --env-file /opt/trader-ia/services/ag3-predictive/.env -f /opt/trader-ia/services/ag3-predictive/compose.yml up -d --no-build ag3-predictive
```

Restaurer `backup/predictive_detail.py` dans le montage dashboard vérifié
`/opt/trader-ia/releases/ag5-ag8-global-context-20260805/services/dashboard/`,
puis redémarrer `root-trading-dashboard-1`. Ne restaurer aucune base métier.
Relire les santés, les approbations et les invariants métier après retour arrière.
