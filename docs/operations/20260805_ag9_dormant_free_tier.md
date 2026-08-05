# AG9 World Monitor — mise en sommeil (offre gratuite)

Date : 2026-08-05  
Décision : Nicolas ne souhaite pas souscrire à une offre payante World Monitor pour l'instant.

## Motif validé

- L'offre gratuite permet des lectures publiques dans le navigateur, mais ne donne pas accès au
  serveur MCP : l'autorisation retourne `401 INSUFFICIENT_TIER` pour un compte gratuit.
- AG9 est un consommateur automatisé serveur-à-serveur. La découverte gratuite du catalogue
  décrit les capacités, mais ne fournit pas les données nécessaires au calcul d'un signal.
- Simuler une origine navigateur ou contourner le contrôle d'accès est exclu.

Conclusion : aucun mode gratuit officiel n'est suffisamment exploitable pour alimenter AG9 de
façon fiable et automatisée dans Trader_IA.

## État appliqué sur le VPS

- Conteneur shadow `ag5ag9-worldmonitor-shadow` : arrêté (`state=exited`).
- Politique de redémarrage : `restart=no`.
- Workflow n8n `AG9GLOBALRISK20260805` : conservé non publié et inactif.
- Valeur par défaut `WORLD_MONITOR_ENABLED=false` : inchangée.
- Base shadow World Monitor et code conservés pour une reprise future.
- Aucun impact sur AG5–AG8, le synthétiseur, AG1 V4 live, le broker ou les ordres.

## Réveil éventuel

Ne réveiller AG9 qu'après décision explicite de Nicolas et disponibilité d'un accès officiel :

1. configurer `WORLD_MONITOR_API_KEY` hors Git ;
2. démarrer uniquement le conteneur shadow ;
3. valider l'authentification et les quotas sans afficher la clé ;
4. observer plusieurs cycles réels et contrôler couverture, fraîcheur et déduplication AG4 ;
5. exécuter le shadow AG1 avec capture LLM ;
6. publier/activer les workflows uniquement après revue des résultats et accord explicite.

Références officielles :

- <https://www.worldmonitor.app/docs/mcp-quickstart>
- <https://www.worldmonitor.app/docs/usage-auth>
