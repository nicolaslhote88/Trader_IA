# News : flux gratuits et DeepSeek Flash — 27 septembre 2026

## Résultat déployé

Autorisation utilisateur : corriger les défauts de l’audit, compléter avec des flux de news **gratuits uniquement**, remplacer Grok par DeepSeek Flash. Publication vérifiée le 27/09/2026 ; premier cycle automatique complet du lundi 28/09 encore à observer.

- **Macro AG4-V3** : `deepseek-v4-flash`, credential DeepSeek existant, chaîne LLM et parseur structuré identiques au principe des AG4 spécialisés. Branche `reduced` maintenue ; branche historique `full` et Forex non activés. Les erreurs du fournisseur arrêtent l’exécution : elles ne deviennent plus `Noise / impact=0`. La calibration sectorielle existante est conservée.
- **Provenance et reprise macro** : source du registre RSS conservée ; extraction du domaine sans dépendance à `URL` dans le sandbox. Réanalyse des sorties d’anciens taggers encore récentes (72 h), des titres modifiés et des dates de nouvelle publication >12 h sur une même URL. L’âge du dernier calcul remplace l’âge de première découverte pour le rafraîchissement. Un doublon inchangé conserve sa date précédente. Pas de réécriture massive de l’historique.
- **RSS officiels ajoutés** : Dassault Systèmes `DSY.PA` et Fast Retailing `9983.T`. Émetteur et symbole sont explicites ; pas de rapprochement flou. Source `issuer_rss` dans le staging existant, puis analyse et écriture par le workflow Finnhub. Fenêtre 14 j, maximum 6 articles par flux ; dates absentes, futures ou trop anciennes exclues. Un PDF sans corps disponible reste un titre explicitement signalé comme tel au modèle.
- **Finnhub gratuit** : segments `HELD,CORE_MANUAL,CORE_AUTO`, même cap 12 articles/symbole, fenêtre 2 j, cadence 1,1 s entre requêtes. Clé `source + article_id + symbol`, avec dédoublonnage compatible avec les anciennes clés. Les associations multi-émetteurs sont préservées ; l’historique ancien n’est pas reconstruit au-delà de la fenêtre collectée.
- Les tickers locaux sans mapping US sont consignés comme lacunes de couverture et ne déclenchent plus de requêtes 403 répétées. Les erreurs sur une requête effectivement supportée restent `PARTIAL` et observables. Les collecteurs écrivent uniquement en staging, sous DuckDB **1.4.3**.
- Le chargement du staging est désormais en lecture seule ; aucune exception SQL n’est transformée en liste vide. Les articles déjà présents en historique et ceux hors fenêtre sont exclus. Priorité aux deux sources officielles, limite totale 400. Le `CHECKPOINT` a été retiré du chemin Finnhub. La purge est bornée et centralisée dans la transaction du collecteur.
- Les trois parseurs single-stock refusent eux aussi une sortie LLM invalide. Les analyses valides conservent leur contrat de sortie. Les métadonnées de temps des nouveaux flux sont enregistrées dans `published_at_raw` ; IBKR le faisait **déjà**. L’audit a été précisé sur ce point.
- **Supervision** : succès/fraîcheur de Boursorama et macro, dernier run macro partiel/échoué, runs inachevés, dates futures, provenance inconnue, ancienneté du staging, statut des deux collecteurs et absence prolongée d’articles IBKR. Table technique `news_collection_runs`, rétention 60 j. Aucun message de test envoyé à Telegram.
- Les deux feeds FXStreet qui renvoient HTTP 403 sont désactivés dans le registre : **20 feeds macro actifs**. Aucune source payante ni souscription nouvelle. Les appels LLM utilisent le compte DeepSeek déjà configuré ; leur facturation reste celle de ce compte.

## Horaires publiés — Europe/Paris, lundi à vendredi

| Composant | Avant | Après |
|---|---|---|
| Collecteur Finnhub hôte | 09/12/15 **UTC**, soit 11/14/17 Paris en été | **09:35/12:35/15:35 Paris**, avec RSS officiels après Finnhub |
| Analyse staging Finnhub | 10/13/16 Paris | Inchangé |
| Boursorama | 08:05/11:05/14:05/17:05 | **08:05/11:05/14:05/15:05** |
| Macro | 06:45/10:45/18:45 | **06:45/10:45/14:45** |
| Health Alert | 16:30 | **16:55** |
| AG1 PM | 17:10 | Inchangé |

Le cron hôte appelle le wrapper à `35 * * * 1-5`. Le wrapper contrôle l’heure en `Europe/Paris` et suit donc le changement d’heure ; `flock` empêche deux collectes concurrentes. Le dernier Boursorama peut encore chevaucher brièvement la collecte de 15:35 : transactions courtes et retry sur verrou jusqu’à 120 s. La marge de 145 min entre macro et AG1 est supérieure aux durées historiques de 89–92 min ; le cycle complet DeepSeek doit encore être mesuré.

## Faits validés

- **19 assertions JavaScript** : rejet des sorties absentes/malformées/erreurs fournisseur, calibration conservée, provenance, doublons, changement de titre, reprise des anciens taggers, contrat des trois parseurs.
- **3 tests Python** : dates/URL relatives/contenu absent, clé multi-émetteur et insertion idempotente compatible avec l’ancien historique.
- **n8n isolé**, même image `root-n8n`, sans volume de production ni nœud de trading : 6 articles réels analysés par DeepSeek, plus 1 scénario synthétique directionnel. Six résultats réels neutres après calibration sectorielle, avec sorties structurées valides ; le scénario synthétique donne un impact 8 et prouve que le chemin n’est pas bloqué à zéro.
- Deux articles RSS réels analysés dans le même environnement isolé : `DSY.PA` impact +2, `9983.T` impact 0 (notification de conseil, sans contenu financier). Les deux sont présents dans `news_analyzed` sur la copie ; date source brute et qualité `PROVIDER_REPORTED` conservées. Ce sont des preuves de transport/contrat, pas une preuve de valeur prédictive.
- Chargement, historique et santé testés via **TaskExecutor dans les trois runners réels**. Écriture des deux résultats sur la copie testée dans chacun ; répétition sans doublon. Aucun ordre invoqué.
- Import des **5** workflows, publication, redémarrage n8n + runners 3/4/5. Relire `workflow_history` confirme l’égalité exacte des nœuds/connexions/réglages attendus et `active=1`, `versionId=activeVersionId`.
- **175 autres workflows inchangés**, dont AG1, AG2 et les workflows FX. Comptes et empreintes des 7 tables AG1 inchangés : 732 runs, 164 orders, 116 fills, 7 199 positions snapshots, 732 portfolio snapshots, 1 cash ledger, 60 lots. Empreintes dans le [reçu JSON](20260927_news_free_flash_evidence.json).
- `/healthz` n8n = `ok`, broker authentifié, zéro approbation en attente. Le décalage préexistant entre compte configuré live et session gateway paper est inchangé ; aucune action sur la connexion ou les gardes.
- **Collecte en production le 27/09 à 16:19 UTC** : Finnhub `SUCCESS`, 75 symboles examinés, 380 lignes reçues, 343 nouvelles associations insérées ; 12 symboles locaux non supportés consignés. RSS officiels `SUCCESS`, 2 articles reçus et 2 insérés. Ces lignes attendent le prochain cron d’analyse en production ; les résultats du shadow n’ont pas été promus artificiellement.

## Versions publiées

| Workflow | Version active |
|---|---|
| AG4-V3 macro | `7de5783c-9bf0-45b8-8ff9-b939f4389132` |
| Finnhub / flux gratuits | `6d30f5d2-3ecf-4acf-b257-190877d5e910` |
| IBKR | `2652228e-d057-468a-a254-d1fbd6c6b359` |
| Boursorama | `d08c6f67-4f36-4d86-ae7c-5db5e433ff9a` |
| Santé | `3a2ffd1f-f3dc-40f8-9050-defc4e8aedc4` |

Les exports du dépôt reflètent ces versions publiées. Les états de rotation et buffers `staticData` ont été préservés lors de l’import et ne sont pas versionnés dans les exports. Le générateur macro historique qui reconstruisait Grok a été remplacé par un rechargement ciblé du graphe vérifié ; les builders spécialisés réutilisent les nouveaux fichiers de nœuds.

## Sauvegardes et retour arrière

VPS : `/local-files/.codex-tmp/news-free-20260927/backup/` contient les deux bases news avant intervention, les cinq exports de workflows avec état, le collecteur/wrapper précédents et le crontab. Copies DB vérifiées identiques par SHA-256 avant écriture :

- `ag4_v3.duckdb` : `ce74655257634722c6f56900af0d7632cdaec1cf7c7b5355752cf0b470cbb6fb`
- `ag4_spe_v2.duckdb` : `375bd83ba1a8031ef185f22a091f88c3e6945bce39feca03c8ca89b556c00a94`

Rollback ciblé après vérification d’absence d’exécutions actives : réimporter l’export du workflow concerné puis `n8n publish:workflow --id=<id>`, redémarrer n8n et les trois runners, vérifier la version active. Restaurer les anciens scripts si nécessaire et remplacer **seulement** la ligne du cron Finnhub par celle sauvegardée ; ne pas écraser des changements ultérieurs du crontab partagé. Pour FXStreet, réactiver seulement les deux `source_id` consignés par `outils/scripts/news_free_20260927_maintenance.py` si leur accès redevient valide.

Les nouvelles lignes news peuvent être conservées ; aucun rollback de ledger requis. Ne restaurer intégralement une DB news qu’après analyse des écritures intervenues depuis la sauvegarde. Revenir à Grok réintroduirait l’incident de crédits constaté : privilégier un retour ciblé au composant défaillant.

## Limites et observation restante

- **SEC non activée** : API gratuite sans clé, mais contrôle VPS de cette intervention = HTTP 403 pour les submissions testées et le fichier de correspondance. Aucun contournement ni offre payante retenu.
- RSS IR accessibles pour deux des six lacunes CORE_AUTO ; ce déploiement ne prouve pas une couverture régulière de Keyence, Tokyo Electron, ABC Arbitrage ou Planisware. Finnhub reste dominé par Yahoo. Les douze tickers sans mapping sont consultables dans `news_collection_runs.errors`.
- Lundi 28/09 : observer les crons réels, la durée macro, la vidange du staging avant AG1 et les éventuels verrous. Aucun résultat AG1 futur ni bénéfice de trading n’est garanti par les tests.
- Déduplication sémantique intersources, enrichissement du texte IBKR et mesure du coût complet LLM restent hors de ce correctif ; aucune modification de scoring ou de gate AG1/dashboard.

## Sources et reproduction

Flux validés depuis le VPS : [RSS Dassault Systèmes](https://www.3ds.com/rss), [flux des communiqués DSY](https://www.3ds.com/contents/feed/content/article_press_release), [Fast Retailing IR RSS](https://www.fastretailing.com/eng/ir/news/index.xml). Contrat SEC : [API officielle](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).

Tests locaux/CI avec Node disponible : `node tests/test_news_pipeline.cjs`. Tests collecteurs avec DuckDB 1.4.3 : `python -m unittest discover -s tests -p test_free_news_collectors.py`. Rejeu détaillé et artefacts temporaires : `.codex-tmp/news-remediation-20260927/` ; copies et preuves shadow VPS : `/local-files/.codex-tmp/news-free-20260927/shadow/`. Branche : `codex/news-free-flash-20260927`.
