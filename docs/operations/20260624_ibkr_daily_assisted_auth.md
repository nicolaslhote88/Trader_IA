# IBKR — authentification assistée quotidienne à 07:00

## But

Amorcer automatiquement le login Client Portal chaque jour à **07:00 heure de
Paris**. IBeam saisit les credentials déjà stockés sur le VPS et déclenche la
demande **IB Key**. L'opérateur valide uniquement la notification reçue sur son
téléphone.

La fenêtre autorisée est **07:00–07:30 Europe/Paris**. Aucune tentative
d'authentification navigateur n'est autorisée hors de cette fenêtre.

## Comportement

- tentatives maximales à 07:00, 07:10 et 07:20 ;
- chaque tentative attend au plus 8 minutes la validation IB Key ;
- arrêt immédiat dès que `/health` retourne `authenticated=true` ;
- verrou anti-concurrence et marqueur quotidien anti-double lancement ;
- `Persistent=false` : aucun rattrapage si le VPS était arrêté à 07:00 ;
- le fuseau `Europe/Paris` gère automatiquement heure d'été/hiver ;
- les sorties IBeam sont stockées en `600 root:root` et le niveau `WARNING`
  évite de journaliser sa configuration ;
- le lancement ponctuel utilise un `IBEAM_INPUTS_DIR` vide : le `conf.yaml`
  monté en lecture seule pour le Gateway n'est donc ni recopié ni modifié ;
- son serveur de santé interne utilise le port `5101`, distinct du port IBeam
  standard `5001` ;
- aucun changement des gardes d'ordres, du compte ou du mode LIVE.

## Fichiers

- script : `/opt/trader-ia/ops/ibkr_daily_auth.sh`
- service : `/etc/systemd/system/ibkr-daily-auth.service`
- timer : `/etc/systemd/system/ibkr-daily-auth.timer`
- journal synthétique : `/var/log/ibkr_daily_auth.log`
- erreurs IBeam : `/var/log/ibkr_daily_auth_ibeam.log`

Le précédent `ibkr-auth-watchdog.timer`, qui pouvait redémarrer le Gateway
toutes les 30 minutes lorsqu'il était déconnecté, doit rester **désactivé**.
Le keepalive normal reste assuré par `ibkr-broker` toutes les 55 secondes.

## Vérification

```bash
systemctl status ibkr-daily-auth.timer --no-pager
systemctl list-timers ibkr-daily-auth.timer --all --no-pager
systemd-analyze calendar '*-*-* 07:00:00 Europe/Paris'
curl -sS http://127.0.0.1:18080/health
tail -n 50 /var/log/ibkr_daily_auth.log
```

Test sans login réel :

```bash
IBKR_AUTH_NOW_PARIS=0700 \
IBKR_AUTH_DATE_PARIS=2099-01-01 \
IBKR_AUTH_DRY_RUN=true \
IBKR_DAILY_AUTH_STATE_DIR=/tmp/ibkr-daily-auth-test \
/opt/trader-ia/ops/ibkr_daily_auth.sh
```

## Rollback

```bash
systemctl disable --now ibkr-daily-auth.timer
rm -f /etc/systemd/system/ibkr-daily-auth.timer
rm -f /etc/systemd/system/ibkr-daily-auth.service
systemctl daemon-reload
```

Ne pas réactiver l'ancien watchdog sans corriger sa branche de redémarrage
automatique hors fenêtre.
