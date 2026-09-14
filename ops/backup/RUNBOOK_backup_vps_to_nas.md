# Runbook — Sauvegarde intégrale VPS `srv961978` → NAS Synology

**Périmètre** : données applicatives (restaurables sur n'importe quel serveur), pas d'image disque.
**Transport** : montage SMB/CIFS du partage `VPS Sauvegarde` sur le VPS, via Tailscale, compte `codex_nas`.
**Mode** : one-shot.

Script associé : `ops/backup/backup_vps_to_nas.sh`

---

## 0. Statut des informations

**Cible confirmée (07/08/2026)**

| Élément | Valeur |
|---|---|
| NAS Tailscale | `nico-synology` — **100.110.120.73** |
| VPS Tailscale | `srv961978` — **100.104.236.78** |
| Compte SMB | `codex_nas` — Lecture/écriture validée sur le partage (capture DSM) |
| Partage destination | `VPS Sauvegarde` — **chiffré**, btrfs, Volume 1 |
| Point de montage VPS | `/mnt/nas-vps-sauvegarde` |
| Destination finale | `VPS Sauvegarde/srv961978/<TS>` |

**Pourquoi SMB et pas SSH.** L'accès `codex_nas` mis en place par Codex est un accès **SMB**, déjà
autorisé sur ce partage. Le réutiliser depuis le VPS évite d'activer SSH sur le DSM et d'ajouter
`codex_nas` au groupe `administrators` — moins de surface exposée, aucun réglage DSM à modifier.

**Ce que l'accès de Codex ne couvre PAS.** Les scripts `Resolve-IANASPath.ps1` / `Test-IANASAccess.ps1`
sont des outils **PowerShell côté Windows**, liés au Gestionnaire d'identifiants de la session `nicol`.
Ils ne donnent aucun accès depuis le VPS, ni depuis un sandbox Linux. Le VPS a besoin de sa propre
copie du mot de passe SMB, dans un fichier root-only (§1bis).

**Faits validés** (mémoire projet, 30/06/2026)

- VPS Hostinger `srv961978` / `82.112.242.251`, 96 Go, ~32 Go utilisés.
- Stacks Docker sous `/docker/root` et `/docker/yfinance` ; services IBKR sous `/opt/trader-ia/services/`.
- Bases DuckDB sous `/local-files/duckdb/` (~11 Go après nettoyage).
- Volume `n8n_data` (~1,1 Go) sous `/var/lib/docker/volumes/`.
- Conteneurs : `root-n8n-1`, `root-task-runners-*`, `root-trading-dashboard-1`, `ibkr-gateway`, `ibkr-broker`, `hermes-webui-*` (hors projet).

**Hypothèses vérifiées par le préflight**

- `cifs-utils` et le module noyau `nls_utf8` installés sur le VPS. Sur l'image
  minimale Hostinger Ubuntu, installer le paquet de modules correspondant au
  noyau actif :

  ```bash
  apt-get install -y cifs-utils "linux-modules-extra-$(uname -r)"
  modprobe nls_utf8
  ```
- Service SMB actif dans DSM (`Panneau de configuration` → `Services de fichiers` → `SMB`).
- Le partage chiffré `VPS Sauvegarde` est **monté** côté DSM.
- `/local-files` est un chemin réel sur l'hôte (bind mount) et non un volume Docker nommé.
- NAS en Linux 3.10.108 → DSM 6.x : SMB3 supporté, fallback `vers=2.1` si besoin.

**Ce que je ne peux pas faire depuis Cowork** : mon shell est un sandbox Linux isolé, sans Tailscale,
sans client SMB, sans PowerShell. Les commandes ci-dessous sont à exécuter par toi ; je peux analyser
toutes les sorties que tu me colles.

---

## 1. Points de vigilance — à lire avant de lancer

| ⚠️ | Risque | Parade appliquée dans le script |
|---|---|---|
| **1** | **Ne jamais faire de `CHECKPOINT` DuckDB avant une copie.** Le 05/07, un CHECKPOINT sur base fragmentée a réduit une base de 3,9 Go à 48 Mo. | Copie **à froid**, aucune commande DuckDB n'est exécutée. |
| **2** | **Session IBKR.** Arrêter `ibkr-gateway` impose un relogin manuel LIVE + 2FA (mémoire 21). | `ibkr-gateway` et `ibkr-broker` **exclus** de l'arrêt. |
| **3** | **Clé de chiffrement n8n.** Sans `n8n_data/_data/config` (contient `encryptionKey`), toutes les credentials n8n sont **irrécupérables**. | Incluse dans `06_docker_volumes`. Présence vérifiée en §4. |
| **4** | **Copie SQLite à chaud = corruption.** `database.sqlite` + `-wal` (427 Mo) doivent être cohérents. | Arrêt de `root-n8n-1` avant la copie. |
| **5** | **CIFS ne restitue ni owner ni permissions POSIX.** Une copie fichier-par-fichier serait inexploitable pour restaurer des volumes Docker. | On copie des **archives `tar --numeric-owner`** ; les droits réels vivent dans le tar. |
| **6** | **Partage chiffré démonté** = le partage disparaît de la liste SMB, le montage échoue. | `nas_mount` échoue explicitement avec ce diagnostic en tête de liste. |
| **7** | **Mot de passe SMB en clair sur le VPS.** Nécessaire au montage CIFS. | Fichier `/root/.smb-nas-credentials` en **0600**, permissions vérifiées à chaque montage, jamais lu ni affiché par le script. |
| **8** | **Fenêtre horaire.** Crons n8n la nuit (AG2 22h/2h, AG3 0h/1h/4h UTC) + runs AG1 en journée. | Lancer **week-end ou après 22h Paris**, marchés fermés. |
| **9** | **Coupure réseau sur un transfert de ~11 Go.** | `rsync --partial` : relancer `push`, la reprise est automatique. |
| **10** | **Chiffrement DSM (eCryptfs)** : noms limités à 143 caractères, débit d'écriture réduit. | Noms d'archives courts ; prévoir un transfert plus lent que le débit brut. |

---

## 1bis. Préparer l'accès SMB sur le VPS (à faire une fois)

### a) Vérifier côté DSM

1. `Panneau de configuration` → `Services de fichiers` → onglet `SMB` → **Activer le service SMB**.
2. `Dossier partagé` → `VPS Sauvegarde` → vérifier que le chiffrement est **monté** (cadenas ouvert).
   S'il est démonté : sélectionner le partage → `Chiffrement` → `Monter`.
   ⚠️ Un partage chiffré ne se remonte **pas** au redémarrage du NAS, sauf si la clé est dans le trousseau DSM.
3. Permissions `codex_nas` : déjà en Lecture/écriture sur `VPS Sauvegarde` — confirmé par ta capture.

### b) Installer le client CIFS sur le VPS

```bash
ssh vps-tailscale
apt-get update
apt-get install -y cifs-utils "linux-modules-extra-$(uname -r)"
modprobe nls_utf8
```

### c) Déposer les identifiants SMB (root-only)

Méthode recommandée depuis la session Windows de Nicolas :

```powershell
& D:\IA\.access\scripts\Install-IANASCredentialOnVPS.ps1 -Prompt
```

Le script lit le credential `codex_nas` dans le Gestionnaire d'identifiants
Windows et l'envoie directement dans l'entrée standard de SSH. Il ne l'affiche
pas, ne le passe pas dans la ligne de commande et ne crée aucun fichier local.

Méthode manuelle de secours :

À taper **toi-même** sur le VPS — je ne dois ni voir ni manipuler ce mot de passe :

```bash
umask 077
cat > /root/.smb-nas-credentials <<'EOF'
username=codex_nas
password=LE_MOT_DE_PASSE
EOF
chmod 600 /root/.smb-nas-credentials
```

> Si le compte est dans un domaine ou un workgroup particulier, ajouter une ligne `domain=WORKGROUP`.

### d) Valider le montage

```bash
bash /root/backup_vps_to_nas.sh preflight
```

Le préflight monte le partage, teste l'écriture, affiche l'espace disponible puis démonte.

> **Si le montage échoue** : essayer `CIFS_VERS=2.1 bash /root/backup_vps_to_nas.sh preflight`,
> puis `1.0`. DSM 6.x négocie normalement SMB3, mais le maximum autorisé est réglable dans
> `Services de fichiers` → `SMB` → `Avancé`.

---

## 2. Ce qui est sauvegardé

| Lot | Source | Contenu |
|---|---|---|
| `00_metadata.tar.gz` | — | crontab, `docker ps/images/volumes/inspect`, paquets, systemd, réseau, ufw, fstab |
| `01_docker_configs` | `/docker` | tous les `docker-compose.yml` + `.env` |
| `02_opt` | `/opt` | sources `ibkr-gateway`, `ibkr-broker` |
| `03_root` | `/root` | scripts de maintenance, historique |
| `04_local_files` | `/local-files` | **bases DuckDB** (le gros du volume) |
| `05_etc` | `/etc` | nginx, systemd, tailscale, confs système |
| `06_docker_volumes` | `/var/lib/docker/volumes` | `n8n_data` (base + **clé de chiffrement**), autres volumes persistants ; exclut le runtime de session IBKR `yfinance_ibeam_outputs` et les sockets Unix |
| `07_docker_images` | `docker save` | images construites localement (non repullables) |

> `03_root` inclut `/root/.smb-nas-credentials`. C'est cohérent — le partage de destination est chiffré —
> mais à garder en tête : ne jamais recopier ce lot ailleurs que sur ce NAS.

Pour garantir une copie cohérente, le script arrête aussi temporairement les
services qui écrivent dans `/local-files`, `/opt` ou les volumes archivés : n8n,
dashboards, traitements AG5/AG9, SIGA, yfinance, Hermes, Portainer, code-server
et Traefik. `ibkr-gateway` et `ibkr-broker` restent actifs. Le volume
`yfinance_ibeam_outputs`, runtime de session du Gateway, est donc exclu et sera
recréé lors du relogin IBKR après restauration.

---

## 3. Exécution

### Première exécution programmée

La première sauvegarde est programmée par systemd le **samedi 8 août 2026 à
23:00 Europe/Paris** avec :

- `/root/run_backup_vps_to_nas_once.sh` ;
- `vps-backup-to-nas-once.service` ;
- `vps-backup-to-nas-once.timer`.

Le lanceur exécute `preflight → stop → dump → start → push`, garantit le
redémarrage des writers en cas d'échec avant `start`, vérifie la présence de la
clé n8n dans l'archive et contrôle la santé IBKR. Le staging local est conservé
après cette première exécution. Journaux : `/var/log/vps-backup-to-nas/` et
`journalctl -u vps-backup-to-nas-once.service`.

Premiere sauvegarde complete validee le **2026-08-09** : snapshot
`20260809T065302Z`, 2,35 Go transferes, huit archives relues depuis le NAS avec
SHA256 `OK`. La tentative du 2026-08-08 s'etait arretee avant transfert parce
que Hermes ecrivait encore dans un volume archive ; la liste d'arret a ete
etendue a tous les producteurs concernes.

Contrôle du minuteur :

```bash
systemctl status vps-backup-to-nas-once.timer
systemctl list-timers vps-backup-to-nas-once.timer
```

### Exécution manuelle

```powershell
# --- Sur ta machine Windows : déposer le script sur le VPS ---
scp D:\IA\Trader_IA\ops\backup\backup_vps_to_nas.sh vps-tailscale:/root/
```

```bash
# --- Sur le VPS ---
ssh vps-tailscale
export TS=$(date -u +%Y%m%dT%H%M%SZ)     # fige l'horodatage pour toutes les étapes
echo "TS=$TS"                            # NOTER cette valeur

bash /root/backup_vps_to_nas.sh preflight   # ne modifie rien — LIRE LA SORTIE
```

**Arrêt du préflight si** : espace libre `/` < 2× la taille des sources, montage CIFS KO, ou écriture refusée.

```bash
bash /root/backup_vps_to_nas.sh stop     # ~1 min — l'interruption de service commence ici
bash /root/backup_vps_to_nas.sh dump     # 10–30 min selon la compression
bash /root/backup_vps_to_nas.sh start    # redémarrage — fin de l'interruption
bash /root/backup_vps_to_nas.sh push     # transfert + vérification, services déjà repartis
```

**Indisponibilité réelle** : uniquement de `stop` à `start`, soit ~15–30 min. Le `push` n'a pas besoin
des services arrêtés, le staging local est déjà figé et cohérent.

Si tu ouvres une nouvelle session SSH entre deux étapes, **réexporter le même `TS`** :

```bash
export TS=<valeur notée>
```

---

## 4. Vérifications après coup

```bash
# La clé de chiffrement n8n est bien dans l'archive — CRITIQUE
tar -tzf /var/backups/vps-snapshot/$TS/06_docker_volumes.tar.gz | grep 'n8n_data/_data/config'

# Contenu déposé sur le NAS
bash /root/backup_vps_to_nas.sh mount
ls -lh /mnt/nas-vps-sauvegarde/srv961978/$TS
cd /mnt/nas-vps-sauvegarde/srv961978/$TS && sha256sum -c SHA256SUMS
cd / && bash /root/backup_vps_to_nas.sh umount

# Services repartis
docker ps
curl -sS http://127.0.0.1:18080/health
```

Depuis Windows, contrôle croisé avec les outils de Codex :

```powershell
$p = & D:\IA\.access\scripts\Resolve-IANASPath.ps1 -Share 'VPS Sauvegarde' -RelativePath 'srv961978'
Get-ChildItem -LiteralPath $p | Sort-Object Name -Descending | Select-Object -First 3
```

Nettoyage du staging **seulement après** validation des checksums :

```bash
rm -rf /var/backups/vps-snapshot/$TS
```

---

## 5. Restauration (à connaître avant d'en avoir besoin)

Sur un serveur neuf, dans cet ordre :

```bash
# 1. Docker + Tailscale + cifs-utils installés, puis rapatrier les archives depuis le NAS
# 2. Restaurer configs et données
tar -xzf 01_docker_configs.tar.gz -C /
tar -xzf 02_opt.tar.gz            -C /
tar -xzf 03_root.tar.gz           -C /
tar -xzf 04_local_files.tar.gz    -C /
# 3. Volumes Docker — Docker doit être ARRÊTÉ
systemctl stop docker
tar -xzf 06_docker_volumes.tar.gz -C /var/lib/docker
systemctl start docker
# 4. Images locales
gunzip -c 07_docker_images.tar.gz | docker load
# 5. Relancer
cd /docker/root && docker compose up -d
# 6. Recréer les crons depuis metadata/crontab_root.txt
# 7. Relogin IBKR manuel (toggle LIVE + 2FA), cf. mémoire 21
```

**Test de restauration recommandé**, sans serveur neuf : extraire `06_docker_volumes.tar.gz` dans un
répertoire temporaire et vérifier la base n8n.

```bash
mkdir -p /tmp/restore_test && tar -xzf 06_docker_volumes.tar.gz -C /tmp/restore_test
sqlite3 /tmp/restore_test/volumes/n8n_data/_data/database.sqlite "PRAGMA integrity_check;"
ls /tmp/restore_test/volumes/n8n_data/_data/config    # la clé de chiffrement doit être là
rm -rf /tmp/restore_test
```

---

## 6. Actions restantes / non couvert

- **Pas de rotation** : c'est un one-shot. En récurrent (cron hebdo + purge > 4 snapshots), la même
  logique résoudrait aussi le problème historique d'accumulation de backups sur le VPS (remplissage à 80 % en juin).
- **Partage chiffré non remonté après reboot du NAS** : mode de panne silencieuse n°1 si tu passes en
  récurrent. Enregistrer la clé dans le trousseau DSM et prévoir une alerte si le montage échoue.
- **Volumes `hermes-webui-*` (~1,8 Go)** : hors projet Trader_IA, inclus par défaut dans le lot 06.
  À exclure si tu veux alléger.
- **Snapshot Hostinger** : indépendant. Vérifier s'il est actif — c'est le complément « image disque ».
- **Documenter l'usage VPS→NAS** dans `D:\IA\ACCES_COMMUNS.md` : la section NAS ne décrit aujourd'hui
  que l'accès SMB depuis Windows, pas ce montage CIFS côté VPS.
