# Atelier Pi SSH Access

Use this note to avoid rediscovering access details for the Raspberry Pi 5
workshop machine.

## Primary SSH Command

```bash
ssh atelier-pi
```

This alias is defined in `C:\Users\nicol\.ssh\config`.

## Confirmed Access

- Host: `atelier-pi`
- User: `nico`
- SSH key: `C:\Users\nicol\.ssh\id_ed25519`
- Key fingerprint: `SHA256:rTpj8rdGfy4T8JJ4Vp7BgkBA/dpIObYWveHw5H+/5Cc`
- Tailscale IP: `100.125.164.44`
- MagicDNS/FQDN: `atelier-pi.tail68c22e.ts.net`
- OS observed: `Debian GNU/Linux 13 (trixie)`
- Kernel observed: `6.12.75+rpt-rpi-2712`

## SSH Config Entries

```sshconfig
Host atelier-pi atelier-pi.local
  HostName atelier-pi.local
  User nico
  IdentityFile C:/Users/nicol/.ssh/id_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new

Host atelier-pi-ts
  HostName 100.125.164.44
  User nico
  IdentityFile C:/Users/nicol/.ssh/id_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new

Host atelier-pi-magicdns
  HostName atelier-pi.tail68c22e.ts.net
  User nico
  IdentityFile C:/Users/nicol/.ssh/id_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
```

## Verification Command

```bash
ssh atelier-pi "hostname; whoami; uname -a; cat /etc/os-release | head; uptime"
```

Expected user/host:

```text
atelier-pi
nico
```

## Notes

- `C:\Users\nicol\.ssh\codex_vps_tailscale_ed25519` is for the VPS, not this Pi.
- `C:\Users\nicol\.ssh\id_ed25519` is the key that worked for `nico@atelier-pi.local`.
- Do not display or copy the private key contents.
