# VPS SSH Access

Use this note to avoid rediscovering access details for the Hostinger VPS.

## Primary SSH Command

```bash
ssh vps
```

This alias is defined in `C:\Users\nicol\.ssh\config`.

## Confirmed Access

- Hostname observed: `srv961978`
- User: `root`
- Public IP: `82.112.242.251`
- Tailscale IP: `100.104.236.78`
- SSH key: `C:\Users\nicol\.ssh\codex_vps_tailscale_ed25519`
- Key fingerprint: `SHA256:jZBJ+i2Q5O0hThnK4xLWmG/IsLYWNYI8nrA2ZcfNrtw`
- OS observed: `Ubuntu 24.04`

## SSH Config Entries

```sshconfig
Host vps srv961978 vps-public
  HostName 82.112.242.251
  User root
  IdentityFile C:/Users/nicol/.ssh/codex_vps_tailscale_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new

Host vps-tailscale srv961978-ts
  HostName 100.104.236.78
  User root
  IdentityFile C:/Users/nicol/.ssh/codex_vps_tailscale_ed25519
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
```

## Verification Commands

```bash
ssh vps "hostname; whoami; cat /etc/os-release | head; uptime"
ssh vps-tailscale "hostname; whoami; tailscale status | head"
```

Expected user/host:

```text
srv961978
root
```

## Notes

- `C:\Users\nicol\.ssh\codex_vps_tailscale_ed25519` is the VPS key.
- `C:\Users\nicol\.ssh\id_ed25519` is for `atelier-pi`, not this VPS.
- Do not display or copy private key contents.
