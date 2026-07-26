#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_dir="$project_root/.ssh"
target_dir="${HOME:?HOME is not set}/.ssh"

for required in config codex_vps_tailscale_ed25519 id_ed25519; do
    if [ ! -f "$source_dir/$required" ]; then
        printf 'Missing SSH bridge file: %s\n' "$required" >&2
        exit 1
    fi
done

umask 077
mkdir -p "$target_dir"
chmod 700 "$target_dir"

install -m 600 "$source_dir/codex_vps_tailscale_ed25519" "$target_dir/codex_vps_tailscale_ed25519"
install -m 600 "$source_dir/id_ed25519" "$target_dir/ia_atelier_pi_ed25519"

if [ -f "$source_dir/codex_vps_tailscale_ed25519.pub" ]; then
    install -m 644 "$source_dir/codex_vps_tailscale_ed25519.pub" "$target_dir/codex_vps_tailscale_ed25519.pub"
fi
if [ -f "$source_dir/id_ed25519.pub" ]; then
    install -m 644 "$source_dir/id_ed25519.pub" "$target_dir/ia_atelier_pi_ed25519.pub"
fi

if [ -f "$source_dir/known_hosts" ]; then
    install -m 600 "$source_dir/known_hosts" "$target_dir/known_hosts"
fi

portable_config="$target_dir/config.ia-tmp"
ia_config="$target_dir/config.ia"
escaped_target=$(printf '%s/' "$target_dir" | sed 's/[&|]/\\&/g')
sed -e 's/\r$//' \
    -e "s|C:/Users/nicol/.ssh/codex_vps_tailscale_ed25519|${escaped_target}codex_vps_tailscale_ed25519|g" \
    -e "s|C:/Users/nicol/.ssh/id_ed25519|${escaped_target}ia_atelier_pi_ed25519|g" \
    "$source_dir/config" > "$portable_config"
chmod 600 "$portable_config"
mv -f "$portable_config" "$ia_config"

main_config="$target_dir/config"
include_line="Include $ia_config"
if [ -f "$main_config" ]; then
    if ! grep -Fqx "$include_line" "$main_config"; then
        config_tmp="$target_dir/config.main-tmp"
        {
            printf '%s\n' "$include_line"
            cat "$main_config"
        } > "$config_tmp"
        chmod 600 "$config_tmp"
        mv -f "$config_tmp" "$main_config"
    fi
else
    printf '%s\n' "$include_line" > "$main_config"
    chmod 600 "$main_config"
fi

ssh -G vps-tailscale >/dev/null
printf 'SSH sandbox bootstrap ready.\n'
