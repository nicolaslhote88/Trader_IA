param(
    [string]$VpsHost = "root@100.104.236.78",
    [string]$KeyPath = "$HOME\.ssh\codex_vps_tailscale_ed25519",
    [int]$LocalPort = 5000
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$TargetScript = Join-Path $RepoRoot "outils\scripts\ibkr-open-tunnel.ps1"

if (-not (Test-Path -LiteralPath $TargetScript)) {
    Write-Host "Erreur: script cible introuvable: $TargetScript" -ForegroundColor Red
    Read-Host "Appuie sur Entree pour fermer"
    exit 1
}

& $TargetScript -VpsHost $VpsHost -KeyPath $KeyPath -LocalPort $LocalPort
