param(
    [string]$VpsHost = "root@100.104.236.78",
    [string]$KeyPath = "$HOME\.ssh\codex_vps_tailscale_ed25519",
    [int]$LocalPort = 5000
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "Trader_IA - Tunnel IBKR Client Portal" -ForegroundColor Cyan
Write-Host "VPS      : $VpsHost"
Write-Host "Local URL: https://localhost:$LocalPort"
Write-Host ""

if (-not (Get-Command ssh -ErrorAction SilentlyContinue)) {
    Write-Host "Erreur: ssh n'est pas disponible dans le PATH Windows." -ForegroundColor Red
    Read-Host "Appuie sur Entrée pour fermer"
    exit 1
}

if (-not (Test-Path -LiteralPath $KeyPath)) {
    Write-Host "Erreur: clé SSH introuvable: $KeyPath" -ForegroundColor Red
    Read-Host "Appuie sur Entrée pour fermer"
    exit 1
}

$existing = Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Le port $LocalPort est déjà ouvert localement. J'ouvre la page IBKR." -ForegroundColor Yellow
    Start-Process "https://localhost:$LocalPort"
    Read-Host "Appuie sur Entrée pour fermer"
    exit 0
}

Write-Host "Ouverture du navigateur dans 3 secondes..." -ForegroundColor Green
Start-Job -ScriptBlock {
    param($Url)
    Start-Sleep -Seconds 3
    Start-Process $Url
} -ArgumentList "https://localhost:$LocalPort" | Out-Null

Write-Host ""
Write-Host "Une fenêtre IBKR va s'ouvrir dans ton navigateur." -ForegroundColor Green
Write-Host "Connecte-toi au compte paper. Garde cette fenêtre PowerShell ouverte pendant la session."
Write-Host "Pour fermer le tunnel: Ctrl+C puis ferme cette fenêtre."
Write-Host ""

ssh `
    -i $KeyPath `
    -o IdentitiesOnly=yes `
    -o ExitOnForwardFailure=yes `
    -L "${LocalPort}:127.0.0.1:5000" `
    -N `
    $VpsHost

Write-Host ""
Write-Host "Tunnel fermé." -ForegroundColor Yellow
Read-Host "Appuie sur Entrée pour fermer"
