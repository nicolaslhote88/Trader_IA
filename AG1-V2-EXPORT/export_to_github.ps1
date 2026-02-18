param(
    [string]$CommitMessage = "AG1: update pre-agent data prep and export pack",
    [switch]$Push
)

$ErrorActionPreference = "Stop"

$branch = (git rev-parse --abbrev-ref HEAD).Trim()
if (-not $branch -or $branch -eq "HEAD") {
    throw "Unable to detect current branch."
}

$workflowFile = Get-ChildItem -File -Filter "AG1 - Workflow*.json" | Select-Object -First 1
if (-not $workflowFile) {
    throw "Workflow file matching 'AG1 - Workflow*.json' not found at repo root."
}

git add -- $workflowFile.Name "AG1-V2-EXPORT"

$staged = git diff --cached --name-only
if (-not $staged) {
    throw "No staged changes."
}

git commit -m $CommitMessage

if ($Push) {
    git push origin $branch
    Write-Host "Pushed to origin/$branch"
} else {
    Write-Host "Commit created on branch '$branch'."
    Write-Host "Run: git push origin $branch"
}
