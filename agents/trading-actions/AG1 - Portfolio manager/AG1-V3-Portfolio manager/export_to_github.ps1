param(
    [string]$CommitMessage = "AG1: rebuild portfolio manager pack from workflow",
    [switch]$Push
)

$ErrorActionPreference = "Stop"

$branch = (git rev-parse --abbrev-ref HEAD).Trim()
if (-not $branch -or $branch -eq "HEAD") {
    throw "Unable to detect current branch."
}

$folder = "AG1-V3-Portfolio manager"
git add -- $folder

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
