param(
    [Parameter(Mandatory=$true)]
    [string]$RepoUrl
)

$git = "C:\Users\shaik\.gemini\antigravity\scratch\tools\git\cmd\git.exe"
$repo = "C:\Users\shaik\.gemini\antigravity\scratch\BDA_Product_Review_Analytics"
Set-Location $repo

Write-Host "Configuring remote origin: $RepoUrl" -ForegroundColor Cyan
& $git remote remove origin -ErrorAction SilentlyContinue
& $git remote add origin $RepoUrl
& $git branch -M main

Write-Host "Pushing to GitHub main branch..." -ForegroundColor Green
& $git push -u origin main
