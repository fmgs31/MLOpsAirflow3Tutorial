# Tear the stack down and delete every artifact the tutorial produced
# (Windows PowerShell + Docker Desktop). The metadata DB volume goes too -
# you get a clean Airflow. Invoke it as:
#   powershell -ExecutionPolicy Bypass -File .\scripts\reset.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$reply = Read-Host "Delete containers, the metadata DB volume, data/ and model_registry/? [y/N]"
if ($reply -notin @("y", "Y")) { Write-Host "Aborted."; exit 1 }

docker compose down --volumes --remove-orphans
foreach ($path in "data/raw", "data/warehouse", "model_registry/*", "logs/*") {
    Remove-Item -Path $path -Recurse -Force -ErrorAction SilentlyContinue
}
foreach ($keep in "data/.gitkeep", "model_registry/.gitkeep", "logs/.gitkeep") {
    New-Item -ItemType File -Path $keep -Force | Out-Null
}
Write-Host "Clean. Run scripts\up.ps1 to start over."
