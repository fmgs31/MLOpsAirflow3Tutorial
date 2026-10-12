# Tear the stack down and delete every artifact the tutorial produced
# (Windows PowerShell + Docker Desktop). The metadata DB and MLflow volumes go
# too - you get a clean Airflow and an empty experiment tracker. Your BigQuery
# tables and Airbyte are left alone. Invoke it as:
#   powershell -ExecutionPolicy Bypass -File .\scripts\reset.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$reply = Read-Host "Delete containers, the metadata DB and MLflow volumes, and data/? [y/N]"
if ($reply -notin @("y", "Y")) { Write-Host "Aborted."; exit 1 }

docker compose down --volumes --remove-orphans
foreach ($path in "data/*", "dbt/*/target", "dbt/*/logs", "logs/*") {
    Remove-Item -Path $path -Recurse -Force -ErrorAction SilentlyContinue
}
foreach ($keep in "data/.gitkeep", "logs/.gitkeep") {
    New-Item -ItemType File -Path $keep -Force | Out-Null
}
Write-Host "Clean. Run scripts\up.ps1 to start over."
