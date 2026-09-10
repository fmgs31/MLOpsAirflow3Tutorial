# Start the tutorial stack (Windows PowerShell + Docker Desktop).
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not (Test-Path ".env")) {
    Copy-Item "env.example" ".env"
    Write-Host "Created .env from env.example"
}

# Build one service, not all six: they share an image tag and parallel builds
# race on it ("image ... already exists").
docker compose build airflow-init
docker compose up -d

Write-Host "`nWaiting for the API server..."
do {
    Start-Sleep -Seconds 3
    $ok = $false
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8080/api/v2/monitor/health" -UseBasicParsing -TimeoutSec 5
        $ok = ($r.StatusCode -eq 200)
    } catch { $ok = $false }
} while (-not $ok)

Write-Host "Airflow 3 UI: http://localhost:8080"
docker compose ps --format "table {{.Service}}`t{{.Status}}"
