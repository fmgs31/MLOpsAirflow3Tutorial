# Start the tutorial stack (Windows PowerShell + Docker Desktop).
# A default Windows install refuses to run .ps1 files, so invoke it as:
#   powershell -ExecutionPolicy Bypass -File .\scripts\up.ps1
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
$tries = 0
do {
    Start-Sleep -Seconds 3
    $ok = $false
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8080/api/v2/monitor/health" -UseBasicParsing -TimeoutSec 5
        $ok = ($r.StatusCode -eq 200)
    } catch { $ok = $false }
    $tries++
    if ($tries -eq 20) {
        Write-Host "Still waiting after 60s. In another terminal:"
        Write-Host "  docker compose ps                  # anything 'Restarting'?"
        Write-Host "  docker compose logs airflow-init   # did the migration succeed?"
    }
} while (-not $ok)

Write-Host "Airflow 3 UI: http://localhost:8080"
docker compose ps --format "table {{.Service}}`t{{.Status}}"
