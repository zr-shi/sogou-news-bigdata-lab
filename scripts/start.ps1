$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

docker compose pull
docker compose up -d --no-build
docker compose ps

Write-Host ""
Write-Host "Dashboard: http://localhost:8501"
Write-Host "Flink UI:  http://localhost:8081"
