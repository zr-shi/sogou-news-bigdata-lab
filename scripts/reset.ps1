$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose down -v --remove-orphans
docker compose pull
docker compose up -d --no-build
docker compose ps
