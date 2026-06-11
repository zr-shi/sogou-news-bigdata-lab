#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  cp .env.example .env
fi

docker compose pull
docker compose up -d --no-build
docker compose ps
printf '\nDashboard: http://localhost:8501\nFlink UI:  http://localhost:8081\n'
