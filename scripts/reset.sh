#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
docker compose down -v --remove-orphans
docker compose pull
docker compose up -d --no-build
docker compose ps
