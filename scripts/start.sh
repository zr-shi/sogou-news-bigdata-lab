#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."

NO_PULL=0
NO_SEED_FALLBACK=0
RESTART_FLINK_JOB=0

for arg in "$@"; do
  case "$arg" in
    --no-pull) NO_PULL=1 ;;
    --no-seed-fallback) NO_SEED_FALLBACK=1 ;;
    --restart-flink-job) RESTART_FLINK_JOB=1 ;;
    -h|--help)
      cat <<'EOF'
Usage: ./scripts/start.sh [--no-pull] [--no-seed-fallback] [--restart-flink-job]

Options:
  --no-pull              Skip docker compose pull.
  --no-seed-fallback     Do not seed demo data if MySQL stays empty.
  --restart-flink-job    Submit the Flink job even if one is already running.
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 2
      ;;
  esac
done

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

wait_http() {
  url="$1"
  name="$2"
  seconds="${3:-120}"
  end_time=$(( $(date +%s) + seconds ))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 3
  done
  echo "$name is not ready after ${seconds}s" >&2
  return 1
}

wait_kafka_healthy() {
  seconds="${1:-120}"
  end_time=$(( $(date +%s) + seconds ))
  attempt=1
  while [ "$(date +%s)" -lt "$end_time" ]; do
    state="$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' news-kafka 2>/dev/null || true)"
    if printf '%s\n' "$state" | grep -Eq 'running[[:space:]]+healthy'; then
      echo "Kafka is healthy"
      return 0
    fi
    if [ "$attempt" -le 3 ]; then
      echo "Kafka is not healthy yet ($state); retrying startup after ZooKeeper session cleanup..."
      sleep 10
      docker compose up -d --no-build kafka
      attempt=$((attempt + 1))
    else
      sleep 5
    fi
  done
  echo "Kafka is not healthy after ${seconds}s" >&2
  return 1
}

running_flink_job() {
  if ! curl -fsS "http://localhost:8081/jobs/overview" >/tmp/sogou-news-flink-jobs.json 2>/dev/null; then
    return 1
  fi
  tr -d '\n' </tmp/sogou-news-flink-jobs.json | grep -q '"name":"KafkaFlinkMySQL".*"state":"RUNNING"'
}

get_db_counts() {
  docker exec -i news-dashboard python - <<'PY'
import os, pymysql
c = pymysql.connect(host=os.environ["DB_HOST"], port=int(os.environ["DB_PORT"]), user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"])
cur = c.cursor()
cur.execute("SELECT COUNT(*) FROM newscount")
print("newscount=" + str(cur.fetchone()[0]))
cur.execute("SELECT COUNT(*) FROM periodcount")
print("periodcount=" + str(cur.fetchone()[0]))
c.close()
PY
}

seed_demo_data() {
  docker exec -i news-dashboard python - <<'PY'
import os, sys
sys.path.insert(0, "/app")
from app import seed_demo_data
n, p = seed_demo_data(os.environ["DB_HOST"], int(os.environ["DB_PORT"]), os.environ["DB_NAME"], os.environ["DB_USER"], os.environ["DB_PASSWORD"])
print("seeded newscount=" + str(n) + ", periodcount=" + str(p))
PY
}

if [ "$NO_PULL" -eq 0 ]; then
  docker compose pull
fi

docker compose up -d --no-build mysql zookeeper kafka jobmanager taskmanager dashboard log-producer
wait_kafka_healthy
wait_http "http://localhost:8501/_stcore/health" "Dashboard"
wait_http "http://localhost:8081/overview" "Flink UI"

if [ "$RESTART_FLINK_JOB" -eq 0 ] && running_flink_job; then
  echo "Flink job is already running; skip submit. Use --restart-flink-job to resubmit after restarting the cluster."
else
  echo "Submitting Flink job..."
  docker compose up -d --no-build --force-recreate flink-job
fi

deadline=$(( $(date +%s) + 120 ))
has_data=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  sleep 5
  counts="$(get_db_counts || true)"
  echo "$counts" | paste -sd ', ' -
  news="$(printf '%s\n' "$counts" | awk -F= '/^newscount=/{print $2; exit}')"
  period="$(printf '%s\n' "$counts" | awk -F= '/^periodcount=/{print $2; exit}')"
  news="${news:-0}"
  period="${period:-0}"
  if [ "$news" -gt 0 ] && [ "$period" -gt 0 ]; then
    has_data=1
    break
  fi
done

if [ "$has_data" -eq 0 ] && [ "$NO_SEED_FALLBACK" -eq 0 ]; then
  echo "No database rows detected; seeding demo data so the dashboard can start cleanly."
  seed_demo_data
fi

docker compose ps
printf '\nDashboard: http://localhost:8501\nFlink UI:  http://localhost:8081\n'
