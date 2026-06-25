param(
    [switch]$NoPull,
    [switch]$NoSeedFallback,
    [switch]$RestartFlinkJob
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

function Wait-Http($Url, $Name, $Seconds = 120) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5 | Out-Null
            Write-Host "$Name is ready"
            return
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    throw "$Name is not ready after $Seconds seconds"
}

function Wait-KafkaHealthy($Seconds = 120) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    $attempt = 1
    while ((Get-Date) -lt $deadline) {
        $state = ""
        try {
            $state = docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' news-kafka 2>$null
        } catch {
            $state = ""
        }
        if ($state -match "running\s+healthy") {
            Write-Host "Kafka is healthy"
            return
        }
        if ($attempt -le 3) {
            Write-Host "Kafka is not healthy yet ($state); retrying startup after ZooKeeper session cleanup..."
            Start-Sleep -Seconds 10
            docker compose up -d --no-build kafka
            $attempt += 1
        } else {
            Start-Sleep -Seconds 5
        }
    }
    throw "Kafka is not healthy after $Seconds seconds"
}

function Get-DbCounts {
    $code = @'
import os, pymysql
c = pymysql.connect(host=os.environ["DB_HOST"], port=int(os.environ["DB_PORT"]), user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"])
cur = c.cursor()
cur.execute("SELECT COUNT(*) FROM newscount")
print("newscount=" + str(cur.fetchone()[0]))
cur.execute("SELECT COUNT(*) FROM periodcount")
print("periodcount=" + str(cur.fetchone()[0]))
c.close()
'@
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($code))
    docker exec news-dashboard python -c "import sys,base64;exec(base64.b64decode(sys.argv[1]))" $encoded
}

function Seed-DemoData {
    $code = @'
import os, sys
sys.path.insert(0, "/app")
from app import seed_demo_data
n, p = seed_demo_data(os.environ["DB_HOST"], int(os.environ["DB_PORT"]), os.environ["DB_NAME"], os.environ["DB_USER"], os.environ["DB_PASSWORD"])
print("seeded newscount=" + str(n) + ", periodcount=" + str(p))
'@
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($code))
    docker exec news-dashboard python -c "import sys,base64;exec(base64.b64decode(sys.argv[1]))" $encoded
}

function Get-RunningFlinkJobs {
    try {
        $overview = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 10
        return @($overview.jobs | Where-Object { $_.name -eq "KafkaFlinkMySQL" -and $_.state -eq "RUNNING" })
    } catch {
        return @()
    }
}

if (-not $NoPull) {
    docker compose pull
}

docker compose up -d --no-build mysql zookeeper kafka jobmanager taskmanager dashboard log-producer
Wait-KafkaHealthy
Wait-Http "http://localhost:8501/_stcore/health" "Dashboard"
Wait-Http "http://localhost:8081/overview" "Flink UI"

$runningJobs = @(Get-RunningFlinkJobs)
if ($runningJobs.Count -gt 0 -and -not $RestartFlinkJob) {
    Write-Host "Flink job is already running; skip submit. Use -RestartFlinkJob to resubmit after restarting the cluster."
} else {
    Write-Host "Submitting Flink job..."
    docker compose up -d --no-build --force-recreate flink-job
}

$deadline = (Get-Date).AddSeconds(120)
$hasData = $false
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 5
    $counts = @(Get-DbCounts)
    Write-Host ($counts -join ", ")
    $news = [int](($counts | Where-Object { $_ -like "newscount=*" }) -replace "newscount=", "")
    $period = [int](($counts | Where-Object { $_ -like "periodcount=*" }) -replace "periodcount=", "")
    if ($news -gt 0 -and $period -gt 0) {
        $hasData = $true
        break
    }
}

if (-not $hasData -and -not $NoSeedFallback) {
    Write-Host "No database rows detected; seeding demo data so the dashboard can start cleanly."
    Seed-DemoData
}

docker compose ps

Write-Host ""
Write-Host "Dashboard: http://localhost:8501"
Write-Host "Flink UI:  http://localhost:8081"
