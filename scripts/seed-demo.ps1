param(
    [switch]$Reset
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$code = @'
import os, sys
sys.path.insert(0, "/app")
from app import seed_demo_data
n, p = seed_demo_data(
    os.environ["DB_HOST"],
    int(os.environ["DB_PORT"]),
    os.environ["DB_NAME"],
    os.environ["DB_USER"],
    os.environ["DB_PASSWORD"],
    reset=os.environ.get("RESET_DEMO", "0") == "1",
)
print(f"seeded newscount={n}, periodcount={p}")
'@

$env:RESET_DEMO = if ($Reset) { "1" } else { "0" }
try {
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($code))
    docker exec -e RESET_DEMO=$env:RESET_DEMO news-dashboard python -c "import sys,base64;exec(base64.b64decode(sys.argv[1]))" $encoded
} finally {
    Remove-Item Env:\RESET_DEMO -ErrorAction SilentlyContinue
}
