param(
    [string]$MODE = "2pc"
)

$ErrorActionPreference = "Stop"

# Resolve repo root
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$REPO_ROOT = Resolve-Path "$SCRIPT_DIR\.."
$ENV_FILE = "$REPO_ROOT\.env"

switch ($MODE) {
    "2pc" {
        $ORDER_MODULE = "order2pcApp"
        $PAYMENT_MODULE = "payment2pcApp"
        $STOCK_MODULE = "stock2pcApp"
    }
    "saga" {
        $MODE = "saga"
        $ORDER_MODULE = "app"
        $PAYMENT_MODULE = "app"
        $STOCK_MODULE = "app"
    }
    default {
        Write-Error "Unsupported mode: $MODE. Use one of: 2pc, saga"
        exit 1
    }
}

# Write .env file
@"
ORDER_APP_MODULE=$ORDER_MODULE
PAYMENT_APP_MODULE=$PAYMENT_MODULE
STOCK_APP_MODULE=$STOCK_MODULE
"@ | Out-File -Encoding ASCII $ENV_FILE

Write-Host "Configured mode: $MODE"
Write-Host "Wrote $ENV_FILE"

Set-Location $REPO_ROOT

# Build and start stack
docker compose up -d --build

# Restart gateway
docker compose restart gateway | Out-Null

Write-Host "Waiting for public gateway on http://127.0.0.1:8000 ..."

# Wait loop
for ($i = 0; $i -lt 60; $i++) {
    try {
        $response = Invoke-WebRequest `
            -Uri "http://127.0.0.1:8000/orders/find/healthcheck" `
            -Method GET `
            -TimeoutSec 3 `
            -ErrorAction SilentlyContinue

        $status_code = $response.StatusCode
    }
    catch {
        $status_code = 0
    }

    if ($status_code -eq 200 -or $status_code -eq 400) {
        Write-Host "Gateway is reachable (HTTP $status_code)."
        Write-Host "Stack is up in $MODE mode."
        exit 0
    }

    Start-Sleep -Seconds 1
}

Write-Error "Timed out waiting for the public gateway to become ready."
exit 1
