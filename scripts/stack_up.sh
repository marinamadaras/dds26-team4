#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   bash scripts/stack_up.sh 2pc
#   bash scripts/stack_up.sh saga
#
# What this does:
# 1. Writes the root .env file with the requested app modules.
# 2. Builds and starts the compose stack.
# 3. Restarts nginx so it re-resolves the api-gateway container after rebuilds.
# 4. Waits until the public gateway responds on localhost:8000.

MODE="${1:-2pc}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

case "${MODE}" in
  2pc)
    ORDER_MODULE="order2pcApp"
    PAYMENT_MODULE="payment2pcApp"
    STOCK_MODULE="stock2pcApp"
    ;;
  saga|app)
    MODE="saga"
    ORDER_MODULE="app"
    PAYMENT_MODULE="app"
    STOCK_MODULE="app"
    ;;
  *)
    echo "Unsupported mode: ${MODE}" >&2
    echo "Use one of: 2pc, saga" >&2
    exit 1
    ;;
esac

cat > "${ENV_FILE}" <<EOF
ORDER_APP_MODULE=${ORDER_MODULE}
PAYMENT_APP_MODULE=${PAYMENT_MODULE}
STOCK_APP_MODULE=${STOCK_MODULE}
EOF

echo "Configured mode: ${MODE}"
echo "Wrote ${ENV_FILE}"

cd "${REPO_ROOT}"

docker compose up -d --build

# nginx caches upstream resolution at startup. Restarting it here makes sure it
# talks to the newly recreated api-gateway container after rebuilds.
docker compose restart gateway >/dev/null

echo "Waiting for public gateway on http://127.0.0.1:8000 ..."

for _ in $(seq 1 60); do
  # We expect a 400 here because the order does not exist; any HTTP response
  # from this route means nginx + api-gateway are both reachable.
  status_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 \
    http://127.0.0.1:8000/orders/find/healthcheck || true)"
  if [[ "${status_code}" == "200" || "${status_code}" == "400" ]]; then
    echo "Gateway is reachable (HTTP ${status_code})."
    echo "Stack is up in ${MODE} mode."
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for the public gateway to become ready." >&2
exit 1
