#!/bin/sh

set -eu

PROJECT_NAME="${COMPOSE_PROJECT_NAME:-dds26-team4}"
INTERVAL_SECONDS="${WATCHDOG_INTERVAL_SECONDS:-2}"
SERVICES="${WATCHDOG_SERVICES:-order-service-0 order-service-1 order-service-2 stock-service-0 stock-service-1 stock-service-2 payment-service-0 payment-service-1 payment-service-2 order-db-0 order-db-1 order-db-2 stock-db-0 stock-db-1 stock-db-2 payment-db-0 payment-db-1 payment-db-2}"
PARTITIONS="${WATCHDOG_PARTITIONS:-}"

if [ -n "$PARTITIONS" ]; then
  SERVICES=""
  i=0
  while [ "$i" -lt "$PARTITIONS" ]; do
    SERVICES="$SERVICES order-service-$i stock-service-$i payment-service-$i order-db-$i stock-db-$i payment-db-$i"
    i=$((i + 1))
  done
fi

find_container_id() {
  docker ps -aq \
    --filter "label=com.docker.compose.project=${PROJECT_NAME}" \
    --filter "label=com.docker.compose.service=$1" \
  | head -n 1
}

container_state() {
  docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || echo unknown
}

restart_container() {
  echo "[watchdog] restarting $1 ($2)"
  docker restart "$1" >/dev/null 2>&1 || true
}

echo "[watchdog] project=${PROJECT_NAME} interval=${INTERVAL_SECONDS}s"
echo "[watchdog] services=${SERVICES}"

while true; do
  for service in $SERVICES; do
    container_id="$(find_container_id "$service")"
    if [ -z "$container_id" ]; then
      continue
    fi

    state="$(container_state "$container_id")"
    if [ "$state" = "dead" ] || [ "$state" = "exited" ]; then
      restart_container "$container_id" "service=${service} state=${state}"
    fi
  done

  sleep "$INTERVAL_SECONDS"
done
