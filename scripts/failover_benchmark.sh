#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

HOST="${HOST:-http://localhost:8000}"
USERS="${USERS:-100}"
SPAWN_RATE="${SPAWN_RATE:-10}"
DURATION="${DURATION:-10m}"
WARMUP_SECONDS="${WARMUP_SECONDS:-10}"
KILL_SLEEP="${KILL_SLEEP:-5}"
RECOVERY_SLEEP="${RECOVERY_SLEEP:-10}"
RECOVERY_TIMEOUT="${RECOVERY_TIMEOUT:-45}"
RESULTS_DIR="${RESULTS_DIR:-results}"
STOP_LOCUST_AFTER_FAILOVER="${STOP_LOCUST_AFTER_FAILOVER:-1}"
LOCUST_STOP_TIMEOUT="${LOCUST_STOP_TIMEOUT:-20}"
KAFKA_INIT_TIMEOUT="${KAFKA_INIT_TIMEOUT:-90}"

SERVICES=(
  "${SERVICES:-order-service stock-service payment-service kafka order-db stock-db payment-db}"
)

RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="$RESULTS_DIR/failover-$RUN_ID"
mkdir -p "$OUT_DIR"

LOCUST_PID=""

container_id() {
  docker compose ps -q "$1" | head -n1
}

kafka_init_container_id() {
  docker compose ps -a -q kafka-init | head -n1
}

wait_for_kafka_init() {
  local timeout="$1"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    local cid
    cid="$(kafka_init_container_id)"

    if [[ -n "$cid" ]]; then
      local state
      state="$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "$cid" 2>/dev/null || true)"
      local status exit_code
      status="$(awk '{print $1}' <<<"$state")"
      exit_code="$(awk '{print $2}' <<<"$state")"

      if [[ "$status" == "exited" && "$exit_code" == "0" ]]; then
        echo "==> kafka-init completed successfully"
        return 0
      fi

      if [[ "$status" == "exited" && "$exit_code" != "0" ]]; then
        echo "==> kafka-init failed (exit code $exit_code)"
        return 1
      fi
    fi

    if (( "$(date +%s)" - start_ts >= timeout )); then
      echo "==> kafka-init did not complete within ${timeout}s"
      return 1
    fi
    sleep 1
  done
}

wait_for_auto_recovery() {
  local service="$1"
  local timeout="$2"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    local cid
    cid="$(container_id "$service")"
    if [[ -n "$cid" ]]; then
      local state
      state="$(docker inspect --format '{{.State.Running}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}} {{.RestartCount}}' "$cid" 2>/dev/null || true)"
      local running health
      running="$(awk '{print $1}' <<<"$state")"
      health="$(awk '{print $2}' <<<"$state")"
      if [[ "$running" == "true" ]] && ([[ "$health" == "healthy" ]] || [[ "$health" == "none" ]]); then
        echo "---- $service recovered automatically"
        return 0
      fi
    fi

    if (( "$(date +%s)" - start_ts >= timeout )); then
      echo "---- $service did not auto-recover within ${timeout}s"
      return 1
    fi
    sleep 1
  done
}

crash_service() {
  local service="$1"
  # Crash PID 1 inside the container so Docker restart policy is triggered.
  docker compose exec -T "$service" sh -c 'kill -9 1'
}

request_code() {
  local method="$1"
  local path="$2"
  curl -sS -o /dev/null -w "%{http_code}" -X "$method" "${HOST}${path}" || echo "000"
}

assert_not_bad_gateway() {
  local code="$1"
  local label="$2"
  if [[ "$code" == "502" || "$code" == "503" || "$code" == "504" || "$code" == "000" ]]; then
    echo "---- endpoint probe failed for $label with HTTP $code"
    return 1
  fi
  return 0
}

probe_service_endpoint() {
  local service="$1"
  local code

  case "$service" in
    order-service|order-db)
      code="$(request_code POST "/orders/create/healthcheck-user")"
      assert_not_bad_gateway "$code" "/orders/create/healthcheck-user"
      ;;
    stock-service|stock-db)
      code="$(request_code POST "/stock/item/create/1")"
      assert_not_bad_gateway "$code" "/stock/item/create/1"
      ;;
    payment-service|payment-db)
      code="$(request_code POST "/payment/create_user")"
      assert_not_bad_gateway "$code" "/payment/create_user"
      ;;
    kafka)
      code="$(request_code POST "/payment/create_user")"
      assert_not_bad_gateway "$code" "/payment/create_user"
      code="$(request_code POST "/stock/item/create/1")"
      assert_not_bad_gateway "$code" "/stock/item/create/1"
      code="$(request_code POST "/orders/create/kafka-health-user")"
      assert_not_bad_gateway "$code" "/orders/create/kafka-health-user"
      ;;
    *)
      echo "---- no probe configured for service $service"
      return 1
      ;;
  esac
}

cleanup() {
  if [[ -n "$LOCUST_PID" ]] && kill -0 "$LOCUST_PID" 2>/dev/null; then
    kill -INT "$LOCUST_PID" >/dev/null 2>&1 || true
    wait "$LOCUST_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> Starting stack"
docker compose up -d --build

echo "==> Waiting for kafka-init"
if ! wait_for_kafka_init "$KAFKA_INIT_TIMEOUT"; then
  echo "kafka-init readiness check failed. Check: docker compose logs kafka-init kafka"
  exit 1
fi

echo "==> Starting Locust"
locust -f "$ROOT_DIR/locustfile.py" \
  --host "$HOST" \
  --headless \
  -u "$USERS" \
  -r "$SPAWN_RATE" \
  -t "$DURATION" \
  --csv "$OUT_DIR/locust" \
  --csv-full-history \
  >"$OUT_DIR/locust.log" 2>&1 &
LOCUST_PID="$!"

sleep "$WARMUP_SECONDS"

echo "==> Injecting failures and validating recovery"
for svc in ${SERVICES[0]}; do
  if ! kill -0 "$LOCUST_PID" 2>/dev/null; then
    echo "Locust exited before failover phase ended. Check $OUT_DIR/locust.log"
    exit 1
  fi

  echo "---- crash $svc"
  if ! crash_service "$svc" >/dev/null 2>&1; then
    echo "---- failed to crash $svc via exec; attempting docker compose kill"
    docker compose kill "$svc" || true
  fi
  sleep "$KILL_SLEEP"

  echo "---- waiting for auto-recovery of $svc"
  if ! wait_for_auto_recovery "$svc" "$RECOVERY_TIMEOUT"; then
    echo "Auto-recovery failed for $svc. Check docker restart policy and container logs."
    exit 1
  fi
  sleep "$RECOVERY_SLEEP"

  echo "---- endpoint probe after $svc recovery"
  if ! probe_service_endpoint "$svc" >"$OUT_DIR/probe-$svc.log" 2>&1; then
    echo "Endpoint probe failed after $svc recovery. See $OUT_DIR/probe-$svc.log"
    exit 1
  fi
done

echo "==> Waiting for Locust to finish"
if [[ "$STOP_LOCUST_AFTER_FAILOVER" == "1" ]] && [[ -n "$LOCUST_PID" ]] && kill -0 "$LOCUST_PID" 2>/dev/null; then
  echo "==> Stopping Locust after failover checks"
  kill -INT "$LOCUST_PID" >/dev/null 2>&1 || true
  stop_deadline=$(( $(date +%s) + LOCUST_STOP_TIMEOUT ))
  while kill -0 "$LOCUST_PID" 2>/dev/null; do
    if (( $(date +%s) >= stop_deadline )); then
      echo "==> Locust did not stop within ${LOCUST_STOP_TIMEOUT}s, forcing termination"
      kill -TERM "$LOCUST_PID" >/dev/null 2>&1 || true
      sleep 2
      kill -KILL "$LOCUST_PID" >/dev/null 2>&1 || true
      break
    fi
    sleep 1
  done
fi
wait "$LOCUST_PID"
LOCUST_PID=""

echo "Benchmark completed successfully."
echo "Artifacts: $OUT_DIR"
