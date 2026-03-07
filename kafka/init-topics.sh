#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"
TOPICS_FILE="${TOPICS_FILE:-/topics.txt}"
PARTITIONS="${TOPIC_PARTITIONS:-1}"
REPLICATION_FACTOR="${TOPIC_REPLICATION_FACTOR:-1}"
PARALLELISM="${TOPIC_CREATE_PARALLELISM:-4}"

mapfile -t TOPICS < <(sed 's/#.*//' "$TOPICS_FILE" | tr -d '\r' | awk 'NF')

if [ "${#TOPICS[@]}" -eq 0 ]; then
  echo "No topics to create."
  exit 0
fi

EXISTING_TOPICS="$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list || true)"
MISSING_TOPICS=()

for topic in "${TOPICS[@]}"; do
  if grep -Fxq "$topic" <<< "$EXISTING_TOPICS"; then
    echo "Topic already exists: $topic"
  else
    MISSING_TOPICS+=("$topic")
  fi
done

if [ "${#MISSING_TOPICS[@]}" -eq 0 ]; then
  echo "All topics already exist. Skipping creation."
  exit 0
fi

printf '%s\n' "${MISSING_TOPICS[@]}" | xargs -P"$PARALLELISM" -I{} /bin/bash -lc '
  topic="$1"
  echo "Creating topic: $topic"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$2" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$3" \
    --replication-factor "$4"
' _ {} "$BOOTSTRAP_SERVER" "$PARTITIONS" "$REPLICATION_FACTOR"

echo "Topic creation complete."
