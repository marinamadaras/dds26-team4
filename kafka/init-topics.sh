#!/bin/bash
set -e

until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:29092 >/dev/null 2>&1
do
  echo "Waiting for Kafka..."
  sleep 2
done

while IFS= read -r topic
do
  if [ -z "$topic" ]; then
    continue
  fi

  echo "Creating topic: $topic"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1

done < /topics.txt

echo "All topics created."
