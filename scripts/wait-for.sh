#!/usr/bin/env bash
# Wait for all services in the stack to report healthy before proceeding.
set -euo pipefail

wait_http() {
  local name="$1" url="$2"
  echo -n "  Waiting for $name ($url) ..."
  for i in $(seq 1 40); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo " ready."
      return 0
    fi
    sleep 3
    echo -n "."
  done
  echo " TIMED OUT." >&2
  return 1
}

wait_kafka() {
  local name="$1" container="$2"
  echo -n "  Waiting for $name ..."
  for i in $(seq 1 40); do
    if docker exec "$container" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      echo " ready."
      return 0
    fi
    sleep 3
    echo -n "."
  done
  echo " TIMED OUT." >&2
  return 1
}

wait_kafka       "kafka-source"      kafka-source
wait_kafka       "kafka-dest"        kafka-dest
wait_http        "schema-registry"   "http://localhost:8082/subjects"
