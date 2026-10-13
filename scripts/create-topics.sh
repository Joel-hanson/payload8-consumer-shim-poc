#!/usr/bin/env bash
# Create the orders and orders-new topics on both source and dest Kafka clusters.
set -euo pipefail

for topic in orders orders-new; do
  echo "Creating topic '$topic' on kafka-source..."
  docker exec kafka-source /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1

  echo "Creating topic '$topic' on kafka-dest..."
  docker exec kafka-dest /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done

echo "Topics ready."
