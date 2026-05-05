#!/usr/bin/env bash
# Creates the topics used by KafkaForge. Idempotent: existing topics are left alone.
set -euo pipefail

BROKER="${BROKER:-kafka-1:19092}"
PARTITIONS="${PARTITIONS:-6}"
RF="${RF:-3}"

run_kafka() {
  docker compose -f "$(dirname "$0")/docker-compose.yml" exec -T kafka-1 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" "$@"
}

create_topic() {
  local name="$1"
  if run_kafka --describe --topic "$name" >/dev/null 2>&1; then
    echo "topic '$name' already exists, skipping"
    return
  fi
  echo "creating topic '$name' (partitions=$PARTITIONS, rf=$RF)"
  run_kafka --create \
    --topic "$name" \
    --partitions "$PARTITIONS" \
    --replication-factor "$RF" \
    --config min.insync.replicas=2
}

create_topic notifications

echo
echo "current topics:"
run_kafka --list
