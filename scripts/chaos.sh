#!/usr/bin/env bash
# Chaos: kill a random broker for DOWN_FOR seconds, then bring it back.
# Run this while a publisher + consumer are active to observe recovery
# behaviour (under-replicated partitions, leader re-election, fetch retries).
set -euo pipefail

COMPOSE="docker compose -f $(dirname "$0")/../deploy/docker-compose.yml"
TARGETS=(kafka-1 kafka-2 kafka-3)
DOWN_FOR="${DOWN_FOR:-10}"

target="${1:-${TARGETS[$((RANDOM % ${#TARGETS[@]}))]}}"
echo ">>> killing $target for ${DOWN_FOR}s"
$COMPOSE kill "$target"
sleep "$DOWN_FOR"
echo ">>> restarting $target"
$COMPOSE start "$target"
echo ">>> waiting for healthcheck..."
until $COMPOSE ps --format json | grep -q "\"Service\":\"$target\".*\"Health\":\"healthy\""; do
  sleep 1
done
echo ">>> $target healthy"
