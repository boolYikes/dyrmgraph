#!/usr/bin/env bash
set -e
BROKER=${1:-kafka:9092}

topics=(gdelt.raw gdelt.embedded gdelt.graph_updates gdelt.es_updates)

for t in "${topics[@]}"; do
  kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 1
done