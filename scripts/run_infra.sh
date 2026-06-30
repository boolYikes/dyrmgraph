#!/usr/bin/env bash

set -u

INF_PATH="/lab/dee/repos_side/dyrmgraph-infra"

if [[ "$1" == "--local" ]]; then
  set -a
  source "$INF_PATH/docker/.env"
  set +a

  docker compose -f "$INF_PATH/docker/dev.docker-compose.yml" --profile airflow up -d

elif [[ "$1" == "--ci" ]]; then
  # WIP
  exit 1

elif [[ "$1" == "--down" ]]; then
  set -a
  source "$INF_PATH/docker/.env"
  set +a

  docker compose -f "$INF_PATH/docker/dev.docker-compose.yml" --profile airflow down
else
  echo "Usage: $0 [--local|--ci]"
  exit 1
fi