#!/usr/bin/env bash

set -u


if [[ "$1" == "--local" ]]; then
  set -a
  source scripts/.env
  set +a

  if docker ps -a --format {{.Names}} | grep -q "$CONT_NAME"; then
    docker stop "$CONT_NAME"
    docker container rm "$CONT_NAME"
  fi

  docker run -d --name "$CONT_NAME" --rm -p 5432:5432 \
    -v dyrmgraph-manifest:/var/lib/postgresql/data \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -e POSTGRES_DB=$POSTGRES_DB \
    "$IMAGE_NAME"
  
  until docker exec "$CONT_NAME" pg_isready; do
    sleep 1
  done

  .venv/bin/pytest
elif [[ "$1" == "--ci" ]]; then
  pytest --cov --cov-branch --cov-report=xml

else
  echo "Usage: $0 [--local|--ci]"
  exit 1
fi