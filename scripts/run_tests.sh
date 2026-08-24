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

  export PYTHONPATH=services:services/airflow/plugins
  export MANIFEST_PG_USER=$POSTGRES_USER
  export MANIFEST_PG_PASSWORD=$POSTGRES_PASSWORD
  export MANIFEST_PG_DB=$POSTGRES_DB
  export MANIFEST_PG_HOST=localhost
  export MANIFEST_PG_PORT=5432
  .venv/bin/pytest services/ingest/tests --ignore-glob='airflow/*'
  .venv/bin/pytest services/airflow/tests --ignore-glob='ingest/*'
  mvn -f services/transform/pom.xml test
elif [[ "$1" == "--ci" ]]; then
  # parallel tests. See .coveragerc for coverage config
  pytest services/ingest/tests --cov=services/ingest --cov-branch --cov-report=xml:coverage-ingest.xml --ignore-glob='airflow/*'
  pytest services/airflow/tests --cov=services/airflow --cov-branch --cov-report=xml:coverage-airflow.xml --ignore-glob='ingest/*'

else
  echo "Usage: $0 [--local|--ci]"
  exit 1
fi