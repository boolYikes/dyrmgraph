#!/usr/bin/env bash

set -u

INF_PATH="/lab/dee/repos_side/dyrmgraph-infra"
set -a
source "$INF_PATH/docker/.env"
set +a

# Temporary cluster creation script
k3d cluster create dyrmgraph-local \
  --api-port 0.0.0.0:32975 \
  --k3s-arg '--tls-san=host.docker.internal@server:*' # needed for local test

# TODO: needs checks and cluster start
kubectl create ns dyrmgraph

kubectl create secret generic minio-secret \
  -n dyrmgraph \
  --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"

kubectl create secret generic manifest-pg-secret \
  -n dyrmgraph \
  --from-literal=username="$META_PG_USER" \
  --from-literal=password="$META_PG_PASSWORD"

k3d kubeconfig get dyrmgraph-local \
  | yq '.clusters[0].cluster.server = "https://host.docker.internal:32975"' \
  > $INF_PATH/k8s/staging/.kubeconfig