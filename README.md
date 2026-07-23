<!--
---
name: Dyrmgraph
date: 2026-07-16
tags: [python, elasticsearch, postgres, spark, airflow, minio, sql]
summary: A pipeline for knowledge ingestion, basic search and OSINT backbone
---
-->

# Dyrmgraph
![CI](https://github.com/boolYikes/dyrmgraph/actions/workflows/main.yaml/badge.svg?branch=main)
![Coverage](https://codecov.io/gh/boolYikes/dyrmgraph/branch/main/graph/badge.svg)

## Overview

### Data Pipeline

Processes raw data and transforms it into structured dataset

1. Polls the manifest every 15 minutes and downloads only newly published files
2. Transforms source data into raw tables
3. Transforms raw data into structured tables
4. Transforms structured data into mart tables
5. Embedding transformation (mart based)

## Tech Stack

### Data Pipeline
- Airflow for orchestration, scheduling, and backfills
- Spark Java as the batch processing engine for historical data
- MinIO as the object storage layer
- dbt-Spark as the transformation and governance layer
- Iceberg as the table format

## Data Modeling
![ERD](#insert_image_path)

## Architecture
```plaintext
python task: downloads source files -> PV/volume
dbt-spark: ingests raw CSV data -> lakehouse
dbt-spark: executes transformations via predefined UDFs -> lakehouse
dbt-spark: the same pattern is applied to the structured layer -> lakehouse
dbt-spark: mart data derived from the previous layer (event-level resolution) -> lakehouse
dbt-spark: embeddings on mart layer -> lakehouse
```

## Project structure
```
services/
  airflow/
    dags/
    plugins/
    tests/
  ingest/
    gdelt_manifest_poller/
    gdelt_downloader/
  transform/
    dbt/
    spark_jobs/
  loaders/
    age_loader/
    embedding_loader/
libs/
  common/
  contracts/
  config/
  observability/
  injestion/
infra/
  docker/
  k8s/
  terraform/
docs/
scripts/
```

## Development Setup
- `python -m venv .venv`
- `source .venv/bin/activate`
- use requirements files to install python packages (there are currently 2 of them)
- use scripts under /scripts to execute unit tests (need dyrmgraph-infra repo for local infra deployment)
- **Airflow connection ids (so far)**
  ```
  discord_conn_id
  redis_conn_id
  postgres_conn_id
  spark_conn_id
  ```

## Roadmap

### Milestone 1 - Data pipeline

Development environment

- [x] Local infra PoC
- [x] Local infrastructure/dev environment

Implementation

- [ ] Pipeline components
    - [x] Manifest ingestion
    - [x] CSV ingestion
    - [ ] Transformation
- Tests
    - [x] Unit tests
    - [ ] Integration tests
- Components tests

Cloud Infra
- Infrastructure and dependency setup
- Scripts (K8S manifests, Terraform, etc)
- Deployment strategy planning
- Observability

Deployment
- Staging
- Production

### Milestone 2
- ...

### Milestone 3

## Engineering Notes

<details>
<summary>Notes</summary>

- GDELT's ahead-of-time upload schedule for manifests (often 2-3 minutes ahead) 

A key design challenge was balancing completeness, latency, and operational complexity. One option was to continuously reconcile against the full manifest and use it as the source of truth, but that required expensive scans of a large dataset and introduced uncertainty because the provider did not disclose when the full manifest was refreshed.

Instead, the pipeline was designed around the incremental manifest as the primary ingestion source. A deferrable sensor persisted manifest snapshots and detected newly published files based on hashes and timestamps. File downloads were decoupled into downstream workflows, allowing ingestion to remain responsive even during large backfills or transient failures.

To handle missed publications and provider-side inconsistencies,
- Automated backfill workflows driven by timestamp ranges.
- Dead-letter handling for failed downloads and validation errors.
- Manifest persistence for replayability and auditability.
- Hash verification to ensure file integrity and idempotent processing.

This architecture reduced polling overhead, improved ingestion latency, and provided a clear recovery path when files were delayed, missing, or republished by the upstream provider.

</details>

<details>

<summary>Todos</summary>

- add healthcheck to docker compose

</details>

<!-- The hardest part wasn't downloading files—it was dealing with an upstream system that provided no push notifications and incomplete guarantees. I had to choose between continuously reconciling a massive full manifest of unknown freshness or treating the 15-minute incremental manifest as the operational source of truth. I chose the latter, built replayable ingestion around persisted manifests, added hash-based idempotency, backfill workflows, and DLQ handling. The result was a low-latency pipeline that could recover from missed updates without repeatedly scanning the entire historical dataset. -->