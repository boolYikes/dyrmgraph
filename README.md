<!--
---
name: Dyrmgraph
date: 2026-03-25
tags: [python, langchain, langgraph, postgres, pgvector, age, spark, java, airflow, dbt, sql]
summary: Information validation workflow
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
- Spark Java as the distributed processing engine
- dbt-Spark as the transformation and governance layer
- Iceberg as the table format
- MinIO as the object storage layer

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
- `pip install -e .[test]`

## Roadmap

### Milestone 1 - Data pipeline

Development environment

- [x] Local infra PoC
- [ ] Local infrastructure/dev environment
- [ ] Observability

Planning

- Data schema
- Data contracts
- Backfill strategy

Implementation

- [ ] Pipeline components
    - [x] Ingestion
- Components tests
- Local integration tests

Cloud Infra
- Infrastructure and dependency setup
- Scripts (K8S manifests, Terraform, etc)
- Deployment strategy planning

Deployment
- Staging
- Production

### Milestone 2 - LangGraph
- ...

### Milestone 3 - Client

## Engineering Notes

<details>
<summary>Notes</summary>

user query
```
  → detect compound intent
  → split into subtopics
  → normalize vague terms
  → extract hard filters
  → run hybrid retrieval for each subtopic
  → find links across results
  → rerank by how well they satisfy the full chain
  → return one best article or a multi-article synthesis
```

</details>

<details>

<summary>Todos</summary>

- add healthcheck to docker compose

</details>
