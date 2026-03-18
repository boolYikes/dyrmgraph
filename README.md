<!--
---
name: Dyrmgraph
date: 2026-03-19
tags: [python, langchain, langgraph, postgres, pgvector, age, spark, java, airflow, dbt, sql]
summary: Information validation workflow
---
-->

# Dyrmgraph (WIP)
<!-- ![CI](https://github.com/boolYikes/dyrmgraph/actions/workflows/main.yaml/badge.svg?branch=main)
![Coverage](https://codecov.io/gh/boolYikes/dyrmgraph/branch/main/graph/badge.svg) -->

Knowledge base pipeline (elaborate)

## Overview

### Data Pipeline
1. Polls the manifest every 15 minutes and downloads only newly published CSV files
2. Transforms source data into raw tables
3. Transforms raw data into structured tables
4. Transforms structured data into mart tables
5. Inserts graph and embedding datasets

## Tech Stack

### Data Pipeline
- Airflow for orchestration, scheduling, and backfills
- Spark Java as the distributed processing engine
- dbt-Spark as the transformation and governance layer
- Iceberg as the table format
- MinIO as the object storage layer

## Data Modeling
![ERD](#insert_image_path)

(elaborate)

## Architecture
```plaintext
# THIS IS A SKETCH
Airflow DAG downloads source files -> PV/volume
dbt-spark ingests raw CSV data -> lakehouse
dbt-Spark executes transformations via predefined UDFs -> lakehouse
the same pattern is applied to the structured layer -> lakehouse
the same pattern is applied to the mart layer -> lakehouse
Airflow DAG loads graph data and embedding -> graphdb
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
- ...

## Roadmap

### Milestone 1 - Data pipeline

Development environment

- Local development environment
- Local infrastructure
- Observability

Planning

- Data schema
- Data contracts
- Backfill strategy

Implementation

- Pipeline components
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
