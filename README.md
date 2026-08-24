<!--
---
name: Dyrmgraph
date: 2026-08-21
tags: [python, java, sql, elasticsearch, postgres, spark, airflow, minio]
summary: A pipeline for knowledge ingestion, basic search and OSINT backbone
---
-->

# Dyrmgraph
![CI](https://github.com/boolYikes/dyrmgraph/actions/workflows/main.yaml/badge.svg?branch=main)
![Coverage](https://codecov.io/gh/boolYikes/dyrmgraph/branch/main/graph/badge.svg)

Dyrmgraph is an evolving news intelligence platform designed to answer complex natural-language questions by combining structured event data with retrieval and evidence-based reasoning.

An example of the type of question the system is intended to support:
```
What conflicts happened in southern Colombia in 2025, and what effect could they have on 2026 commodity prices?
```
Questions like this cannot be answered reliably with a single keyword search or one database query. They require multiple steps: identifying relevant events, resolving geographic and temporal constraints, connecting actors and locations, retrieving supporting evidence, and synthesizing the result while preserving provenance.

The long-term goal of Dyrmgraph is to provide this workflow through an agentic query layer backed by a reliable data and retrieval platform.

The current implementation focuses on that foundation: data ingestion, validation, normalization, orchestration, and recoverability.

## Purpose
Large public news datasets such as GDELT contain useful information for event discovery and open-source intelligence, but exposing the raw data directly to an LLM or user is not sufficient.

The source data has several characteristics that make downstream reasoning difficult:
- large volumes of continuously published files
- uncertain upstream publication behavior
- denormalized CSV schemas
- codes and identifiers that require interpretation
- duplicate or republished data
- the need to preserve provenance
- analytical questions that span time, geography, actors, events, and external context

Dyrmgraph treats the problem as more than a search interface.

The system is being designed as a pipeline that turns raw event data into structured, validated representations that can later be used by retrieval and agent workflows.
```
External data sources
        │
        ▼
Publication detection
        │
        ▼
Raw file ingestion
        │
        ▼
Schema validation
        │
        ▼
Normalization / Silver layer
        │
        ▼
Serving / retrieval models
        │
        ▼
Search + evidence retrieval
        │
        ▼
Agentic query workflow
        │
        ▼
Grounded answer with provenance
```
## Current Status
Dyrmgraph is under active development.

The data platform foundation is currently the most developed part of the project. The retrieval and agentic query layers are planned but are not yet implemented as production features.

| Component |	Status |
|-|-|
| Manifest ingestion and persistence |✅ |
| Ppublication detection	| ✅ |
| CSV file ingestion	| ✅ |
| File integrity checks	| ✅ |
| Schema validation	| ✅ |
| Structured / normalized Silver tables	| ✅ |
| Airflow orchestration	| ✅ |
| Spark transformations	| 🚧 |
| Spark execution on k3d	| ✅ |
| MinIO object storage for development	| ✅ |
| Automated backfill workflow	| 🚧 |
| Replayable ingestion	| ✅ |
| Dead-letter handling	| 🚧 |
| Serving / mart models	| 🚧 |
| Search and retrieval layer | 📋 |
| Embedding / vector retrieval	| 📋 |
| Knowledge relationship layer	| 📋 |
| Agentic query workflow	| 📋 |
| Evidence-grounded answer generation	| 📋 |
| AWS production deployment	| 📋 |
## Goal
The end goal is not simply to provide search results.

Dyrmgraph is intended to decompose complex questions into smaller data and reasoning operations.

For example:
`"What conflicts happened in southern Colombia in 2025,
and what effect could they have on 2026 commodity prices?"`
may require a workflow such as:

1. Interpret geography

   └─ What locations should be considered "southern Colombia"?

2. Apply temporal constraints
   
   └─ Which relevant events occurred during 2025?

3. Identify event types
   
   └─ Which events qualify as armed conflict or instability?

4. Resolve entities
   
   └─ Actors, organizations, locations, industries, commodities

5. Retrieve supporting records
   
   └─ Events and related source documents

6. Connect events with economic exposure
   
   └─ Production areas, infrastructure, transport, exports, commodities

7. Retrieve additional evidence
   
   └─ Market or industry context

8. Synthesize
   
   └─ Separate observed facts from inferred effects

9. Preserve provenance
   
   └─ Track which evidence supports each conclusion

An agent framework such as LangGraph or LangChain may eventually orchestrate parts of this workflow, but the framework itself is not the primary architectural goal.

The main design problem is determining what data, tools, relationships, and intermediate states the agent needs in order to produce defensible answers.

## Architecture
### Current Pipeline
```
                   GDELT 
                     │ 
                     ▼
        Incremental Manifest Polling 
                     │ 
                     ▼ 
        Persisted Manifest State 
                     │
            ┌────────┴────────┐
            │                 │ 
            ▼                 ▼ 
    New File Detection      Replay / 
            │               Backfill 
            ▼ 
      File Download 
            │ 
            ▼ 
          MinIO 
            │ 
            ▼ 
    Schema Validation 
            │ 
            ▼ 
  Spark Transformation 
            │ 
            ▼ 
  Structured Silver Data
```
### Orchestration
Airflow manages:

- scheduled publication checks
- manifest persistence
- task dependencies
- file ingestion workflows
- backfills
- retries and failure handling
- downstream Spark job submission

Airflow is currently run in Docker for the development environment.

Spark jobs are executed on a local k3d Kubernetes cluster and submitted through Airflow using KubernetesPodOperator.

This keeps orchestration separate from batch computation while allowing transformation jobs to run in isolated containers.

### Object Storage
MinIO is currently used as the S3-compatible object storage layer during development.

The intended cloud architecture uses Amazon S3 while retaining a similar object-storage boundary between ingestion and transformation stages.

### Processing
Spark performs batch transformation and normalization of ingested CSV data.

The current pipeline produces structured Silver-layer tables from source data and applies schema validation before data is promoted downstream.

## Data Modeling
The source dataset is not treated as an application-ready schema.

The transformation layer converts raw source records into normalized structures intended to make downstream querying and relationship analysis easier.

The Silver layer focuses on:
- consistent null handling
- normalization of source values
- schema enforcement using regex
- removal of source-format assumptions from downstream consumers
- separation of ingestion concerns from analytical representations

Future layers will derive query-oriented representations for retrieval and agent use cases.

Potential downstream entities include concepts such as:
```
Event
Actor
Location
Source
Topic
Organization
Commodity
Industry
```
and relationships such as:
```
Event ── occurred_at ──> Location
Event ── involves ─────> Actor
Event ── reported_by ──> Source
Location ── part_of ───> Region
Claim ── supported_by ─> Source
```
**These relationships describe the direction of the project rather than a completed ontology implementation.**

The exact representation will be determined as the retrieval and agent layers are developed.
## Engineering Decisions
### Incremental Manifests as the Operational Source

**Problem**
```
- GDELT does not provide a push-notification mechanism for newly published files.

- The full manifest can be used to determine the complete historical file set, but its refresh timing is not sufficiently clear for low-latency operational ingestion.

- Incremental manifests are published frequently, but relying on them introduces the possibility of missed, delayed, or republished files.
```

**Options Considered**

Option A — Continuously reconcile against the full manifest
```
Advantages:
- enables comparison against the complete dataset
- useful for completeness checks

Trade-offs:
- repeatedly scans a large manifest
- increases polling and comparison cost
```
Option B — Use incremental manifests as the primary operational source
```
Advantages:
- lower ingestion latency
- smaller polling workload
- better suited to detecting newly published files

Trade-offs:
- individual publications can potentially be missed between intervals
- requires an explicit recovery/backfill strategy
```
**Decision**

The pipeline uses the 15-minute incremental manifest as the primary operational ingestion source.

Completeness is handled separately rather than forcing the main ingestion loop to continuously reconcile the entire historical manifest.

**Recovery Strategy**

The pipeline compensates for the weaker completeness guarantee through:
- persisted manifest snapshots
- eventual gap-filling backfills
- replayable ingestion
- dead-letter handling
- file hash verification
- date-level idempotent processing

This separates two concerns:

- Normal operation

   └─ Optimize for low-latency detection
- Recovery / reconciliation
   
   └─ Optimize for eventual completeness and correctness

This design avoids repeatedly scanning the complete historical file list while preserving a recovery path for upstream inconsistencies.
### Persist Ingestion State Instead of Treating Polling as Ephemeral
Manifest polling results are persisted rather than existing only for the lifetime of an Airflow task.

This allows the system to answer questions such as:
- Which manifest was observed?
- When was it observed?
- Which files were considered new?
- Was a file previously processed?
- Can the ingestion decision be replayed?
- What happened before a failed downstream task?

Persisting ingestion state improves:
- traceability
- replayability
- debugging
- auditability
- failure recovery

It also prevents upstream discovery from being tightly coupled to downstream processing success.

### Separate Publication Detection from File Processing
A newly discovered file does not need to be completely downloaded and transformed before the system can continue checking for additional publications.

The pipeline therefore separates:
```
Publication detection
        │
        ▼
Persist ingestion state
        │
        ▼
Trigger downstream work
```
from:
```
Download
   │
Validate
   │
Transform
```
This means large backfills or transient failures in processing do not need to block the primary publication-detection workflow.

**The separation also makes individual downstream stages easier to retry independently.**

### Idempotency as a Pipeline Requirement
Retries are expected in distributed data systems.

A retried task should therefore not create logically duplicated results or corrupt ingestion state.

Dyrmgraph uses persisted state and file hashes to make repeated processing detectable and to support idempotent execution.

Hash verification also provides a mechanism for distinguishing between:
- an already processed file
- a duplicate publication
- a changed or republished file
- corrupted content

This becomes especially important when supporting automated backfills and replay.
### Airflow for Orchestration, Spark for Data Processing
Airflow coordinates workflows but is not used as the primary batch-processing engine.

Transformation workloads are implemented as Spark jobs and executed independently.

The current development environment uses:
```
Airflow
   │
   ▼
KubernetesPodOperator
   │
   ▼
Spark workload on k3d
```
Using KubernetesPodOperator is sufficient for the current scale and development stage and avoids introducing additional cluster-level infrastructure before it is required.

A Spark-specific operator or different job submission architecture can be evaluated later if operational requirements justify the additional complexity.
### Local Infrastructure Should Resemble Cloud Boundaries Without Requiring Cloud Resources
The development environment currently uses:
- MinIO for object storage
- k3d for Kubernetes workloads
- Docker for Airflow and supporting services

The intention is not to reproduce AWS locally in every detail.

Instead, the development environment preserves important architectural boundaries so components can later be replaced with managed infrastructure.

For example:
```
Development                 Target

MinIO              ───────> S3
Local containers   ───────> Cloud compute
k3d workloads      ───────> Kubernetes / cloud execution
```
This keeps local development inexpensive while avoiding unnecessary coupling to local-only storage or execution behavior.

## Reliability and Data Quality
The pipeline is designed around the assumption that upstream data and infrastructure can fail.
Current mechanisms include:

**Schema Validation**

Incoming data is validated against expected schemas before downstream transformation.
This prevents malformed or unexpectedly changed data from silently entering structured layers.

**Hash Verification**

File hashes are used to verify integrity and support duplicate/republication detection.

**Dead-Letter Handling**

Files that cannot be downloaded or validated can be isolated instead of indefinitely blocking unrelated ingestion work.

**Backfills**

Timestamp-range workflows allow historical periods to be reprocessed without modifying the primary real-time ingestion strategy.

**Replay**

Persisted manifest state allows previously observed ingestion decisions to be reconstructed and replayed.

**Task Isolation**

Detection, download, validation, and transformation responsibilities are separated so failures can be retried at the appropriate stage.

## Planned Agentic Query Layer

The eventual query system will sit on top of the structured data platform.

A likely high-level architecture is:
```
User Question
     │
     ▼
Query Understanding
     │
     ▼
Task / Query Decomposition
     │
     ├───────────────┐
     ▼               ▼
Structured Query   Evidence Retrieval
     │               │
     ▼               ▼
Event Data       Source Documents
     │               │
     └───────┬───────┘
             ▼
      Evidence Aggregation
             │
             ▼
       Reasoning / Synthesis
             │
             ▼
     Answer + Provenance
```
Possible tools may include:
- LangGraph / LangChain for workflow orchestration
- vector or hybrid retrieval
- structured SQL queries
- entity and relationship retrieval
- LLM-based query decomposition
- evidence ranking
- tool-assisted synthesis

The exact agent architecture is intentionally not fixed yet.

It will be designed around the retrieval and reasoning requirements discovered while building the serving layer rather than around a specific framework.
## Technology
### Currently Used
**Languages**

`Python`, `Java`, `SQL`, 

**Data / Processing**

`Apache Spark`, `PostgreSQL`, `MinIO`, 

**Orchestration**

`Apache Airflow`, `Bash`

**Infrastructure**

`Docker`, `Kubernetes / k3d`

**CI / Testing**

`GitHub Actions`, `Codecov`, `unit tests`

**Planned / Under Evaluation**

`Amazon S3`, `cloud deployment on AWS`, `dbt`, `Iceberg`, 

**vector / hybrid retrieval**

`LangGraph or LangChain`, 

**agentic query orchestration**

`knowledge / relationship-oriented serving models`

***Items in this section are not necessarily implemented yet.***

## Repository Structure
```
services/
  airflow/
    config/
    dags/
    plugins/
    tests/

  ingest/
    gdelt_csv/
    gdelt_manifest/
    libs/
    models/
    tests/
    Dockerfile

  transform/
    main/java/com/dyrmgraph/transform/
      utils/
    test/

scripts/
```
The infra is managed under a [separate repository](https://github.com/boolYikes/dyrmgraph-infra).

The repository structure is evolving together with the architecture.
## Development Environment
```
python -m venv .venv
source .venv/bin/activate
```
Install dependencies using the relevant requirements files for each component.

Local infrastructure is managed separately from application components.

Scripts under scripts/ are used for common development and test operations.

Current Airflow connection identifiers include:
```
discord_conn_id
redis_conn_id
postgres_conn_id
spark_conn_id
kubernetes_conn_id
```
## Roadmap
### Milestone 1 — Data Foundation

**Ingestion**

- [x]: Incremental manifest ingestion
- [x]: Manifest persistence
- [x]: New-file detection
- [x]: CSV ingestion
- [x]: Hash validation
- [x]: Replay support
- [x]: Timestamp-based backfill
- [x]: Dead-letter handling

**Transformation**

- [x]: Spark transformation jobs
- [x]: Schema validation
- [x]: Structured / normalized Silver tables
- [ ]: Additional serving-oriented transformations
- [ ]: Mart / Gold models

**Development Infrastructure**

- [x]: Docker-based local infrastructure
- [x]: MinIO object storage
- [x]: Airflow orchestration
- [x]: Spark execution on k3d
- [x]: Unit testing
- [ ]: Integration testing
- [ ]: Expanded observability

### Milestone 2 — Retrieval Foundation
- [ ]: Define serving models for search
- [ ]: Build event-oriented query layer
- [ ]: Add source-document retrieval
- [ ]: Evaluate vector, lexical, and hybrid retrieval
- [ ]: Entity resolution
- [ ]: Relationship representation
- [ ]: Evidence provenance model
- [ ]: Retrieval evaluation dataset

### Milestone 3 — Agentic Query Workflow
- [ ]: Natural-language query decomposition
- [ ]: Structured data query tools
- [ ]: Document retrieval tools
- [ ]: Multi-step workflow orchestration
- [ ]: Evidence aggregation
- [ ]: Answer synthesis
- [ ]: Citation / provenance output
- [ ]: Evaluation of groundedness and retrieval quality

### Milestone 4 — Deployment
- [ ]: Replace local object storage with S3
- [ ]: Separate stateless and stateful infrastructure
- [ ]: Define AWS deployment architecture
- [ ]: Terraform infrastructure
- [ ]: Staging environment
- [ ]: Production deployment
- [ ]: Monitoring and alerting

## What I Am Exploring Through This Project
Dyrmgraph is also an engineering exercise in several broader questions:

- Where should deterministic data processing end and model-based reasoning begin?
How should provenance be preserved across multi-step AI workflows?
- How can an ingestion platform remain responsive when the upstream provider offers incomplete operational guarantees?
- What information should be normalized ahead of time, and what should be resolved dynamically at query time?
- When is a relational representation sufficient, and when does an explicit graph or ontology provide meaningful value?
- How should failures and uncertainty be represented so an agent does not silently treat missing data as negative evidence?

The project is intentionally being built from the data foundation upward.

Before adding an agent layer, the underlying data needs to be traceable, reproducible, validated, and queryable enough that downstream AI behavior can be evaluated against something reliable.