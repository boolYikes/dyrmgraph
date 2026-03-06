<!--
---
name: Dyrmgraph
date: 2026-03-04
tags: [python, langchain, langgraph, postgres, pgvector, age, spark, airflow]
summary: Information validation workflow
---
-->

## Dyrmgraph (WIP)

- **This project is being renewed**

### Overview
- **Backend**: Postgres, pgvector, Apache AGE, Spark, Airflow
- **AI Layer**: Local LLM(GPU), Embedding model(GPU), LangGraph(workflow engine), LangChain(tooling, memory)
- **Infra**: Redis(TTL cache), FastAPI, Docker Compose or GKE for later expansion
1. Collects data periodically as knowledge base
   - Scheduled ingestion (Airflow)
   - Normalize, transform (Spark) (EventID, Actors, Themes, Loc, TS, URL...)
   - Generate embeddings (sentence transformers etc)
   - Store:
     - Raw normalized event -> pg, 
     - embedding -> pgvector, 
     - entity relationship -> AGE
     - **GPU used in embedding step**
2. Crawls data per user input for fact check
   - Scrape, extract text, pub date and source domain
   - LLM claim extraction
     - Break article into atomic claims
     - Extract entities
     - identify factual assertions
   - LLM cross referencing
     - For each claim:
     - A) Semantic retrieval: embed claim, query pgvector, retrieve top-k related events
     - B) Graph expansion: find related actors, check co-occurence freq, find contradictory events, check event timeline proximity, etc
   - Narrative drift analysis: detect narrative mutation over N timeframe
     - Track embedding centroid shifts over time
     - Track claim framing evolution
     - Saved back to db for historical analysis
   - Deterministic credibility evaluation
     - Source trust score + num of corroborating events + graph connectivity strength + temporal consistency + semantic similarity score
   - LLM Report generation
     - Summarizes claim, supporting events, contradictions, graph relations, credibility, confidence
   - Query is cached with TTL (Redis)
3. Result is shown
   - Summary: from previous step
   - Relation graph: query AGE + render via D3.js or Sigma.js
   - Geo map: using GDELT coordinates, render via Leaflet.js
   - Timeline: Event histogram if timeline is relevant for presentation
4. Metrics and logs
   - *Managed with ELK*
   - Query latency
   - Embedding time
   - LLM token usage
   - Cache hit rate
   - Retrieval recall
   - Graph traversal depth
   - GPU utilization

### Things to remind
- narrative drift schema
```
topic_id
time_window
centroid_vector
drift_score
actor_graph_delta
source_entropy
```


### Usage
```bash
docker compose up -d --build

docker compose exec kafka bash /scripts/create_kafka_topics.sh

# Cron has replaced Airflow
# docker compose exec airflow airflow dags trigger gdelt_etl
```

### Todos:
- [x] Extract and first add to postgres
- [x] Join gkg-events-mentions to build a one-table-schema(Transform)
- [x] Publish + Topic init script
- [x] File not found exception - local side
- [x] Order the column list json by the actual order in respective tables
- [x] Add CSV Ingestion
- [x] Logger
- [x] class-based
- [ ] prune_columns to transform not gdelt
- [ ] Airflow tests
- [x] Spark + parquet
- [ ] Encapsulate and validate, use property
- [x] Column number extractor
- [ ] kibana config mount in compose
- [ ] Spark service in compose + changes to Config class + driver/exec memory
- [ ] Spark master url in code
- [ ] Use KRaft
- [x] Implement lazy loading
- [ ] Alerts: content - Insufficient tables, etc
- [ ] Downstream pipeline adjustment << 
- [ ] Use enums
- [x] Test suite
- [ ] Freeze deps
- [ ] Future
- [x] Typing
- [ ] Typing suite for spark, kafka etc
- [ ] Sort by <=> similarty, for canonical events listing
- [ ] Manifests, charts, configs for GKE