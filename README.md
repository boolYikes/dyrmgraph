## dyrmgraph (WIP)
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
- [ ] Implement lazy loading
- [ ] Alerts: content - Insufficient tables, etc
- [ ] Downstream pipeline adjustment << 
- [ ] Use enums
- [x] Test suite
- [ ] Typing
- [ ] Sort by <=> similarty, for canonical events listing
- [ ] Manifests, charts, configs for GKE