### In Progress
- [ ] impl tf tests

### Backlog
- [ ] Event*Code are based on CAMEO codes (zero-lead, unspecified digits). make change to schema and tests accordingly
- [ ] Use a singleton logger via a config object
- [ ] Messy env variables: migrate all to pod operator
- [ ] XCOM cleaner
- [ ] on failure callback on a failure operator is awkward semantically. attach callbacks to actual tasks
- [ ] use Iceberg because merging lookup is a pain
- [ ] Compaction job (Continue with QueryExecutor.getPendingCompactionJobs)
- [ ] Backfill, historical

### Done
- [x] clear ingestion bucket and do thorough integration test
- [x] fix validation errors
- [x] impl airflow handler tests