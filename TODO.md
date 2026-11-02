### long term
- [x] clear ingestion bucket and do thorough integration test
- [x] fix validation errors
- [ ] Use a singleton logger via a config object
- [ ] Messy env variables: migrate all to pod operator
- [ ] XCOM cleaner
- [ ] on failure callback on a failure operator is awkward semantically. attach callbacks to actual tasks
- [ ] use Iceberg because merging lookup is a pain


### now
- [ ] rebuild, test the xcom output, check if downstream uses it correctly