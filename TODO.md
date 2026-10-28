### long term
- [x] clear ingestion bucket and do thorough integration test
- [ ] fix validation errors
- [ ] Use a singleton logger via a config object
- [ ] Messy env variables: migrate all to pod operator
- [ ] XCOM cleaner
- [ ] on failure callback on a failure operator is awkward semantically. attach callbacks to actual tasks
- [ ] use Iceberg because merging lookup is a pain


### now
- [ ] make result save each dt, each result in the loop correctly. currently it overwrites previous dt and tables. the outermost of result should be a list for containing all dts