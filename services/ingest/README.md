### How it's used
- This service is used as a package to minimize overhead
- Installed in airflow using `pip install .`
- Scripts are used in Airflow PythonOperator
- Shares Postgres Airflow meta storage

### Options
- Use a separate db instance: This might be the cleanest minus the cost of one more instance.
- As a separate service: Airflow is stateless -- more overhead (one more pod the deploy + api layer overhead)
- Object storage as semi-sync source: Download on system start, upload on system down or constant syncing -- very costly on S3
- Partition the manifest and only sync the last part in object storage