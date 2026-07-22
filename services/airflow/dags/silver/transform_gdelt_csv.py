from airflow import DAG
from callbacks.failure import on_dag_failure_notify
from pendulum import datetime

# NOTE: this is detached from the ingestion
# NOTE: file list table needs a revision version column
# NOTE: ingestion needs one more step that inserts to transform_runs table
# transform_runs schema:
# run_id, output_path(e.g., s3://bucket/silver/.../version=N/), partition_date, version, status (pending/running/success/failed), created_at, started_at, completed_at, log
# version numbers are from successful transformations only
# a retry would make a new run_id(here, semantically a run_id is a transform request AND a transform run)

# Ingestion downloads csv, add it to bronze, add record to downloaded_file meta
# Ingestion adds record to transform_runs with partition_date, status PENDING, without version
# Example keys at ingestion time: s3://bucket/bronze/table=gkg/date=2026-07-20/001500.gkg.csv

# Transform runs every hour, detached from ingestion = at most syncs 4 data points per hour
# Claims max N records of pending transform_runs, update those as "running"
# Reads latest version on the dates assoc with the claimed recods, if any (probably use two spark workers if handling two dates?)
# Writes compacted parquets, incrementally (add and update rows if there were previous versions)
# Example partitions after transform: s3://bucket/silver/table=gkg/date=2026-07-20/version=N+1/part-0000.parquet
# Update the transform_runs record to "success" on successful transform
# Update the transform_runs record with N+1 version on successful transform
# Update the transform_runs status to "fail" and notify + dlq, update log column with errors


with DAG(
    dag_id="transform_gdelt_csv",
    schedule="*/15 * * * *",
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "csv", "transform"],
    on_failure_callback=on_dag_failure_notify,
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
    },
) as dag:
    pass


# NOTE: If the re-published old records -> hash would be different but the date would be the same
