from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime

with DAG(
    dag_id="get_gdelt_csv",
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "manifest", "download"],
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
    },
) as dag:
    # TODO: Use docker/pod operator and invoke programs under ingest/

    # download the csv.zip files and validate hash
    download_files = DockerOperator(
        task_id="t1_download_files",
        image="xuanminator/dyrmgraph_ingest:latest",
        command="python -m ingest.gdelt_csv",
        environment={"MANIFEST": "{{ dag_run.conf | tojson }}"},
        retries=3,
        retry_delay=120,
    )

    # failed -> dlq (not the manifest dlq!!)
