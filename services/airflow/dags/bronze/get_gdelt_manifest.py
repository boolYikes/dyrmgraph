from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime, duration

with DAG(
    dag_id="get_gdelt_manifest",
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 1, 1, tz="Asia/Seoul"),
    tags=["gdelt", "manifest", "download"],
    default_args={
        "owner": "dg_airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": duration(minutes=5),
        # "max_active_runs": 1,
    },
) as dag:
    # TODO: Use docker/pod operator and invoke programs under ingest/
    get_manifest_task = DockerOperator(
        task_id="fetch_gdelt_manifest",
    )

if __name__ == "__main__":
    dag.test(mark_success_pattern="wait_for_.*|get_manifest_task")
