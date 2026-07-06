# from os import environ, path

from airflow import DAG

# from airflow.providers.docker.operators.docker import DockerOperator
# from airflow.providers.standard.operators.empty import EmptyOperator
# from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
# from operators.dict_branch import DictBranchOperator
# from operators.fail import FailOperator
from pendulum import datetime

with DAG(
    dag_id="check_dlq_and_notify",
    schedule="*/15 * * * *",
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "dlq", "alert", "logging"],
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
    },
) as dag:
    pass
