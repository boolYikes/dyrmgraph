from airflow import DAG
from pendulum import datetime

with DAG(
    dag_id="get_gdelt_csv",
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "download", "csv"],
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
    },
) as dag:
    pass
