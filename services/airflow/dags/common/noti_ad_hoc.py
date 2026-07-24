from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from callbacks.failure import on_dag_failure_notify, push_and_log
from operators.fail import FailOperator
from pendulum import datetime


def temp_fire(*args, **kwargs):
    print(*args)
    print(**kwargs)


with DAG(
    dag_id="notify_callback_fire_test",
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "alert", "test", "temporary"],
    on_failure_callback=on_dag_failure_notify,
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
        # "on_failure_callback": on_dag_failure_notify,
    },
) as dag:
    upstream_dummy = EmptyOperator(task_id="t1_dummy")
    # success
    fail = FailOperator(task_id="t2_fail", on_failure_callback=push_and_log)

    upstream_dummy >> fail
