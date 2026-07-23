from airflow import DAG
from callbacks.failure import on_dag_failure_notify
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
    tags=["gdelt", "alert", "test"],
    on_failure_callback=temp_fire,
    # on_success_callback=lambda context: logging.error("dyrm_success"),
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
        "on_failure_callback": on_dag_failure_notify,
    },
) as dag:
    # success = EmptyOperator(task_id="t1_success")
    # success
    fail = FailOperator(task_id="t1_fail")
    fail
