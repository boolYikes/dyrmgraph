from os import environ, path

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from callbacks.failure import on_dag_failure_notify, push_and_log
from operators.dict_branch import DictBranchOperator
from operators.fail import FailOperator
from pendulum import datetime

with DAG(
    dag_id="get_gdelt_latest_manifest",
    schedule="*/15 * * * *",
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "manifest", "latest"],
    on_failure_callback=on_dag_failure_notify,
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
        # "retry_delay": duration(minutes=5),
    },
) as dag:
    # TODO: Use docker/pod operator and invoke programs under ingest/
    check_manifest_file_is_uploaded = DockerOperator(
        task_id="t1_check_manifest_file_is_uploaded",
        image="xuanminator/dyrmgraph_ingest:latest",
        command="python -m ingest.gdelt_manifest",
        retries=3,
        retry_delay=120,
        # NOTE: sensing the result from downstream makes it too decoupled and flaky
        # -> instead, directly retrieve the result from the docker operator and decide branching logic here
        # -> necessitates xcom db cleanup periodically though,
        # -> but beats having to depend on print() log from the dockeroperator.
        retrieve_output=True,
        retrieve_output_path=path.join(environ["PICKLE_PATH"], "result.pkl"),
        network_mode="docker_default",  # change if using full docker compose profile
        environment={
            "MANIFEST_PG_USER": environ["MANIFEST_PG_USER"],
            "MANIFEST_PG_DB": environ["MANIFEST_PG_DB"],
            "MANIFEST_PG_HOST": environ["MANIFEST_PG_HOST"],
            "MANIFEST_PG_PORT": environ["MANIFEST_PG_PORT"],
            "MANIFEST_PG_PASSWORD": environ["MANIFEST_PG_PASSWORD"],
            "PICKLE_PATH": environ["PICKLE_PATH"],
        },
        auto_remove="success",
    )

    next_step = DictBranchOperator(
        task_id="t2_decide_next_step",
        source_task_id="t1_check_manifest_file_is_uploaded",
        key="status",
        branch_map={
            "is_new_manifest": "t3_trigger_downstream_dag",
            "is_dupe_manifest": "t4_pass_dupe_cases",
            "is_failed": "t5_handle_failed_cases",
        },
    )

    trigger_downstream_dag = TriggerDagRunOperator(
        task_id="t3_trigger_downstream_dag",
        trigger_dag_id="get_gdelt_csv",
        conf=check_manifest_file_is_uploaded.output,
        wait_for_completion=True,
        skip_when_already_exists=True,
    )

    pass_dupe_cases = EmptyOperator(task_id="t4_pass_dupe_cases")

    # NOTE: Now I think dlq should be done on on_failure_callback and do the alert with a separate dag
    # handle_failed_cases = DockerOperator(
    #     task_id="t5_handle_failed_cases",
    #     image="xuanminator/dyrmgraph_ingest:latest",
    #     command="python -m ingest.gdelt_manifest --handle-failed",
    # )
    handle_failed_cases = FailOperator(task_id="t5_handle_failed_cases", on_failure_callback=push_and_log)

    check_manifest_file_is_uploaded >> next_step
    next_step >> [trigger_downstream_dag, pass_dupe_cases, handle_failed_cases]

if __name__ == "__main__":
    dag.test(mark_success_pattern="wait_for_.*|check_manifest_file_is_uploaded")
