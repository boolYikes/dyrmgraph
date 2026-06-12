from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from plugins.operators.dict_branch import DictBranchOperator

with DAG(
    dag_id="get_gdelt_latest_manifest",
    schedule="*/15 * * * *",
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "manifest", "latest"],
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        # "retry_delay": duration(minutes=5),
        # "max_active_runs": 1,
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
        # TODO: decide output path at airflow cfg level -> from docker compose
        retrieve_output_path="./result.pkl",
    )

    next_step = DictBranchOperator(
        task_id="decide_next_step",
        source_task_id="check_manifest_file_is_uploaded",
        key="status",
        branch_map={
            "is_new_manifest": "t2_trigger_downstream_dag",
            "is_dupe_manifest": "t3_pass_dupe_cases",
            "is_failed": "t4_handle_failed_cases",
        },
    )

    trigger_downstream_dag = TriggerDagRunOperator(
        task_id="t2_trigger_downstream_dag",
        trigger_dag_id="get_gdelt_csv",
        conf=check_manifest_file_is_uploaded.output,
        wait_for_completion=True,
        skip_when_already_exists=True,
    )

    pass_dupe_cases = EmptyOperator(task_id="t3_pass_dupe_cases")

    handle_failed_cases = DockerOperator(
        task_id="t4_handle_failed_cases",
        image="xuanminator/dyrmgraph_ingest:latest",
        command="python -m ingest.gdelt_manifest --handle-failed",
    )

    check_manifest_file_is_uploaded >> next_step
    next_step >> [trigger_downstream_dag, pass_dupe_cases, handle_failed_cases]

if __name__ == "__main__":
    dag.test(mark_success_pattern="wait_for_.*|check_manifest_file_is_uploaded")
