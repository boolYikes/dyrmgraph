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
    dag_id="get_gdelt_csv",
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "manifest", "download"],
    on_failure_callback=on_dag_failure_notify,
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
    },
) as dag:
    download_files = DockerOperator(
        task_id="t1_download_files",
        image="xuanminator/dyrmgraph_ingest:latest",
        command="python -m ingest.gdelt_csv",
        environment={
            "MANIFEST_PG_USER": environ["MANIFEST_PG_USER"],
            "MANIFEST_PG_DB": environ["MANIFEST_PG_DB"],
            "MANIFEST_PG_HOST": environ["MANIFEST_PG_HOST"],
            "MANIFEST_PG_PORT": environ["MANIFEST_PG_PORT"],
            "MANIFEST_PG_PASSWORD": environ["MANIFEST_PG_PASSWORD"],
            "MANIFEST": "{{ dag_run.conf | tojson }}",
            "PICKLE_PATH": environ["PICKLE_PATH"],
            "CSV_DOWNLOAD_PATH": environ["CSV_DOWNLOAD_PATH"],
        },
        retries=3,
        retry_delay=120,
        retrieve_output=True,
        retrieve_output_path=path.join(environ["PICKLE_PATH"], "csv_file_result.pkl"),
        auto_remove="success",
    )

    # NOTE: no dupe is guaranteed from the upstream DAG,
    # but in case this DAG was triggered as an ad-hoc
    next_step = DictBranchOperator(
        task_id="t2_decide_next_step",
        source_task_id="t1_download_files",
        key="status",
        branch_map={
            "is_new_file": "t3_trigger_downstream_dag",  # -> downstream transformation
            "is_dupe_file": "t4_pass_dupe_cases",
            "is_failed": "t5_handle_failed_cases",
        },
    )

    # NOTE: should transformation depend on this? or be decoupled?
    # Maybe it should. but for batch operations it shouldn't
    trigger_downstream_dag = TriggerDagRunOperator(
        task_id="t3_trigger_downstream_dag",
        trigger_dag_id="transform_gdelt_csv",
        conf=download_files.output,
        wait_for_completion=False,  # not dependent on downstream success
        skip_when_already_exists=True,
    )

    pass_dupe_cases = EmptyOperator(task_id="t4_pass_dupe_cases")

    handle_failed_cases = FailOperator(task_id="t5_handle_failed_cases", on_failure_callback=push_and_log)

    download_files >> next_step
    next_step >> [trigger_downstream_dag, pass_dupe_cases, handle_failed_cases]
