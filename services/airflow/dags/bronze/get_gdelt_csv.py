from os import environ, path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
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
        "max_active_runs": 1,
    },
) as dag:
    download_files = DockerOperator(
        task_id="t1_download_files",
        image="xuanminator/dyrmgraph_ingest:latest",
        command="python -m ingest.gdelt_csv",
        environment={
            "MANIFEST": "{{ dag_run.conf | tojson }}",
            "RUN_ID": "{{ dag_run.run_id }}",
            "MANIFEST_PG_USER": environ["MANIFEST_PG_USER"],
            "MANIFEST_PG_DB": environ["MANIFEST_PG_DB"],
            "MANIFEST_PG_HOST": environ["MANIFEST_PG_HOST"],
            "MANIFEST_PG_PORT": environ["MANIFEST_PG_PORT"],
            "MANIFEST_PG_PASSWORD": environ["MANIFEST_PG_PASSWORD"],
            "PICKLE_PATH": environ["PICKLE_PATH"],
            "MINIO_HOST": environ["MINIO_HOST"],
            "MINIO_PORT": environ["MINIO_PORT"],
            "MINIO_ACCESS_KEY": environ["MINIO_ACCESS_KEY"],
            "MINIO_SECRET_KEY": environ["MINIO_SECRET_KEY"],
            "CSV_DOWNLOAD_PATH": environ["CSV_DOWNLOAD_PATH"],
            "CSV_PERM_PATH": environ["CSV_PERM_PATH"],
            "CSV_INGESTION_BUCKET": environ["CSV_INGESTION_BUCKET"],
        },
        network_mode="docker_default",
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
            "is_new_file": "t3_queue_transform_run",
            "is_dupe_file": "t4_pass_dupe_cases",
            "is_failed": "t5_mark_failed_file_for_cleanup",
        },
    )

    queue_transform_run = SQLExecuteQueryOperator(
        task_id="t3_queue_transform_run",
        conn_id="postgres_conn_id",
        # The table is append only
        sql="""
            INSERT INTO transform_runs (queued_by, partition_date, status)
            VALUES (
                '{{ dag_run.run_id }}',
                TO_DATE(
                    '{{ ti.xcom_pull(task_ids="t1_download_files")["manifest"]["dt"][:8] }}',
                    'YYYYMMDD'
                ),
                'ready'
            )
        """,
    )

    pass_dupe_cases = EmptyOperator(task_id="t4_pass_dupe_cases")

    mark_failed_file_for_cleanup = SQLExecuteQueryOperator(
        task_id="t5_mark_failed_file_for_cleanup",
        conn_id="postgres_conn_id",
        sql="""
            UPDATE csv_file_registry
            SET status = 'failed'
            WHERE ingestion_id = '{{ dag_run.run_id }}'
        """,
    )

    handle_failed_cases = FailOperator(task_id="t6_handle_failed_cases", on_failure_callback=push_and_log)

    download_files >> next_step
    next_step >> [queue_transform_run, pass_dupe_cases, mark_failed_file_for_cleanup]
    mark_failed_file_for_cleanup >> handle_failed_cases
