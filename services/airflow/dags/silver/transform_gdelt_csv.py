from os import environ

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from callbacks.failure import on_dag_failure_notify, push_and_log
from operators.dict_branch import DictBranchOperator
from operators.fail import FailOperator
from pendulum import datetime

# NOTE: Previously, in ingestion...
# Ingestion downloads csv, add it to bronze, add record to downloaded_file meta
# Ingestion adds record to transform_runs with partition_date, status PENDING, without version
# Example keys at ingestion time: s3://bucket/bronze/gkg/date=2026-07-20/001500.csv


secrets = [
    Secret(
        deploy_type="env",
        deploy_target="AWS_ACCESS_KEY_ID",
        secret="minio-secret",
        key="AWS_ACCESS_KEY_ID",
    ),
    Secret(
        deploy_type="env",
        deploy_target="AWS_SECRET_ACCESS_KEY",
        secret="minio-secret",
        key="AWS_SECRET_ACCESS_KEY",
    ),
    Secret(
        deploy_type="env",
        deploy_target="MANIFEST_PG_USER",
        secret="manifest-pg-secret",
        key="username",
    ),
    Secret(
        deploy_type="env",
        deploy_target="MANIFEST_PG_PASSWORD",
        secret="manifest-pg-secret",
        key="password",
    ),
]


with DAG(
    dag_id="transform_gdelt_csv",
    schedule="0 * * * *",
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "csv", "transform"],
    on_failure_callback=on_dag_failure_notify,
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
    },
) as dag:
    # TODO: identify and add a dag for external dimension dictionaries like GCAM etc
    # TODO: add a separate dag for compacting and version bumping

    # NOTE: See init.sql for transform_runs schema
    # version numbers are from successful transformations only
    # 1. Claims max N records of transform_runs that are "ready", update those as "claimed"
    # 2. Reads latest version on the dates assoc with the claimed recods, if any (probably use two spark workers if handling two dates?)
    claim_transform_job = SQLExecuteQueryOperator(
        task_id="t1_claim_transform_job",
        conn_id="postgres_conn_id",
        sql="""
            WITH claimed AS (
                SELECT id
                FROM transform_runs
                WHERE status = 'ready'
                ORDER BY id
                LIMIT 96
                FOR UPDATE SKIP LOCKED
            )
            UPDATE transform_runs tr
            SET status = 'claimed'
            FROM claimed
            WHERE tr.id = claimed.id;
        """,
    )

    # 3. Writes compacted parquets, incrementally (add and update rows if there were previous versions)
    # run 1 worker per date
    # Example partitions after transform: s3://bucket/silver/event-mentions/date=2026-07-20/version=N+1/part-0000.parquet
    # fmt: off
    perform_transformation = KubernetesPodOperator(
        task_id="t2_perform_transformation",
        kubernetes_conn_id="kubernetes_conn_id",
        name="transform-{{ ts_nodash }}",
        namespace="dyrmgraph",
        image="xuanminator/dyrmgraph_transform",
        cmds=["spark-submit"],
        # TODO: spark config TBD,
        # TODO: master url needed
        # configs are merged (this and the builder() config from the app)
        # spark-submit should specify provision-related configs
        arguments=[
            "--master", "k8s://https://<k8s api server>",
            "--deploy-mode", "cluster",

            "--num-executors", "2",
            "--executor-cores", "2",
            "--executor-memory", "4g",
            "--driver-memory", "2g",

            "--conf", "spark.kubernetes.namespace=dyrmgraph",
            "--conf", "spark.kubernetes.container.image=xuanminator/dyrmgraph_transform",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",

            "--conf", f"spark.hadoop.fs.s3a.endpoint={environ['MINIO_HOST']}",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",

            "local:///transform.jar",
        ],
        image_pull_policy="IfNotPresent",
        get_logs=True,
        on_finish_action="delete_pod",
        deferrable=True,
        env_vars={  # maybe use configmaps instead?
            "S3_HOST": "http://192.168.0.100:9000",
            "BUCKET": "gdelt-silver",
        },
        secrets=secrets,
        # config_file="/opt/airflow/plugins/kubeconfig",  # mutually exclusive with AF connections config
        poll_interval=30.0,
        logging_interval=5,
        do_xcom_push=True,
    )
    # fmt: on

    # NOTE: probably will need to retrieve xcom with return_value?
    next_step = DictBranchOperator(
        task_id="t3_next_step",
        source_task_id="t2_perform_transformation",
        key="status",
        branch_map={
            "is_success": "t4_mark_transform_run_as_success",
            "is_failed": "t5_mark_transform_run_as_failed",
        },
    )

    # 4. Update the transform_runs record to "success" on successful transform
    # 5. Update the transform_runs record with N+1 version on successful transform
    mark_as_success = SQLExecuteQueryOperator(task_id="t4_mark_transform_run_as_success", conn_id="postgres_conn_id", sql="")  # NOTE: decide what to do next or downstream

    # 6. Update the transform_runs status to "fail" update log column with errors
    mark_as_failed = SQLExecuteQueryOperator(task_id="t5_mark_transform_run_as_failed", conn_id="postgres_conn_id", sql="")

    # and fail + notify + dlq,
    fail = FailOperator(task_id="t6_declare_dag_run_fail", on_failure_callback=push_and_log)

    claim_transform_job >> perform_transformation >> next_step
    next_step >> [mark_as_success, mark_as_failed]
    mark_as_failed >> fail


# NOTE: If the re-published old records -> hash would be different but the date would be the same
