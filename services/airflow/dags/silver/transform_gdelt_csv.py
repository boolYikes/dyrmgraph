from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from callbacks.failure import on_dag_failure_notify, push_and_log
from handlers.sql_fetch import _handler_fetch_flat_list
from operators.dict_branch import DictBranchOperator
from operators.fail import FailOperator
from operators.s3 import S3ReadObjectOperator
from operators.sql_branch import SQLSkipBranchOperator
from pendulum import datetime

# NOTE: Previously, in ingestion...
# Ingestion downloads csv, add it to bronze, add record to downloaded_file meta
# Ingestion adds record to transform_runs with partition_date, status PENDING, without version
# Example keys at ingestion time: s3://bucket/bronze/gkg/date=2026-07-20/001500.csv


secrets = [
    Secret(
        deploy_type="env",
        deploy_target="BUCKET",
        secret="minio-secret",
        key="BUCKET",
    ),
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
    Secret(
        deploy_type="env",
        deploy_target="MANIFEST_PG_HOST",
        secret="manifest-pg-secret",
        key="host",
    ),
    Secret(
        deploy_type="env",
        deploy_target="MANIFEST_PG_PORT",
        secret="manifest-pg-secret",
        key="port",
    ),
    Secret(
        deploy_type="env",
        deploy_target="MANIFEST_PG_DB",
        secret="manifest-pg-secret",
        key="db",
    ),
    Secret(
        deploy_type="env",
        deploy_target="CSV_INGESTION_BUCKET",
        secret="minio-secret",
        key="INGESTION_BUCKET",
    ),
]


with DAG(
    dag_id="transform_gdelt_csv",
    schedule="0 * * * *",
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "csv", "transform"],
    on_failure_callback=on_dag_failure_notify,
    max_active_runs=1,
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
    },
) as dag:
    # NOTE: See dyrmgraph-infra/docker/postgres/init.sql for transform_runs schema
    # 1. Claims max N records of transform_runs that are "ready", update those as "claimed"
    # 2. Reads latest version on the dates assoc with the claimed recods, if any (probably use two spark workers if handling two dates?)
    # TODO: if there is no claim, pass
    claim_transform_job = SQLExecuteQueryOperator(
        task_id="t1_claim_transform_job",
        conn_id="postgres_conn_id",
        handler=_handler_fetch_flat_list,
        do_xcom_push=True,
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
            WHERE tr.id = claimed.id
            RETURNING tr.id;
        """,
    )

    decide_skip_or_continue = SQLSkipBranchOperator(
        task_id="t2_decide_skip_or_continue",
        source_task_id="t1_claim_transform_job",
        skip_branch_id="t3_skip_if_no_claim",
        continue_branch_id="t3_perform_transformation",
    )

    skip_if_no_claim = EmptyOperator(task_id="t3_skip_if_no_claim")

    # 3. Normalizes the fundamental three tables
    # run 1 worker per date
    # Example partitions after transform: s3://bucket/silver/documents/partition_date=2026-07-20/part-0000.parquet
    # fmt: off
    perform_transformation = KubernetesPodOperator(
        task_id="t3_perform_transformation",
        kubernetes_conn_id="kubernetes_conn_id",
        name="transform-{{ ts_nodash }}",
        namespace="dyrmgraph",
        image="xuanminator/dyrmgraph_transform:0.9",
        cmds=["/opt/spark/bin/spark-submit"],
        # configs are merged (this and the builder() config from the app)
        # spark-submit should specify provision-related configs
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc:443",
            "--deploy-mode", "cluster",

            "--num-executors", "2",
            "--executor-cores", "2",
            "--executor-memory", "4g",
            "--driver-memory", "2g",

            "--conf", "spark.kubernetes.namespace=dyrmgraph",
            "--conf", "spark.kubernetes.container.image=xuanminator/dyrmgraph_transform:0.9",
            "--conf", "spark.kubernetes.container.image.pullPolicy=IfNotPresent",
            "--conf", "spark.kubernetes.authenticate.driver.serviceAccountName=spark",

            # MinIO
            "--conf", "spark.hadoop.fs.s3a.endpoint=http://host.k3d.internal:9000",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf", "spark.hadoop.fs.s3a.endpoint.region=us-east-1",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",

            # Use these when running on cluster mode, but not needed for client mode
            # ENV-Driver
            "--conf", "spark.kubernetes.driverEnv.XCOM_PATH=s3a://system/xcom/{{ dag.dag_id }}/{{ run_id | replace(':', '_') | replace('/', '_') }}/return.json",
            "--conf", "spark.kubernetes.driver.secretKeyRef.MANIFEST_PG_USER=manifest-pg-secret:username",
            "--conf", "spark.kubernetes.driver.secretKeyRef.MANIFEST_PG_PASSWORD=manifest-pg-secret:password",
            "--conf", "spark.kubernetes.driver.secretKeyRef.MANIFEST_PG_DB=manifest-pg-secret:db",
            "--conf", "spark.kubernetes.driver.secretKeyRef.MANIFEST_PG_HOST=manifest-pg-secret:host",
            "--conf", "spark.kubernetes.driver.secretKeyRef.MANIFEST_PG_PORT=manifest-pg-secret:port",
            "--conf", "spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=minio-secret:AWS_ACCESS_KEY_ID",
            "--conf", "spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-secret:AWS_SECRET_ACCESS_KEY",
            "--conf", "spark.kubernetes.driver.secretKeyRef.BUCKET=minio-secret:BUCKET",
            "--conf", "spark.kubernetes.driver.secretKeyRef.CSV_INGESTION_BUCKET=minio-secret:INGESTION_BUCKET",
            # ENV-Executors
            "--conf", "spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=minio-secret:AWS_ACCESS_KEY_ID",
            "--conf", "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-secret:AWS_SECRET_ACCESS_KEY",

            "--conf", "spark.eventLog.enabled=true", # for spark history server
            "--conf", "spark.eventLog.dir=s3a://system/spark-events/",

            # Dependencies config
            # Now embeded inside docker image
            # "--packages", (
            #     "org.postgresql:postgresql:42.7.7,"
            #     "org.apache.hadoop:hadoop-aws:3.4.2"
            # ),
            # "--conf", "spark.jars.ivy=/tmp/.ivy2",
            # "--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2",

            "--class", "com.dyrmgraph.transform.GDELTTables",
            "local:///opt/transform/transform.jar",
        ],
        image_pull_policy="IfNotPresent",
        get_logs=True,
        on_finish_action="keep_pod", # debug
        # on_finish_action="delete_pod",
        deferrable=True,
        env_vars={  # maybe use configmaps instead?
            "S3_HOST": "http://host.k3d.internal:9000", # not used
        },
        secrets=secrets,
        # config_file="/opt/airflow/plugins/kubeconfig",  # mutually exclusive with AF connections config
        poll_interval=30.0,
        logging_interval=5,
        service_account_name="spark",
        do_xcom_push=False,
    )
    # fmt: on

    read_xcom = S3ReadObjectOperator(
        task_id="t4_read_transform_xcom",
        aws_conn_id="s3_conn_id",
        s3_bucket="system",
        s3_key="xcom/{{ dag.dag_id }}/{{ run_id | replace(':', '_') | replace('/', '_') }}/return.json",
    )

    next_step = DictBranchOperator(
        task_id="t5_next_step",
        source_task_id="t4_read_transform_xcom",
        key="status",
        fail_branch="t6_mark_transform_run_as_failed",
        branch_map={
            "is_success": "t6_mark_transform_run_as_success",
            "is_failed": "t6_mark_transform_run_as_failed",
        },
    )

    mark_as_success = SQLExecuteQueryOperator(
        task_id="t6_mark_transform_run_as_success",
        conn_id="postgres_conn_id",
        sql="""
            UPDATE transform_runs
            SET status = 'success'
            WHERE queued_by = '{{ dag_run.run_id }}'
        """,
        # NOTE: using dagrun id instead for stability at scale
        # parameters={"ids": claim_transform_job.output},
        # sql="""
        #     UPDATE transform_runs
        #     SET status = 'success'
        #     WHERE id = ANY(%(ids)s)
        # """,
    )

    mark_as_failed = SQLExecuteQueryOperator(
        task_id="t6_mark_transform_run_as_failed",
        conn_id="postgres_conn_id",
        # parameters={"ids": claim_transform_job.output},
        sql="""
            UPDATE transform_runs
            SET status = 'failed'
            WHERE queued_by = '{{ dag_run.run_id }}'
        """,
    )

    fail = FailOperator(task_id="t7_declare_dag_run_fail", json_output_source="t4_read_transform_xcom", on_failure_callback=push_and_log)

    claim_transform_job >> decide_skip_or_continue
    decide_skip_or_continue >> [skip_if_no_claim, perform_transformation]
    perform_transformation >> read_xcom >> next_step
    next_step >> [mark_as_success, mark_as_failed]
    mark_as_failed >> fail


# NOTE: If the re-published old records -> hash would be different but the date would be the same
