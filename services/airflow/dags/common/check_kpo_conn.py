from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from callbacks.failure import on_dag_failure_notify
from pendulum import datetime

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
    dag_id="check_kpo_connection",
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "check", "temporary", "external"],
    on_failure_callback=on_dag_failure_notify,
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
    },
) as dag:
    perform_transformation = KubernetesPodOperator(
        task_id="t1_check_cluster_connectivity",
        kubernetes_conn_id="kubernetes_conn_id",
        name="check-{{ ts_nodash }}",
        namespace="dyrmgraph",
        image="curlimages/curl",
        cmds=["curl"],
        arguments=["-f", "http://192.168.0.100:9000/minio/health/live"],
        image_pull_policy="IfNotPresent",
        get_logs=True,
        on_finish_action="delete_pod",
        deferrable=True,
        env_vars={  # maybe use configmaps instead?
            "S3_HOST": "http://192.168.0.100:9000",
            "BUCKET": "gdelt-silver",
        },
        secrets=secrets,
        # config_file="/opt/airflow/plugins/kubeconfig",  # mounted from ~/.kube/config
        poll_interval=30.0,
        logging_interval=5,
    )
