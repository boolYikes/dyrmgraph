# from os import environ, path

from os import environ

from airflow import DAG
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from operators.fail import DLQAggregator

# from airflow.providers.docker.operators.docker import DockerOperator
# from airflow.providers.standard.operators.empty import EmptyOperator
# from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
# from operators.dict_branch import DictBranchOperator
# from operators.fail import FailOperator
from pendulum import datetime

with DAG(
    dag_id="check_dlq_and_notify",
    schedule="*/15 * * * *",
    catchup=False,
    start_date=datetime(2026, 6, 11, 0, 0, 0, tz="Asia/Seoul"),
    tags=["gdelt", "dlq", "alert", "logging"],
    default_args={
        "owner": "dyrmgraph_airflow",
        "retries": 0,
        "max_active_runs": 1,
    },
) as dag:
    # NOTE: A message must be removed only after a successful notification
    collect_messages = DLQAggregator(
        task_id="t1_collect_messages",
    )

    # Pop from redis list

    # NOTE: inject conn id at init
    send_to_discord = DiscordWebhookOperator(
        task_id="t2_send_to_discord",
        http_conn_id="discord_conn_id",
        webhook_endpoint=environ["DISCORD_HOOK"],
        message="{{ ti.xcom_pull(task_ids='t1_aggregate_dlq_messages', key='processed_dlq_messages') }}",
        username="Sassy Myy Bot",
        # avatar_url="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fwww.tavaratalomainio.fi%2Fstorage%2Fproduct_images%2F4%2FMUUMI-MATTO80CMPikkuMyy_6430049590554_9de121a99aef_1.webp&f=1&nofb=1&ipt=b3c16c6501bdc4d471567336e1234ab04e583e920ba083558e2359821e8d0e37",
    )

    # TODO: A clean up task for successfully processed messages
