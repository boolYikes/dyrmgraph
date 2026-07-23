import json
import logging
from os import environ

import pendulum
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.providers.discord.notifications.embed import Embed, EmbedField
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.context import Context


def push_and_log(context: Context) -> None:
    """
    Takes a failed upstream task's id and pushes the failed task's payload to the DLQ Redis List for later processing.
    This callback is for tasks that has upstream tasks.
    """
    logging.info("Firing task failure callback")

    ti = context["ti"]
    failed_task_id = ti["failed_task_id"]

    payload = ti.xcom_pull(
        task_ids=failed_task_id, key=failed_task_id
    )  # fetch the return value from the failed task which is the unpickled payload

    exception = context["exception"]

    # NOTE: the payload will be {"status": xx, "dt": xx, "files": xx}
    final = {
        **(payload or {}),  # although, it is guaranteed not empty
        "reason": str(exception),
    }

    h = RedisHook("redis_conn_id")
    r = h.get_conn()
    r.lpush("dlq:new", json.dumps(final))


def on_dag_failure_notify(context: Context) -> None:
    """
    This callback is for dags' on_failure_callback
    """
    logging.info("Firing dag failure callback")
    try:
        dag_run = context["dag_run"]
        url = f"{environ['AIRFLOW_UI_URL']}/dags/{dag_run.dag_id}/runs/{dag_run.run_id}"

        discord_embed = Embed(
            title="DAG FAILURE",
            url=url,
            timestamp=(dag_run.end_date or pendulum.now()).isoformat(),
            color="red",
            provider="Apache Airflow",
            author="Pikku Myy the Orchestrator",
            fields=[
                EmbedField("dag", dag_run.dag_id, True),
                EmbedField("run_id", dag_run.run_id, True),
                EmbedField(
                    "start_date",
                    dag_run.start_date.isoformat() if dag_run.start_date else None,
                    True,
                ),
                EmbedField(
                    "end_date",
                    dag_run.end_date.isoformat() if dag_run.end_date else None,
                    True,
                ),
                EmbedField("logical_date", context["logical_date"].isoformat(), False),
                EmbedField("reason", str(context.get("exception")), False),
            ],
        )

        d = DiscordWebhookHook(
            http_conn_id="discord_conn_id",
            webhook_endpoint=environ["DISCORD_HOOK"],
            username="Sassy Myy Bot",
            embed=discord_embed,
        )
        d.execute()

    except Exception:
        logging.exception("Notification failure")
        # Do not raise again
