import json
import logging
from os import environ

import pendulum
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.providers.discord.notifications.embed import (
    Embed,
    EmbedAuthor,
    EmbedField,
    EmbedProvider,
)
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.context import Context

logger = logging.getLogger(__name__)


def push_and_log(context: Context) -> None:
    """
    Takes a failed upstream task's id and pushes the failed task's payload to the DLQ Redis List for later processing.
    This callback is for tasks that has ONE upstream tasks.
    """
    logger.info("Firing task failure callback")

    ti = context["ti"]
    failed_task_id = context["task"].upstream_task_ids.pop()

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


# NOTE: DagRun-level on_failure_callack does not output exceptions from callbacks.
# Use it in default_args for debugging
def on_dag_failure_notify(context: Context) -> None:
    """
    This callback is for dags' on_failure_callback
    """
    logger.info("Firing dag failure callback")
    try:
        dag_run = context["dag_run"]
        url = f"{environ['AIRFLOW_UI_URL']}/dags/{dag_run.dag_id}/runs/{dag_run.run_id}"

        discord_embed = Embed(
            title="DAG FAILURE",
            url=url,
            timestamp=(dag_run.end_date or pendulum.now()).isoformat(),
            color=15548997,
            provider=EmbedProvider(
                name="Apache Airflow", url="https://airflow.apache.org"
            ),
            author=EmbedAuthor(name="Pikku Myy the Orchestrator"),
            fields=[
                EmbedField({"name": "dag", "value": dag_run.dag_id, "inline": True}),
                EmbedField({"name": "run_id", "value": dag_run.run_id, "inline": True}),
                EmbedField(
                    {
                        "name": "start_date",
                        "value": dag_run.start_date.isoformat()
                        if dag_run.start_date
                        else "No Start Date",
                        "inline": True,
                    }
                ),
                EmbedField(
                    {
                        "name": "end_date",
                        "value": dag_run.end_date.isoformat()
                        if dag_run.end_date
                        else "No End Date",
                        "inline": True,
                    }
                ),
                EmbedField(
                    {
                        "name": "logical_date",
                        "value": context["logical_date"].isoformat(),
                        "inline": False,
                    }
                ),
                EmbedField(
                    {
                        "name": "reason",
                        "value": str(context.get("exception")),
                        "inline": False,
                    }
                ),
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
        logger.exception("Notification failure")
        # Do not raise again
