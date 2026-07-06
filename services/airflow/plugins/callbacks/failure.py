import json

from airflow.utils.context import Context


def push_and_log(context: Context) -> None:
    # NOTE: Maybe this belongs to FailOperator because it specifically handles a specific task failure
    from airflow.plugins.helpers.clients import get_redis_client

    ti = context["ti"]
    failed_task_id = ti["failed_task_id"]

    payload = ti.xcom_pull(task_ids=failed_task_id)  # fetch the whole xcom

    exception = context["exception"]

    final = {
        **(payload or {}),  # although, it is guaranteed not empty
        "reason": str(exception),
    }

    with get_redis_client() as r:
        r.lpush("dlq:manifest", json.dumps(final))
