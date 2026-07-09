import json

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.context import Context


def push_and_log(context: Context) -> None:
    # NOTE: Maybe this belongs to FailOperator because it specifically handles a specific task failure

    ti = context["ti"]
    failed_task_id = ti["failed_task_id"]

    payload = ti.xcom_pull(task_ids=failed_task_id)  # fetch the whole xcom

    exception = context["exception"]

    final = {
        **(payload or {}),  # although, it is guaranteed not empty
        "reason": str(exception),
    }

    h = RedisHook("redis_conn_id")
    r = h.get_conn()
    r.lpush("dlq:new", json.dumps(final))
