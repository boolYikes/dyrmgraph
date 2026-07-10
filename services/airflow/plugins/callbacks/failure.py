import json

from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.context import Context


def push_and_log(context: Context) -> None:
    """
    Takes a failed upstream task's id and pushes the failed task's payload to the DLQ Redis List for later processing.
    """

    ti = context["ti"]
    failed_task_id = ti["failed_task_id"]

    payload = ti.xcom_pull(
        task_ids=failed_task_id
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
