import json
import logging

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.redis.hooks.redis import RedisHook


class FailOperator(BaseOperator):
    """
    For clean automatic failure and handing down the failed task id to the on_failure_callback function
    """

    def execute(self, context):
        ti = context["ti"]
        failed_task_id = context["task"].upstream_task_ids.pop()  # only one top task
        ti.xcom_push(key="failed_task_id", value=failed_task_id)  # now the callback can use this
        raise AirflowFailException("Manifest check did not run succesfully.")  # callback is called here


# TODO: parameterize things
class DLQAggregator(BaseOperator):
    """
    Aggregates DQL messages from Redis Lists atomically.
    """

    def __init__(self, batch_size=10, **kwargs):
        super().__init__(**kwargs)
        self.batch_size = batch_size

    def execute(self, context):
        r = RedisHook("redis_conn_id").get_conn()
        ti = context["ti"]

        # Flush new messages to processing first
        while True:
            item = r.lmove("dlq:new", "dlq:processing", "RIGHT", "LEFT")
            if not item:
                break

        n_items_to_process = r.llen("dlq:processing")
        n_items_to_notify = min(self.batch_size, n_items_to_process)

        items = []
        try:
            if n_items_to_notify == 0:
                ti.xcom_push(key="messages_exist", value="false")
                items = []
            else:
                ti.xcom_push(key="messages_exist", value="true")
                items = r.lrange("dlq:processing", -n_items_to_notify, -1)
                items = [i.decode("utf-8") if isinstance(i, bytes) else i for i in items]
        except Exception as e:
            logging.error(e)

        # TODO: prettier discord message

        ti.xcom_push(key="processed_dlq_messages", value=json.dumps(items))
        ti.xcom_push(key="n_processed_messages", value=len(items))  # for cleanup


class DLQCleaner(BaseOperator):
    template_fields = ("n_messages",)

    def __init__(self, n_messages: str, **kwargs):
        super().__init__(**kwargs)
        self.n_messages = n_messages

    def execute(self, context):
        r = RedisHook("redis_conn_id").get_conn()
        logging.info(f"Cleaning up {self.n_messages} messages from dlq:processing")
        for _ in range(int(self.n_messages)):
            r.rpop("dlq:processing")
