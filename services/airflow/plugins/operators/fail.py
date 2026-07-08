from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.plugins.helpers.clients import get_redis_client


class FailOperator(BaseOperator):
    """
    For clean automatic failure and handing down the failed task id to the on_failure_callback function
    """

    def execute(self, context):
        ti = context["ti"]
        failed_task_id = context["task"].upstream_task_ids.pop()  # only one top task
        ti.xcom_push(key="failed_task_id", value=failed_task_id)  # now the callback can use this
        raise AirflowFailException("Manifest check did not run succesfully.")  # callback is called here


class DLQAggregator(BaseOperator):
    """
    Aggregates DQL messages from Redis Lists atomically.
    """

    with get_redis_client() as r:
        # TODO: lmove to other list -> process -> delete the item on success, retain on failure
        r.lmove(...)
