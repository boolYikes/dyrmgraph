import json
from unittest.mock import patch

from callbacks.failure import push_and_log


@patch("airflow.plugins.helpers.clients.get_redis_client")
def test_push_and_log(mock_get_redis_client, context, task_instance, redis_client):
    mock_get_redis_client.return_value.__enter__.return_value = redis_client

    push_and_log(context)

    task_instance.xcom_pull.assert_called_once_with(task_ids="extract_manifest")

    redis_client.lpush.assert_called_once_with(
        "dlq:manifest",
        json.dumps(
            {
                "manifest_id": "123",
                "bucket": "my-bucket",
                "reason": "boom",
            }
        ),
    )
