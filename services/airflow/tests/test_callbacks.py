import json
from unittest.mock import MagicMock, patch

from callbacks.failure import push_and_log


def test_push_and_log(context, task_instance):
    with patch("callbacks.failure.RedisHook") as mock_hook:
        hook_instance = mock_hook.return_value
        mock_client = MagicMock()
        hook_instance.get_conn.return_value = mock_client

        push_and_log(context)

        task_instance.xcom_pull.assert_called_once_with(task_ids="extract_manifest")

        mock_client.lpush.assert_called_once_with(
            "dlq:new",
            json.dumps(
                {
                    "manifest_id": "123",
                    "bucket": "my-bucket",
                    "reason": "boom",
                }
            ),
        )
