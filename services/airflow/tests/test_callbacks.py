import json
from unittest.mock import MagicMock, call, patch

import pendulum
from airflow.exceptions import AirflowException
from callbacks.failure import on_dag_failure_notify, push_and_log


def test_push_and_log(context, task_instance):
    with patch("callbacks.failure.RedisHook") as mock_hook:
        hook_instance = mock_hook.return_value
        mock_client = MagicMock()
        hook_instance.get_conn.return_value = mock_client

        push_and_log(context)

        task_instance.xcom_pull.assert_called_once_with(
            task_ids="mock_upstream_id", key="mock_upstream_id"
        )

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


def test_on_dag_failure_notify(monkeypatch):
    monkeypatch.setenv("AIRFLOW_UI_URL", "http://mock.url")
    monkeypatch.setenv("DISCORD_HOOK", "webhook/mock_key")

    mock_dag_run = MagicMock()
    mock_dag_run.dag_id = "mock_dag_id"
    mock_dag_run.run_id = "mock_run_id"
    mock_dag_run.start_date = pendulum.DateTime(2026, 7, 11, 14, 15, 0)
    mock_dag_run.end_date = pendulum.DateTime(2026, 7, 11, 14, 15, 30)
    fake_logical_date = pendulum.DateTime(2026, 7, 11, 14, 0, 0)
    mock_exception = AirflowException("Mock Exception")

    fake_context = {
        "dag_run": mock_dag_run,
        "logical_date": fake_logical_date,
        "exception": mock_exception,
    }

    with (
        patch("callbacks.failure.Embed") as MockEmbed,
        patch("callbacks.failure.EmbedField") as MockEmbedField,
        patch("callbacks.failure.EmbedProvider") as MockEmbedProvider,
        patch("callbacks.failure.EmbedAuthor") as MockEmbedAuthor,
        patch("callbacks.failure.DiscordWebhookHook") as MockDiscordHook,
    ):
        mock_embed_instance = MagicMock()
        MockEmbed.return_value = mock_embed_instance

        MockEmbedField.return_value = MagicMock()

        mock_discord_instance = MagicMock()
        MockDiscordHook.return_value = mock_discord_instance

        mock_provider_instance = MagicMock()
        MockEmbedProvider.return_value = mock_provider_instance
        mock_author_instance = MagicMock()
        MockEmbedAuthor.return_value = mock_author_instance

        on_dag_failure_notify(fake_context)

        field_calls_to_assert = [
            call({"name": "dag", "value": mock_dag_run.dag_id, "inline": True}),
            call({"name": "run_id", "value": mock_dag_run.run_id, "inline": True}),
            call(
                {
                    "name": "start_date",
                    "value": mock_dag_run.start_date.isoformat(),
                    "inline": True,
                }
            ),
            call(
                {
                    "name": "end_date",
                    "value": mock_dag_run.end_date.isoformat(),
                    "inline": True,
                }
            ),
            call(
                {
                    "name": "logical_date",
                    "value": fake_context["logical_date"].isoformat(),
                    "inline": False,
                }
            ),
            call(
                {
                    "name": "reason",
                    "value": str(fake_context.get("exception")),
                    "inline": False,
                }
            ),
        ]

        assert MockEmbedField.call_args_list == field_calls_to_assert

        assert MockEmbedProvider.call_args_list == [
            call(name="Apache Airflow", url="https://airflow.apache.org")
        ]
        assert MockEmbedAuthor.call_args_list == [
            call(name="Pikku Myy the Orchestrator")
        ]

        MockEmbed.assert_called_once_with(
            title="DAG FAILURE",
            url="http://mock.url/dags/mock_dag_id/runs/mock_run_id",
            timestamp=mock_dag_run.end_date.isoformat(),
            color=15548997,
            provider=MockEmbedProvider.return_value,
            author=MockEmbedAuthor.return_value,
            fields=[MockEmbedField.return_value] * 6,
        )
        MockDiscordHook.assert_called_once_with(
            http_conn_id="discord_conn_id",
            webhook_endpoint="webhook/mock_key",
            username="Sassy Myy Bot",
            embed=mock_embed_instance,
        )
        mock_discord_instance.execute.assert_called_once()
