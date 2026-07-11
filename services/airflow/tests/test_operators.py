# NOTE: Gonna be a boilerplate. Optimize later
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowFailException
from operators.dict_branch import DictBranchOperator
from operators.fail import DLQAggregator, DLQCleaner, FailOperator
from pytest import raises


def test_fail_operator():

    fail_operator = FailOperator(task_id="test_fail_operator")

    mock_ti = MagicMock()

    mock_task = MagicMock()
    mock_task.upstream_task_ids = {"mock_task_id"}

    mock_context = {"ti": mock_ti, "task": mock_task}

    with raises(AirflowFailException):
        fail_operator.execute(mock_context)

    mock_ti.xcom_push.assert_called_once_with(key="failed_task_id", value="mock_task_id")


def test_dict_branch_operator():
    dict_branch_operator = DictBranchOperator(
        task_id="test_dict_branch_operator",
        source_task_id="mock_source_task_id",
        key="mock_status",
        branch_map={
            "mock_status_1": "mock_downstream_task_1",
            "mock_status_2": "mock_downstream_task_2",
        },
    )

    # first branch
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = {"mock_status": "mock_status_1"}
    mock_context = {
        "ti": mock_ti,
    }

    result = dict_branch_operator.choose_branch(mock_context)
    mock_ti.xcom_pull.assert_called_once_with(task_ids="mock_source_task_id")
    assert result == "mock_downstream_task_1"

    # second branch
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = {"mock_status": "mock_status_2"}
    mock_context = {"ti": mock_ti}

    result = dict_branch_operator.choose_branch(mock_context)
    mock_ti.xcom_pull.assert_called_once_with(task_ids="mock_source_task_id")
    assert result == "mock_downstream_task_2"


def test_dlq_aggregator():
    fake_xcom_soldiers: list[dict] = []

    def _xcom_push_side_effect(key, value):
        fake_xcom_soldiers.append({key: value})

    mock_ti = MagicMock()
    mock_ti.xcom_push.side_effect = _xcom_push_side_effect
    fake_context = {"ti": mock_ti}

    source = [{"num": 1}, {"num": 2}, {"num": 3}]
    dest = []

    def _lmove_side_effect(src, dst, lfrom, lto):
        if not source:
            return None
        else:
            item = source.pop()
            dest.append(item)
            return item

    with patch("operators.fail.RedisHook") as mock_hook:
        mock_task = DLQAggregator(task_id="mock_dlq_aggregator")
        mock_hook_instance = mock_hook.return_value
        mock_redis_client = MagicMock()
        mock_redis_client.lmove.side_effect = _lmove_side_effect
        mock_redis_client.llen.return_value = 3
        mock_redis_client.lrange.return_value = [{"num": 1}, {"num": 2}, {"num": 3}]
        mock_hook_instance.get_conn.return_value = mock_redis_client

        mock_task.execute(fake_context)

        mock_redis_client.lrange.assert_called_once_with("dlq:processing", -3, -1)
        mock_hook.assert_called_once_with("redis_conn_id")
        assert dest == [{"num": 3}, {"num": 2}, {"num": 1}]
        assert fake_xcom_soldiers == [
            {"processed_dlq_messages": '[{"num": 1}, {"num": 2}, {"num": 3}]'},
            {"n_processed_messages": 3},
        ]


def test_dlq_cleaner():
    mock_ti = MagicMock()
    fake_context = {"ti": mock_ti}
    N_MESSAGES = 5

    with patch("operators.fail.RedisHook") as mock_hook:
        mock_task = DLQCleaner(task_id="mock_dlq_cleaner", n_messages=N_MESSAGES)
        mock_hook_instance = mock_hook.return_value
        mock_redis_client = MagicMock()
        mock_hook_instance.get_conn.return_value = mock_redis_client

        mock_task.execute(fake_context)

        mock_redis_client.rpop.assert_called_with("dlq:processing")
        assert mock_redis_client.rpop.call_count == N_MESSAGES
        mock_hook.assert_called_once_with("redis_conn_id")
