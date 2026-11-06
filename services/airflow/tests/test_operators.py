from unittest.mock import MagicMock, call, patch

from airflow.exceptions import AirflowFailException
from operators.dict_branch import DictBranchOperator
from operators.fail import DLQAggregator, DLQCleaner, FailOperator
from operators.s3 import S3ReadObjectOperator
from operators.sql_branch import SQLSkipBranchOperator
from pytest import raises


def test_fail_operator():

    fail_operator = FailOperator(task_id="test_fail_operator", json_output_source="test_source_task_id")

    mock_ti = MagicMock()
    mock_ti.dag_id = "some_dag_id"
    mock_ti.run_id = "run_id"
    mock_xcom_pull_result = {"mock_result": "abcdefg"}
    mock_ti.xcom_pull.return_value = mock_xcom_pull_result

    mock_context = {"ti": mock_ti}

    with raises(AirflowFailException):
        fail_operator.execute(mock_context)

    mock_ti.xcom_push.assert_called_once_with(
        key="failed_task_dump",
        value={"dag_id": "some_dag_id", "run_id": "run_id", **mock_xcom_pull_result},
    )


def test_dict_branch_operator():
    dict_branch_operator = DictBranchOperator(
        task_id="test_dict_branch_operator",
        source_task_id="mock_source_task_id",
        key="status",
        branch_map={
            "mock_status_1": "mock_downstream_task_1",
            "mock_status_2": "mock_downstream_task_2",
        },
        fail_branch="mock_fail_branch",
    )

    # first branch
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = {"status": "mock_status_1"}
    mock_context = {
        "ti": mock_ti,
    }

    result = dict_branch_operator.choose_branch(mock_context)
    assert mock_ti.xcom_pull.call_args_list == [call(task_ids="mock_source_task_id"), call(task_ids="mock_source_task_id")]
    assert result == "mock_downstream_task_1"

    # second branch
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = {"status": "mock_status_2"}
    mock_context = {"ti": mock_ti}

    result = dict_branch_operator.choose_branch(mock_context)
    assert mock_ti.xcom_pull.call_args_list == [call(task_ids="mock_source_task_id"), call(task_ids="mock_source_task_id")]
    assert result == "mock_downstream_task_2"


def test_dict_branch_operator_with_non_status_key():
    dbo = DictBranchOperator(
        task_id="test_dict_branch_operator",
        source_task_id="mock_source_task_id",
        key="mock_key",
        branch_map={
            "mock_status_1": "mock_downstream_task_1",
            "mock_status_2": "mock_downstream_task_2",
        },
    )

    # first branch
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "mock_status_1"
    mock_context = {
        "ti": mock_ti,
    }

    result = dbo.choose_branch(mock_context)
    assert mock_ti.xcom_pull.call_args_list == [call(task_ids="mock_source_task_id"), call(task_ids="mock_source_task_id", key="mock_key")]
    assert result == "mock_downstream_task_1"

    # second branch
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "mock_status_2"
    mock_context = {"ti": mock_ti}

    result = dbo.choose_branch(mock_context)
    assert mock_ti.xcom_pull.call_args_list == [call(task_ids="mock_source_task_id"), call(task_ids="mock_source_task_id", key="mock_key")]
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
            {"messages_exist": "true"},
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


def test_s3_read_object_operator():
    with patch("operators.s3.S3Hook") as mock_hook:
        mock_hook_instance = mock_hook.return_value
        mock_content = '{"mock_key": "mock_value"}'
        mock_hook_instance.read_key.return_value = mock_content
        operator = S3ReadObjectOperator(
            task_id="mock_s3_read_object_operator",
            aws_conn_id="mock_conn_id",
            s3_bucket="mock_bucket",
            s3_key="mock-key/mock-result.json",
        )

        mock_context = {"ti": "mock_ti"}
        res = operator.execute(mock_context)
        assert res == {"mock_key": "mock_value"}
        mock_hook.assert_called_once_with(aws_conn_id="mock_conn_id")
        mock_hook_instance.read_key.assert_called_once_with(bucket_name="mock_bucket", key="mock-key/mock-result.json")


def test_sql_skip_branch_operator():
    operator = SQLSkipBranchOperator(
        task_id="mock_task_id",
        source_task_id="mock_source_task_id",
        skip_branch_id="mock_skip_branch_id",
        continue_branch_id="mock_continue_branch_id",
    )

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "mock_sql_result"
    mock_context = {"ti": mock_ti}
    res = operator.choose_branch(mock_context)
    assert res == "mock_continue_branch_id"
    mock_ti.xcom_pull.assert_called_once_with(task_ids="mock_source_task_id")

    mock_ti.xcom_pull.reset_mock()

    mock_ti.xcom_pull.return_value = ""
    res = operator.choose_branch(mock_context)
    assert res == "mock_skip_branch_id"
    mock_ti.xcom_pull.assert_called_once_with(task_ids="mock_source_task_id")
