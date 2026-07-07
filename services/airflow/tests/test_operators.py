from unittest.mock import MagicMock

from airflow.exceptions import AirflowFailException
from airflow.plugins.operators.dict_branch import DictBranchOperator
from airflow.plugins.operators.fail import FailOperator
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
