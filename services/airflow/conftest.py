from unittest.mock import MagicMock

import pytest


@pytest.fixture
def task_instance():
    ti = MagicMock()

    ti.__getitem__.side_effect = {
        "failed_task_id": "extract_manifest",
    }.__getitem__

    ti.xcom_pull.return_value = {
        "manifest_id": "123",
        "bucket": "my-bucket",
    }

    return ti


@pytest.fixture
def redis_client():
    return MagicMock()


@pytest.fixture
def context(task_instance):
    mock_task_operator = MagicMock()
    mock_task_operator.upstream_task_ids = {"mock_upstream_id"}
    return {
        "ti": task_instance,
        "task": mock_task_operator,
        "exception": RuntimeError("boom"),
    }
