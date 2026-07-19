from os import environ
from unittest.mock import MagicMock, patch

from ingest.libs.storage_utils import get_client, get_file, init_storage, put_file


def test_get_client(monkeypatch):
    monkeypatch.setenv("MINIO_HOST", "localhost")
    monkeypatch.setenv("MINIO_PORT", "9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")

    with patch("ingest.libs.storage_utils.Minio") as mock_minio:
        mock_minio.return_value = MagicMock()
        client = get_client()
        assert client is not None
        mock_minio.assert_called_once_with(
            endpoint=f"{environ['MINIO_HOST']}:{environ['MINIO_PORT']}",
            access_key=environ["MINIO_ACCESS_KEY"],
            secret_key=environ["MINIO_SECRET_KEY"],
            secure=False,
        )


def test_init_storage():
    mock_client = MagicMock()
    mock_client.bucket_exists.return_value = False
    init_storage(mock_client, "test-bucket")
    mock_client.make_bucket.assert_called_once_with("test-bucket")

    mock_fail_client = MagicMock()
    mock_fail_client.bucket_exists.return_value = True
    init_storage(mock_fail_client, "test-bucket")
    mock_fail_client.make_bucket.assert_not_called()


def test_get_file():
    mock_client = MagicMock()
    get_file(mock_client, "test-bucket", "test-object", "test-file")
    mock_client.fget_object.assert_called_once_with("test-bucket", "test-object", "test-file")


def test_put_file():
    mock_client = MagicMock()
    put_file(mock_client, "test-bucket", "test-object", "test-file")
    mock_client.fput_object.assert_called_once_with("test-bucket", "test-object", "test-file")
