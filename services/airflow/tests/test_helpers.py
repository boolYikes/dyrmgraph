from unittest.mock import MagicMock, patch

from airflow.plugins.helpers.clients import get_redis_client


def test_get_redis_client_creates_and_closes_client(monkeypatch):
    monkeypatch.setenv("REDIS_HOST", "localhost")
    monkeypatch.setenv("REDIS_PORT", "6379")

    mock_client = MagicMock()

    with patch("redis.Redis", return_value=mock_client) as mock_redis:
        with get_redis_client() as client:
            assert client is mock_client

        mock_redis.assert_called_once_with(
            host="localhost",
            port=6379,
            decode_responses=True,
        )
        mock_client.close.assert_called_once()
