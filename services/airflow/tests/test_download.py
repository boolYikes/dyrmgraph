import os

import aiofiles
import aiohttp
import pytest
from mock import AsyncMock, MagicMock, patch

from libs.common.airflow_utils import download_file


@pytest.mark.asyncio
async def test_download_file(tmp_path):
    URL = "http://example.com/file.txt"
    EXPECTED = b"testing\n"

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.content.iter_chunked = MagicMock()

    async def chunk_iter():
        yield EXPECTED
        yield b"should not be written"

    mock_response.content.iter_chunked.return_value = chunk_iter()

    mock_get_ctx = AsyncMock()
    mock_get_ctx.__aenter__.return_value = mock_response
    mock_get_ctx.__aexit__.return_value = None

    mock_session = MagicMock()
    mock_session.get.return_value = mock_get_ctx

    mock_session_ctx = AsyncMock()
    mock_session_ctx.__aenter__.return_value = mock_session
    mock_session_ctx.__aexit__.return_value = None

    PATH = os.path.join(tmp_path, "download_test.txt")
    with patch("libs.injestion.airflow_utils.aiohttp.ClientSession", return_value=mock_session_ctx):
        await download_file(URL, PATH, test=True)

    async with aiofiles.open(PATH, "rb") as f:
        content = await f.read()

    assert content == EXPECTED
    mock_response.raise_for_status.assert_called_once()
    mock_session.get.assert_called_once_with(URL)


@pytest.mark.asyncio
async def test_download_file_raises_on_http_error(tmp_path):
    PATH = os.path.join(tmp_path, "download_test.txt")
    URL = "http://example.com/file.txt"

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=500, message="Eeek"
    )

    mock_get_ctx = AsyncMock()
    mock_get_ctx.__aenter__.return_value = mock_response
    mock_get_ctx.__aexit__.return_value = None

    mock_session = MagicMock()
    mock_session.get.return_value = mock_get_ctx

    mock_session_ctx = AsyncMock()
    mock_session_ctx.__aenter__.return_value = mock_session
    mock_session_ctx.__aexit__.return_value = None

    with patch("libs.injestion.airflow_utils.aiohttp.ClientSession", return_value=mock_session_ctx):
        with pytest.raises(aiohttp.ClientResponseError):
            await download_file(URL, PATH)
