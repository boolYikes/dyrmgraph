import tempfile

import pytest


@pytest.fixture(scope="session")
def tmp_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir
