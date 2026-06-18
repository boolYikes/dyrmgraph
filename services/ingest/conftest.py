import tempfile

import pytest


@pytest.fixture(scope="session")
def tmp_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_parsed_line():
    return (
        "58655",
        "07eb757d3e080cef2446507e67d015fe",
        "http://data.gdeltproject.org/gdeltv2/20260612051500.export.CSV.zip",
        "20260612051500.export.CSV",
        "20260612051500",
        "CSV",
    )


@pytest.fixture
def sample_manifest_line():
    return "58655 07eb757d3e080cef2446507e67d015fe http://data.gdeltproject.org/gdeltv2/20260612051500.export.CSV.zip"


@pytest.fixture
def sample_pickle():
    return {
        "status": "is_new_manifest",
        "dt": "20260618063000",
        "files": [
            {
                "hash": "48a7b954f6574e8c4aed4e3537e81edf",
                "size": "65564",
                "url": "http://data.gdeltproject.org/gdeltv2/20260618063000.export.CSV.zip",
                "basename": "20260618063000",
                "format": "zip",
            },
            {
                "hash": "84cc74d99e45d6ca7cbfdb7728753b8e",
                "size": "75293",
                "url": "http://data.gdeltproject.org/gdeltv2/20260618063000.mentions.CSV.zip",
                "basename": "20260618063000",
                "format": "zip",
            },
            {
                "hash": "8455457ebd552dad9938291e29045dfd",
                "size": "4149074",
                "url": "http://data.gdeltproject.org/gdeltv2/20260618063000.gkg.csv.zip",
                "basename": "20260618063000",
                "format": "zip",
            },
        ],
    }
