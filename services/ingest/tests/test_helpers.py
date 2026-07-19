from pathlib import Path


# NOTE: tmp_path is predefined in pytest
def test_pickle_and_dump(tmp_path):
    from pickle import load

    from ingest.libs.gdelt_utils import pickle_and_dump

    data = {"key": "value"}
    file_path = Path(tmp_path) / "test.pkl"
    pickle_and_dump(data, file_path)

    with open(file_path, "rb") as f:
        loaded_data = load(f)

    assert loaded_data == data, f"Loaded data doesn't match: {loaded_data} != {data}"


def test_parse_line():
    from ingest.libs.gdelt_utils import parse_line

    line = "58655 07eb757d3e080cef2446507e67d015fe http://data.gdeltproject.org/gdeltv2/20260612051500.export.CSV.zip"
    expected_result = (
        "58655",
        "07eb757d3e080cef2446507e67d015fe",
        "http://data.gdeltproject.org/gdeltv2/20260612051500.export.CSV.zip",
        "20260612051500.export",
        "20260612051500",
        "CSV",
    )
    result = parse_line(line)
    assert result == expected_result, f"Parse results don't match: {result} != {expected_result}"


def test_read_meta(tmp_path):
    from ingest.libs.gdelt_utils import read_meta

    test_file = Path(tmp_path) / "test_meta.txt"
    lines = ["line1\n", "line2\n", "line3\n"]

    with open(test_file, "w") as f:
        f.writelines(lines)

    async def run_test():
        async for line in read_meta(test_file):
            assert line in lines, f"Line {line} not found in expected lines"

    from asyncio import run

    run(run_test())


def test_unzip_csv(tmp_path):
    from ingest.libs.gdelt_utils import unzip_csv

    # Create a sample CSV file and zip it
    csv_content = "col1,col2\nval1,val2\n"
    csv_file = Path(tmp_path) / "test.csv"
    zip_file = Path(tmp_path) / "test.csv.zip"

    with open(csv_file, "w") as f:
        f.write(csv_content)

    import zipfile

    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.write(csv_file, arcname="test.csv")

    # unzip the file
    unzip_csv(zip_file, Path(tmp_path))

    # Check if the unzipped file exists and has the correct content
    assert (Path(tmp_path) / "test.csv").exists(), "Unzipped CSV file does not exist"
    with open(Path(tmp_path) / "test.csv", "r") as f:
        unzipped_content = f.read()
        assert unzipped_content == csv_content, f"Unzipped content doesn't match: {unzipped_content} != {csv_content}"


def test_compute_hash(tmp_path):
    from ingest.libs.gdelt_utils import compute_hash

    # Create a sample file
    file_content = "sample content"
    test_file = Path(tmp_path) / "test_file.txt"

    with open(test_file, "w") as f:
        f.write(file_content)

    # Compute the hash
    computed_hash = compute_hash(test_file)

    # Compute the expected hash using hashlib directly
    import hashlib

    expected_hash = hashlib.md5(file_content.encode()).hexdigest()

    assert computed_hash == expected_hash, f"Computed hash doesn't match: {computed_hash} != {expected_hash}"
