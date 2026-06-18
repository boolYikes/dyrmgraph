from services.ingest.libs.gdelt_utils import parse_line


# parse line
def test_parse_line(sample_manifest_line, sample_parsed_line):
    result = parse_line(sample_manifest_line)
    assert result == sample_parsed_line, f"Parse results don't match: {result} != {sample_parsed_line}"


# read meta file line by line
# NOTE: not used yet

# pickle
# NOTE: not tested due to simplicity
