import pytest
from requests.exceptions import InvalidSchema

from services.ingest.gdelt_manifest.check_latest import decide_final_status, get_latest_manifest, run_check_latest
from services.ingest.libs.gdelt_utils import ManifestStatusError
from services.ingest.models.manifest import Manifest


def test_manifest_latest_check(create_test_table, manifest_db_conn):
    # DB is always in a fresh state so this should always be is_new_manifest
    result = run_check_latest(manifest_db_conn.cursor())
    assert result["status"] == "is_new_manifest"

    # result schema check
    Manifest.model_validate(result)


def test_wrong_url_error():
    with pytest.raises(InvalidSchema):
        get_latest_manifest("htt://bad_url.nope")


def test_is_failed_is_atomic():
    """
    Should be is_failed if at least one file is flagged is_failed
    """
    test1 = ["is_failed", "is_failed", "is_failed"]
    test2 = ["is_failed", "is_dupe_manifest", "is_dupe_manifest"]
    test3 = ["is_new_manifest", "is_failed", "is_new_manifest"]
    test4 = ["is_dupe_manifest", "is_new_manifest", "is_failed"]
    for tc in [test1, test2, test3, test4]:
        status = decide_final_status(tc)
        assert status == "is_failed", f"Incorrect status inferrence for case {tc}: {status}. Should be 'is_failed'"


def test_is_new_is_returned_correctly():
    """
    Should only be inferred as a new one if there is no is_failed and at least one is is_new_manifest
    """
    test1 = ["is_failed", "is_new_manifest", "is_dupe_manifest"]
    test2 = ["is_new_manifest"] * 3
    test3 = ["is_dupe_manifest", "is_dupe_manifest", "is_new_manifest"]
    test4 = ["is_failed", "is_new_manifest", "is_failed"]

    correct = ["is_failed", "is_new_manifest", "is_new_manifest", "is_failed"]

    for i, tc in enumerate([test1, test2, test3, test4]):
        status = decide_final_status(tc)
        assert status == correct[i], f"Incorrect status inferrence for case {tc}: {status}. Should be '{correct[i]}'"


def test_is_dupe_is_returned_correctly():
    """
    Should only be inferred as a dupe if all three files are dupe
    """
    test = ["is_dupe_manifest"] * 3
    status = decide_final_status(test)
    assert status == "is_dupe_manifest", (
        f"Incorrect status inferrence for case {test}: {status}. Should be 'is_dupe_manifest'"
    )


def test_status_inferrence_raises_correct_error():
    with pytest.raises(ManifestStatusError):
        decide_final_status(["is_", "is__", "#$%"])
    with pytest.raises(ManifestStatusError):
        decide_final_status([])
