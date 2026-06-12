def run_check_latest():

    # blabla

    # NOTE: example result
    result = {
        "status": "is_new_manifest",  # or "is_dupe_manifest" or "is_failed"
        "manifest_info": {
            "hash": "abc123",
            "size": 123456,
            "url": "http://example.com/manifest.csv",
            "filename": "manifest.csv",
            "filedate": 20250218230000,
        },
    }

    return result
