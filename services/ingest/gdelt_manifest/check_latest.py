from ingest.libs.db_utils import is_done
from ingest.libs.gdelt_utils import ManifestStatusError, parse_line


def get_latest_manifest(url: str):
    import requests

    response = requests.get(url)
    response.raise_for_status()  # fail the task so the DAG can retry

    return response.text


def get_manifest_statuses(cursor, raw_manifest_text: str):
    txt_lines = raw_manifest_text.splitlines()

    # example file content:
    # 58655 07eb757d3e080cef2446507e67d015fe http://data.gdeltproject.org/gdeltv2/20260612051500.export.CSV.zip
    # 82808 907baf2cbbf1d33662ec2f6ab55819fe http://data.gdeltproject.org/gdeltv2/20260612051500.mentions.CSV.zip
    # 4175051 e7bda5c86bd64cbe6cb3859bea3a4d29 http://data.gdeltproject.org/gdeltv2/20260612051500.gkg.csv.zip

    # NOTE: Atomic manifest: if even one file is new, than treat the manifest as new
    statuses = []
    files = []
    for line in txt_lines:
        parsed = parse_line(line)
        if not parsed:
            statuses.append("is_failed")
            files.append({})
            continue

        size, hash_value, url, basename, dt, format = parsed

        # Only check if the input manifest is the latest and not a dupe.
        # contiguity check is done in a detached DAG.
        # ticking the manifest as done will be done in the downstream DAG for downloading the csv.
        is_dupe = is_done(cursor, hash_value)
        statuses.append("is_dupe_manifest" if is_dupe else "is_new_manifest")

        files.append(
            {
                "hash": hash_value,
                "size": size,
                "url": url,
                "basename": basename,
                "format": format,
            }
        )
    return dt, statuses, files


def decide_final_status(statuses: list[str]):
    status = "is_failed"

    # failure > new > dupe
    if any(s == "is_failed" for s in statuses):
        status = "is_failed"
    elif any(s == "is_new_manifest" for s in statuses):
        status = "is_new_manifest"
    elif all(s == "is_dupe_manifest" for s in statuses):
        status = "is_dupe_manifest"
    else:
        raise ManifestStatusError(f"Unexpected status string: {statuses}")

    if not statuses:
        raise ManifestStatusError("No statuses collected")

    return status


def run_check_latest(cursor):
    from os import environ

    url = environ.get("MANIFEST_URL", "http://data.gdeltproject.org/gdeltv2/lastupdate.txt")

    raw_txt_content = get_latest_manifest(url)
    dt, statuses, files = get_manifest_statuses(cursor, raw_txt_content)

    status = decide_final_status(statuses)

    return {"status": status, "dt": dt, "files": files}
