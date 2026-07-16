# TODO: need new create_table and insert_record
import logging
from asyncio import gather
from pathlib import Path

from ingest.models.manifest import Manifest
from libs.db_utils import create_csv_file_registry_table, insert_csv_file_record
from libs.gdelt_utils import compute_hash, download_file, unzip_csv
from psycopg2.extensions import cursor as Cursor

logging.basicConfig(level=logging.INFO)


async def download(manifest: str, csv_download_path: Path, csv_perm_path: Path):
    """
    Downloads latest csv files for the three GDELT tables, async.
    Validate manifest -> Get file info -> Download -> Unzip -> Return correct hashes
    """
    manifest_obj = Manifest.model_validate_json(manifest)

    coroutines = []
    correct_hashes: list[dict[str, str]] = []
    downloaded_files = []
    for file in manifest_obj.files:
        file_name = f"{manifest_obj.dt}.{file.basename}.{file.format}"
        correct_hashes.append({"file_name": file_name, "hash": file.hash})
        full_path = Path(csv_download_path) / file_name
        downloaded_files.append(full_path)
        coroutines.append(download_file(file.url, full_path))

    await gather(*coroutines)

    for file in downloaded_files:
        unzip_csv(file, csv_perm_path)

    return correct_hashes


def validate(correct_hashes: list[dict], csv_perm_path: Path):
    """compute target hash -> compare against correct hash -> return validated hashes"""
    valid_hashes: list[dict] = []
    for item in correct_hashes:
        full_path = csv_perm_path / item["file_name"]
        target = compute_hash(full_path)
        if item["hash"] == target:
            item["file_path"] = full_path
            valid_hashes.append(item)

    if not valid_hashes or len(valid_hashes) != 3:
        logging.error("Hashes do not match")
        raise ValueError("Hashes do not match")

    return valid_hashes


def archive(valid_hashes: list[dict], cursor: Cursor):
    """
    Meant for the latest manifest (3 files, fixed)
    query db for existing hash -> add to db if new -> return result
    """
    create_csv_file_registry_table(cursor)
    inserted = insert_csv_file_record(cursor, valid_hashes)  # must be 3
    return inserted


async def batch_download(dts: list):
    # import aiohttp
    # TBD
    ...
