import json
import logging
import os
from asyncio import run
from pathlib import Path

from ingest.libs.db_utils import get_conn
from ingest.libs.gdelt_utils import pickle_and_dump

from .run import archive, download, load_to_s3, validate

logging.basicConfig(level=logging.INFO)


def main():
    # NOTE: processes 3 latest files.
    logging.info("Downloading and processing a new file...")
    with get_conn() as cur:
        # NOTE: this is heavy and maybe haven't much use aside from dt. Consider revision later
        manifest = json.loads(os.environ["MANIFEST"])

        hashes, unzipped_files = run(
            download(manifest, Path(os.environ["CSV_DOWNLOAD_PATH"]), Path(os.environ["CSV_PERM_PATH"]))
        )

        valid_hashes = validate(hashes, Path(os.environ["CSV_DOWNLOAD_PATH"]))

        result = {"status": None, "manifest": manifest, "files": valid_hashes}
        # NOTE: Needed a compensating-action or reconciliation strategy.
        # S3 uploads and archive DB updates are not atomic across systems.
        # Currently, if load_to_s3() succeeds but archive() fails,
        # the system can be left in a partially-completed state:
        # "object exists in S3 but not in the archive file list".
        #
        # to that end, a cleanup marking step was added to the exception below
        try:
            load_to_s3(unzipped_files)
            ingestion_id = os.environ["RUN_ID"]
            inserted = archive(valid_hashes, cur, ingestion_id)
            if inserted == 3:
                result["status"] = "is_new_file"
            elif 0 <= inserted < 3:
                result["status"] = "is_dupe_file"
            else:
                logging.exception(f"Unexpected row count: {inserted}")
                result["status"] = "is_failed"  # more than 3 ?
                raise

        except Exception as e:
            cur.connection.rollback()  # the contextmanager won't roll back because I intercept the exception

            result["status"] = "is_failed"
            logging.exception(f"Error occurred: {e}")
            # NOTE: if it raised, and if the business logic succeeded, Airflow would retry this DAG regardless!
            # raise ValueError(f"Failed to pickle and dump result: {e}")

        try:
            pickle_path = os.path.join(os.environ["PICKLE_PATH"], "csv_file_result.pkl")
            pickle_temp = pickle_path + ".tmp"
            pickle_and_dump({**result, "files": list(map(lambda p: str(p), result["files"]))}, pickle_temp)
            os.replace(pickle_temp, pickle_path)  # atomic pickle
        except Exception as e:
            cur.connection.rollback()
            logging.exception(f"Failed to pickle the result: {e}")
            raise  # let's just raise when pickling itself fails


# TODO: batch processing logic with argparse
if __name__ == "__main__":
    main()
