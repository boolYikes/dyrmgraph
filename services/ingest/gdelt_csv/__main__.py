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
        manifest = os.environ["MANIFEST"]
        hashes, unzipped_files = run(
            download(manifest, Path(os.environ["CSV_DOWNLOAD_PATH"]), Path(os.environ["CSV_PERM_PATH"]))
        )

        valid_hashes = validate(hashes, Path(os.environ["CSV_DOWNLOAD_PATH"]))

        result = {"status": None, "manifest": manifest, "files": valid_hashes}
        # TODO: Consider a compensating-action or reconciliation strategy.
        # S3 uploads and archive DB updates are not atomic across systems.
        # Currently, if load_to_s3() succeeds but archive() fails,
        # the system can be left in a partially-completed state:
        # "object exists in S3 but not in the archive file list".
        #
        # Airflow retries are effectively self-healing because re-uploading
        # the same key overwrites the existing object and archive() can be
        # attempted again, but S3 and the archive DB are not updated atomically.
        load_to_s3(unzipped_files)
        try:
            inserted = archive(valid_hashes, cur)
            if inserted == 3:
                result["status"] = "is_new_file"
            elif 0 <= inserted < 3:
                result["status"] = "is_dupe_file"
            else:
                logging.exception(f"Unexpected row count: {inserted}")
                result["status"] = "is_failed"  # more than 3 ?
        except Exception as e:
            cur.connection.rollback()  # the contextmanager won't roll back because I intercept the exception
            logging.error(f"Error occurred while inserting file records to DB: {e}")
            result["status"] = "is_failed"

        # NOTE: what if pickling fails?? rollback?!
        pickle_path = os.path.join(os.environ["PICKLE_PATH"], "csv_file_result.pkl")
        pickle_temp = pickle_path + ".tmp"
        try:
            pickle_and_dump({**result, "files": list(map(lambda p: str(p), result["files"]))}, pickle_temp)
            os.replace(pickle_temp, pickle_path)  # atomic pickle
        except Exception as e:
            logging.exception(f"Error occurred while pickling and dumping result: {e}")
            # NOTE: if it raised, and if the business logic succeeded, Airflow would retry this DAG regardless!
            # raise ValueError(f"Failed to pickle and dump result: {e}")


# TODO: batch processing logic with argparse
if __name__ == "__main__":
    main()
