import argparse
import logging
import os

from ingest.libs.db_utils import create_table, get_conn, insert_record

# TODO: decide pickle temp path at airflow cfg level -> from docker compose
from ingest.libs.gdelt_utils import pickle_and_dump

from .check_latest import run_check_latest
from .handle_failed import run_handle_failed

logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(
        description="Check if the latest GDELT manifest file is uploaded and trigger downstream DAGs accordingly."
    )
    parser.add_argument("--handle-failed", help="Handle failed cases instead of checking for new manifest files.")
    args = parser.parse_args()

    failed_file_info = args.handle_failed

    if args.handle_failed and not failed_file_info:
        # TODO: File format TBD
        logging.error("Must provide --handle-failed with the failed file info in xxx format.")

    elif args.handle_failed and failed_file_info:
        logging.info("Handling failed cases with the provided file info...")
        with get_conn() as cur:
            run_handle_failed(cur, failed_file_info)

    else:
        logging.info("Checking if the latest GDELT manifest file is uploaded...")
        with get_conn() as cur:  # All must succeed or rollback
            create_table(cur)
            result = run_check_latest(cur)
            insert_record(cur, result)
            pickle_path = os.path.join(os.environ["PICKLE_PATH"], "result.pkl")
            pickle_temp = pickle_path + ".tmp"
            try:
                pickle_and_dump(result, pickle_temp)
                os.replace(pickle_temp, pickle_path)  # atomic pickle
            except Exception as e:
                logging.error(f"Error occurred while pickling and dumping result: {e}")
                raise ValueError(f"Failed to pickle and dump result: {e}")


if __name__ == "__main__":
    main()
