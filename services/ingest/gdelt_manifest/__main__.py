import argparse
import logging

# TODO: decide pickle temp path at airflow cfg level -> from docker compose
from services.ingest.libs.gdelt_utils import pickle_and_dump

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
        run_handle_failed(failed_file_info)

    else:
        logging.info("Checking if the latest GDELT manifest file is uploaded...")
        result = run_check_latest()
        # TODO: decide pickle temp path at airflow cfg level -> from docker compose
        try:
            pickle_and_dump(result, "somepath/result.pkl")
        except Exception as e:
            logging.error(f"Error occurred while pickling and dumping result: {e}")
            raise ValueError(f"Failed to pickle and dump result: {e}")


if __name__ == "__main__":
    main()
