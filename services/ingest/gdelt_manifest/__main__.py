import argparse
import logging

from ingest.gdelt_manifest.check_latest import run_check_latest
from ingest.gdelt_manifest.handle_failed import run_handle_failed

# TODO: decide pickle temp path at airflow cfg level -> from docker compose
from ingest.libs.gdelt_utils import pickle_and_dump

logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(
        description="Check if the latest GDELT manifest file is uploaded and trigger downstream DAGs accordingly."
    )
    parser.add_argument(
        "--handle-failed", action="store_true", help="Handle failed cases instead of checking for new manifest files."
    )
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
        pickle_and_dump(result, "somepath/result.pkl")
