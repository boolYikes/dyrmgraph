import logging
import os

from ingest.libs.db_utils import create_table, get_conn, insert_record
from ingest.libs.gdelt_utils import pickle_and_dump

from .check_latest import run_check_latest

logging.basicConfig(level=logging.INFO)


def main():
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
