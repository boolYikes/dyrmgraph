# Must guarantee contiguous dates ALL THE TIME
from contextlib import contextmanager
from os import environ

import pendulum
from psycopg2 import connect
from psycopg2.extensions import cursor as Cursor


@contextmanager
def get_conn():
    conn = connect(
        database=environ.get("POSTGRES_DB"),
        user=environ.get("POSTGRES_USER"),
        password=environ.get("POSTGRES_PASSWORD"),
        host=environ.get("POSTGRES_HOST"),
        port=environ.get("POSTGRES_PORT"),
    )
    try:
        yield conn.cursor()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Enable WAL -> better concurrency for RW
def create_table(cursor: Cursor):
    # cursor.execute("PRAGMA journal_mode=WAL") # only in sqlite

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manifest_registry (
        hash TEXT PRIMARY KEY,
        size INTEGER,
        url TEXT,
        basename TEXT,
        filedate INTEGER,
        fileformat TEXT,
        processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_manifest_registry_processed_at ON manifest_registry(processed_at)
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_manifest_registry_filedate ON manifest_registry(filedate)")


def insert_record(cursor: Cursor, hash: str, size: str, url: str, basename: str, filedate: str, fileformat: str):
    cursor.execute(
        """
        INSERT OR IGNORE INTO manifest_registry (hash, size, url, basename, filedate, fileformat)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (hash, int(size), url, basename, int(filedate), fileformat),
    )


def is_done(cursor: Cursor, hash):
    cursor.execute("SELECT 1 FROM manifest_registry WHERE hash = %s AND processed_at IS NOT NULL", (hash,))
    result = cursor.fetchone()
    return result is not None


def is_contiguous(cursor: Cursor, latest_date: str, interval=15):
    cursor.execute("SELECT MAX(filedate) FROM manifest_registry")
    row = cursor.fetchone()

    if row is None or row[0] is None:
        return False

    local_latest = pendulum.from_format(str(row[0]), "YYYYMMDDHHmmss")
    new_latest = pendulum.from_format(latest_date, "YYYYMMDDHHmmss")
    # if the diff is bigger than 15m, the date is not contiguous -> must retry whole manifest download
    # if the diff is smaller than 15m, then either the their api is publishing weird latest or my local manifest is corrupt -> retry
    # either way it's retry -> hence boolean
    return new_latest.diff(local_latest).in_minutes() == interval


# For ad hoc tests
if __name__ == "__main__":
    db_path = "./test.db"
    with get_conn(db_path) as cur:
        create_table(cur)
