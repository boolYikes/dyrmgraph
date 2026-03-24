# Must guarantee contiguous dates ALL THE TIME
import pathlib
from contextlib import contextmanager

import aiofiles
import pendulum
from psycopg2 import connect
from psycopg2.extensions import cursor as Cursor


@contextmanager
def get_conn():
    conn = connect(
        # TODO: parameterize this
        database="manifest_registry",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432,
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
    cursor.execute("PRAGMA journal_mode=WAL")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manifest_registry (
        hash TEXT PRIMARY KEY,
        size INTEGER,
        url TEXT,
        filename TEXT,
        filedate INTEGER,
        processed_at DATETIME
    )
    """)
    cursor.execute("""
    CREATE INDEX idx_manifest_registry_processed_at ON manifest_registry(processed_at)
    """)
    cursor.execute("CREATE INDEX idx_manifest_registry_filedate ON manifest_registry(filedate)")


def parse_line(line: str) -> tuple[str, str, str] | None:
    parts = line.split()
    if len(parts) == 3:
        url = parts[2].strip()
        filename = url.split("/")[-1]
        return (
            parts[0].strip(),  # size
            parts[1].strip(),  # hash
            url,
            filename,
            filename.split(".")[0],  # filedate, format: 20150218230000
        )


async def read_meta(path: pathlib.Path):
    async with aiofiles.open(path, "r") as f:
        while line := await f.readline():
            yield line


def insert_record(cursor: Cursor, hash: str, size: str, url: str, filename: str, filedate: str):
    cursor.execute(
        """
        INSERT OR IGNORE INTO manifest_registry (hash, size, url, filename, filedate)
        VALUES (?, ?, ?, ?, ?)
    """,
        (hash, int(size), url, filename, int(filedate)),
    )


def is_done(cursor: Cursor, hash):
    cursor.execute("SELECT 1 FROM manifest_registry WHERE hash = ? AND processed_at IS NOT NONE", (hash,))
    result = cursor.fetchone()
    return result is not None


def contiguity(cursor: Cursor, latest_date: str, interval=15):
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


def update_incrementally(db_path: str):
    """
    db_path: Object storage path for the registry
    """
    # on system init, pull .db file from the object storage
    cur = get_conn(db_path)
    create_table(cur)

    # on system shutdown, push .db file to object storage


# For ad hoc tests
if __name__ == "__main__":
    db_path = "./test.db"
    with get_conn(db_path) as cur:
        create_table(cur)
