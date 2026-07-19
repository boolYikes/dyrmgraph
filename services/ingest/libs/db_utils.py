# Must guarantee contiguous dates ALL THE TIME
# TODO: Use SQLAlchemy if queries get out of hands. These are temporary
from contextlib import contextmanager
from os import environ

import pendulum
from psycopg2 import connect
from psycopg2.extensions import cursor as Cursor
from psycopg2.extras import execute_values


# NOTE: the name is a bit misleading but this actually is a get_cursor
@contextmanager
def get_conn():
    conn = connect(
        database=environ["MANIFEST_PG_DB"],
        user=environ["MANIFEST_PG_USER"],
        password=environ["MANIFEST_PG_PASSWORD"],
        host=environ["MANIFEST_PG_HOST"],
        port=environ["MANIFEST_PG_PORT"],
    )
    try:
        yield conn.cursor()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# NOTE: next time be more specific?!
def create_table(cursor: Cursor):
    # cursor.execute("PRAGMA journal_mode=WAL") # only in sqlite

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS manifest_registry (
        hash TEXT PRIMARY KEY,
        size INTEGER,
        url TEXT,
        basename TEXT,
        filedate TIMESTAMPTZ,
        fileformat TEXT,
        processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_manifest_registry_processed_at ON manifest_registry(processed_at)
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_manifest_registry_filedate ON manifest_registry(filedate)")


def insert_record(cursor: Cursor, result):
    from pendulum import from_format

    # all files are already guaranteed greenlight
    if result["status"] == "is_new_manifest":
        dt = from_format(result["dt"], "YYYYMMDDHHmmss")
        for f in result["files"]:
            cursor.execute(
                """
                INSERT INTO manifest_registry (hash, size, url, basename, filedate, fileformat)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (hash) DO NOTHING
            """,
                (f["hash"], int(f["size"]), f["url"], f["basename"], dt, f["format"]),
            )


def is_done(cursor: Cursor, hash):
    cursor.execute("SELECT 1 FROM manifest_registry WHERE hash = %s AND processed_at IS NOT NULL", (hash,))
    result = cursor.fetchone()
    return result is not None


def create_csv_file_registry_table(cursor: Cursor):
    # NOTE: maybe use hash indexes if this is going to be a equality-only lookup?
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS csv_file_registry (
        hash TEXT,
        basename TEXT,
        file_path TEXT,
        processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (hash, basename)
    )
    """)  # NOTE: basename, in this case (the right side of unique()), is not indexed alone in pg!, e.g.: WHERE basename = 'xxx' is not guaranteed O(logn)


def insert_csv_file_record(cursor: Cursor, valid_files: list[dict]):
    """
    Tries to insert the file records and naturally invalidates dupe
    Succeeds on hash+basename composite dupe
    """
    # NOTE: fast and forgiving on dupe hashes + diff file names -> handled in a separate dag
    execute_values(
        cursor,
        """
        INSERT INTO csv_file_registry (hash, basename, file_path)
        VALUES %s
        ON CONFLICT (hash, basename) DO NOTHING
        RETURNING hash, basename
        """,
        [(f["hash"], f["file_name"], str(f["file_path"])) for f in valid_files],
    )
    inserted = cursor.fetchall()
    return len(inserted)


# NOTE: this is not used?!
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
