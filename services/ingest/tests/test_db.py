from unittest.mock import MagicMock, patch


def normalize_sql(sql: str):
    return " ".join(sql.split()).strip()


def test_get_conn():
    from ingest.libs.db_utils import get_conn

    with patch("ingest.libs.db_utils.connect") as mock_connect:
        mock_connect.return_value = MagicMock()
        mock_connect.return_value.cursor.return_value = MagicMock()
        mock_cursor = mock_connect.return_value.cursor.return_value
        with get_conn() as cursor:
            assert cursor is mock_cursor
        mock_connect.assert_called_once()
        mock_connect.return_value.commit.assert_called_once()
        mock_connect.return_value.close.assert_called_once()


def test_create_table():
    from ingest.libs.db_utils import create_table

    mock_cursor = MagicMock()
    create_table(mock_cursor)

    executed_sql = [
        normalize_sql(args[0]) for args, kwargs in mock_cursor.execute.call_args_list
    ]

    assert executed_sql == [
        normalize_sql("""
            CREATE TABLE IF NOT EXISTS manifest_registry (
                hash TEXT PRIMARY KEY,
                size INTEGER,
                url TEXT,
                basename TEXT,
                filedate TIMESTAMPTZ,
                fileformat TEXT,
                processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """),
        normalize_sql("""
            CREATE INDEX IF NOT EXISTS
            idx_manifest_registry_processed_at
            ON manifest_registry(processed_at)
        """),
        normalize_sql("""
            CREATE INDEX IF NOT EXISTS
            idx_manifest_registry_filedate
            ON manifest_registry(filedate)
        """),
    ]


def test_insert_record():
    from ingest.libs.db_utils import insert_record

    mock_cursor = MagicMock()
    result = {
        "status": "is_new_manifest",
        "dt": "20240101123000",
        "files": [
            {
                "hash": "abc123",
                "size": 12345,
                "url": "http://example.com/file1",
                "basename": "file1.txt",
                "format": "txt",
            },
            {
                "hash": "def456",
                "size": 67890,
                "url": "http://example.com/file2",
                "basename": "file2.txt",
                "format": "txt",
            },
        ],
    }

    insert_record(mock_cursor, result)

    executed_sql = [
        normalize_sql(args[0]) for args, kwargs in mock_cursor.execute.call_args_list
    ]

    expected_sql = normalize_sql("""
        INSERT INTO manifest_registry (hash, size, url, basename, filedate, fileformat)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (hash) DO NOTHING
    """)

    assert all(sql == expected_sql for sql in executed_sql)


def test_insert_record_with_non_new_manifest():
    from ingest.libs.db_utils import insert_record

    mock_cursor = MagicMock()
    result = {
        "status": "not_new_manifest",
        "dt": "20240101123000",
        "files": [
            {
                "hash": "abc123",
                "size": 12345,
                "url": "http://example.com/file1",
                "basename": "file1.txt",
                "format": "txt",
            }
        ],
    }

    insert_record(mock_cursor, result)

    # Ensure that no SQL execution occurred for non-new manifests
    mock_cursor.execute.assert_not_called()


def test_is_done():
    from ingest.libs.db_utils import is_done

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)  # Simulate a record found

    result = is_done(mock_cursor, "abc123")

    mock_cursor.execute.assert_called_once_with(
        "SELECT 1 FROM manifest_registry WHERE hash = %s AND processed_at IS NOT NULL",
        ("abc123",),
    )
    assert result is True

    # Test when no record is found
    mock_cursor.fetchone.return_value = None
    result = is_done(mock_cursor, "def456")
    assert result is False


def test_create_csv_file_registry_table():
    from ingest.libs.db_utils import create_csv_file_registry_table

    mock_cursor = MagicMock()
    create_csv_file_registry_table(mock_cursor)

    executed_sql = [
        normalize_sql(args[0]) for args, kwargs in mock_cursor.execute.call_args_list
    ]

    assert executed_sql == [
        normalize_sql("""
            CREATE TABLE IF NOT EXISTS csv_file_registry (
                id BIGSERIAL PRIMARY KEY,
                ingestion_id TEXT,
                hash TEXT,
                object_name TEXT,
                object_path TEXT,
                status TEXT,
                processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """),
        normalize_sql("""
            CREATE INDEX IF NOT EXISTS idx_csv_file_registry_ingestion_id
            ON csv_file_registry (ingestion_id)
        """),
        normalize_sql("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_csv_file_registry_hash_active
            ON csv_file_registry (hash, object_name)
            WHERE status IS NULL
        """),
    ]


def test_insert_csv_file_record():
    from ingest.libs.db_utils import insert_csv_file_record

    with patch("ingest.libs.db_utils.execute_values") as mock_execute_values:
        mock_cursor = MagicMock()
        record = [
            {
                "hash": "abc123",
                "file_name": "file1.csv",
                "file_path": "/path/to/file1.csv",
            }
        ]

        insert_csv_file_record(mock_cursor, record)

        mock_execute_values.assert_called_once()
        args, kwargs = mock_execute_values.call_args
        executed_sql = normalize_sql(args[1])
        expected_sql = normalize_sql("""
            INSERT INTO csv_file_registry (ingestion_id, hash, object_name, object_path)
            VALUES %s
            ON CONFLICT (hash, object_name) WHERE status IS NULL
            DO NOTHING
            RETURNING hash, object_name
        """)
        assert executed_sql == expected_sql


# NOTE: is_contiguos is not used currently
