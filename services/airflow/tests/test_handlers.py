from unittest.mock import MagicMock

from handlers.sql_fetch import _handler_fetch_as_dict, _handler_fetch_flat_list


def test_fetch_as_dict():
    mock_description = (
        ("string_col1", 25),  # mock the col name and type code in description
        ("int_col", 23),
        ("string_col2", 25),
    )
    mock_fetchall = [("r1_c1", 12, "r1_c4"), ("r2_c1", 22, "r2_d2")]
    mock_cursor = MagicMock()
    mock_cursor.description = mock_description
    mock_cursor.fetchall.return_value = mock_fetchall
    res = _handler_fetch_as_dict(mock_cursor)
    correct = [{"string_col1": "r1_c1", "int_col": 12, "string_col2": "r1_c4"}, {"string_col1": "r2_c1", "int_col": 22, "string_col2": "r2_d2"}]
    assert res == correct, f"should be {correct}"


def test_fetch_flat_list():
    mock_fetchall = [
        ("r1_id", "r1_c2", "r1_c3", "r1_c4"),
        ("r2_id", "r2_c2", "r2_c3", "r2_d2"),  # 🤖
    ]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_fetchall
    correct = ["r1_id", "r2_id"]
    res = _handler_fetch_flat_list(mock_cursor)
    assert res == correct, f"should be {correct}"
