def _handler_fetch_as_dict(cursor):
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _handler_fetch_flat_list(cursor):
    return [row[0] for row in cursor.fetchall()]
