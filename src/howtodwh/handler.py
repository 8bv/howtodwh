from datetime import datetime, date
from typing import Union

from sqlalchemy import select, Table
from sqlalchemy.engine import Connection

SupportsIncrement = Union[int, date, datetime, None]


def get_snapshot(connection: Connection, table: Table):
    stmt = select(table)
    result = connection.execute(stmt)
    return result.fetchall()


def get_increment(connection: Connection, table: Table, increment_key: str, increment_value: SupportsIncrement):
    stmt = select(table)
    if increment_value is not None:
        stmt = stmt.where(table.c[increment_key] >= increment_value)

    result = connection.execute(stmt)
    return result.fetchall()


def compare(connection: Connection, table1: Table, table2: Table):
    ...

