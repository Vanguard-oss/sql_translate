from typing import Dict
import os
import unittest
import pytest
from sql_translate.engine import sql_format

Formatter = sql_format.Formatter()


@pytest.mark.parametrize(['query', 'expected'], [
    (
        "",
        ""
    ),
    (
        "select distinct my_Table from b where c=d; -- comment",
        "SELECT DISTINCT my_table FROM b WHERE c=d"
    )
])
def test_format_query(query: str, expected: str) -> None:
    assert Formatter.format_query(query) == expected


def test_format_file() -> None:
    path = os.path.join(os.path.dirname(__file__), "..", "samples", "sql_format", "file_to_format.sql")
    path_formatted_file = Formatter.format_file(path)
    with open(path_formatted_file) as f:
        data = f.read()
    assert data == "SELECT DISTINCT my_table FROM b WHERE c=d"
