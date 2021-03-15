from collections import namedtuple
import pytest
import unittest
import pyodbc
from typing import Optional
from unittest.mock import MagicMock
from sql_translate import query_utils


def helper():
    raise Exception


def test_run_query():
    conn = MagicMock()
    query_utils.run_query('select * from db.table', conn)
    assert conn.cursor.called


def test_run_query_Exception():
    conn = MagicMock()
    conn.cursor = helper
    with pytest.raises(Exception):
        query_utils.run_query('select * from db.table', conn)


def test_fetch() -> None:
    conn = MagicMock()
    query_utils.fetch('select * from db.table', conn)
    assert conn.cursor.called


def test_fetch_Exception():
    conn = MagicMock()
    conn.cursor = helper
    with pytest.raises(Exception):
        query_utils.fetch('select * from db.table', conn)


def test_fetch_many():
    conn = MagicMock()
    query_utils.fetch('select * from db.table', conn, 1)
    assert conn.cursor.called
