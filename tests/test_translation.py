from unittest.mock import MagicMock
import pytest
import os
from typing import Dict
import sqlparse
from sql_translate import translation


def test_create_parent() -> None:
    _Translator = translation._Translator()


@pytest.mark.parametrize(['statement', 'expected'], [
    ("", ""),
    ("\n\t\n\n\n\n", ""),
    ("With a as (select b from c) INSERT INTO table d.e PARTITION (f='g') SELECT d from a",
     "With a as (select b from c) INSERT INTO table d.e PARTITION (f='g') SELECT d from a")
])
def test_translate_statement(statement: str, expected: str) -> None:
    Translation = translation.HiveToPresto()
    Translation.Formatter.format_query = MagicMock(side_effect=lambda x: x)
    Translation.GlobalTranslator.translate_query = MagicMock(side_effect=lambda x: x)
    Translation.RecursiveTranslator.translate_query = MagicMock(side_effect=lambda x, **kwargs: x)
    Translation.GlobalTranslator.move_insert_statement = MagicMock(side_effect=lambda x: x)
    result = Translation.translate_statement(statement)
    assert result == expected


def test_translate_statement_NotImplementedError() -> None:
    Translation = translation.HiveToPresto()
    print(Translation.Formatter.format_query)
    Translation.Formatter.format_query = MagicMock(side_effect=Exception)
    Translation.GlobalTranslator.translate_query = MagicMock(side_effect=lambda x: x)
    Translation.RecursiveTranslator.translate_query = MagicMock(side_effect=lambda x: x)
    with pytest.raises(NotImplementedError):
        Translation.translate_statement("select something from db.table;select col from cte")


def test_translate_statement_Exception() -> None:
    Translation = translation.HiveToPresto()
    Translation.Formatter.format_query = MagicMock(side_effect=Exception)
    Translation.GlobalTranslator.translate_query = MagicMock(side_effect=lambda x: x)
    Translation.RecursiveTranslator.translate_query = MagicMock(side_effect=lambda x: x)
    with pytest.raises(Exception):
        Translation.translate_statement("select something from db.table")
    with pytest.raises(Exception):
        Translation.translate_statement("select something from db.table", verbose=True)


def test_translate_file() -> None:
    Translation = translation.HiveToPresto()
    Translation.translate_statement = MagicMock(side_effect=lambda x: x)
    path_file = os.path.join(os.path.dirname(__file__), "samples", "translation", "file_to_translate.sql")
    path_translated_file = Translation.translate_file(path_file)
    with open(path_translated_file) as f:
        translated_data = f.read()
    assert translated_data == "with a as (select b from c) insert into table d.e PARTITION (f='g') select d from a"
