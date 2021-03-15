import unittest
import pytest
from typing import Union
import re
from sql_translate.engine import regex


def test_unexpected_behavior() -> None:
    pass  # Tested as part of the other two functions


@pytest.mark.parametrize(['pattern', 'string', 'strict', 'case_sensitive', 'expected'], [
    (r"h.", "Hello world!", True, False, "He"),
    (r"h.", "Hello world!", False, True, None),
    (r"H", "Hello world!", True, True, "H")
])
def test_search(pattern: str, string: str, strict: bool, case_sensitive: bool, expected: str) -> None:
    Regex = regex.Regex()
    output = Regex.search(pattern, string, strict=strict, case_sensitive=case_sensitive)
    if output:
        assert output.group(0) == expected
    else:
        assert output == expected


def test_search_Exception() -> None:
    Regex = regex.Regex()
    with pytest.raises(Exception):
        Regex.search(r"a", "Hello world!")


@pytest.mark.parametrize(['pattern', 'repl', 'string', 'strict', 'case_sensitive', 'expected'], [
    (r"WORLD", "you", "Hello WORLD!", True, True, "Hello you!"),
    (r"a", "you", "Hello world!", False, False, "Hello world!"),
    (r"\[(\d+)\]", lambda match: f"[{int(match.group(1))+1}]", "array[1]", True, False, "array[2]")
])
def test_sub(pattern: str, repl: Union[str, callable], string: str, strict: bool, case_sensitive: bool, expected: str) -> None:
    Regex = regex.Regex()
    assert Regex.sub(pattern, repl, string, strict=strict, case_sensitive=case_sensitive) == expected


def test_sub_Exception() -> None:
    Regex = regex.Regex()
    with pytest.raises(Exception):
        Regex.sub(r"a", "you", "Hello world!")
