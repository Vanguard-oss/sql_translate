import unittest
import pytest
import sqlparse
from sql_translate.engine import error_handling
from typing import Dict, List
import re

E = error_handling._ErrorHandler()  # Just for coverage


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select cast(a as integer)",
     "line 1:8: Cannot cast timestamp to integer (1)",
     "select to_unixtime(a)"),
    ("select cast(a as integer) as a",
     "line 1:8: Cannot cast timestamp to integer (1)",
     "select to_unixtime(a) AS a")
])
def test_cast_timestamp_to_epoch(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._cast_timestamp_to_epoch]
    assert ErrorHandlerHiveToPresto._cast_timestamp_to_epoch(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select 1 in (select '1')",
     "line 1:10: value and result of subquery must be of the same type for IN expression: integer vs varchar (1)",
     "select cast(1 AS varchar) in (select '1')")
])
def test_cast_in_subquery(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._cast_in_subquery]
    assert ErrorHandlerHiveToPresto._cast_in_subquery(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select coalesce('1', 1)",
     "line 1:22: All COALESCE operands must be the same type: varchar (1)",
     "select coalesce('1', cast(1 AS varchar))")
])
def test_coalesce_statements(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._coalesce_statements]
    assert ErrorHandlerHiveToPresto._coalesce_statements(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select case when true then 'a' else 1 end",
     "line 1:37: All CASE results must be the same type: varchar (1)",
     "select case when true then 'a' else cast(1 AS varchar) end")
])
def test_case_statements(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._case_statements]
    assert ErrorHandlerHiveToPresto._case_statements(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select a \nfrom cte\nwhere a in (  \n 1, 2, 3)",
     "line 4:2: IN value and list items must be the same type: bigint (1)",
     "select a \nfrom cte\nwhere cast(a AS bigint) in (  \n 1, 2, 3)"),
    ("select a \nfrom cte\nwhere a in (1.1, 2.3, 3.1)",
     "line 3:13: IN value and list items must be the same type: float (1)",
     "select a \nfrom cte\nwhere cast(a AS double) in (1.1, 2.3, 3.1)"),
    ("select a \nfrom cte\nwhere a in ('1', '2', '3')",
     "line 3:13: IN value and list items must be the same type: varchar (1)",
     "select a \nfrom cte\nwhere cast(a AS varchar) in ('1', '2', '3')"),
    ("select a \nfrom cte\nwhere a in ('1')",
     "line 3:13: IN value and list items must be the same type: varchar (1)",
     "select a \nfrom cte\nwhere cast(a AS varchar) in ('1')")
])
def test_cast_in(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._cast_in]
    assert ErrorHandlerHiveToPresto._cast_in(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message'], [
    ("select a \nfrom cte\nwhere a in (  \n 1, '2', 3)",
     "line 4:2: IN value and list items must be the same type: bigint (1)")
])
def test_cast_in_ValueError(statement: str, error_message: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._cast_in]
    with pytest.raises(ValueError):
        ErrorHandlerHiveToPresto._cast_in(statement, re.search(pattern[0], error_message))


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select cast(a as integer) from cte",
     "line 1:8: Cannot cast char(10) to integer (1)",
     "select cast(trim(cast(a AS varchar)) AS integer) from cte"),
    ("select a from cte",
     "line 1:8: Cannot cast bigint to integer (1)",
     "select a from cte")
])
def test_cannot_cast_to_type(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._cannot_cast_to_type]
    assert ErrorHandlerHiveToPresto._cannot_cast_to_type(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select a from cte where b between c and d",
     "line 1:27: Cannot check if varchar is BETWEEN varchar and date (1)",
     "select a from cte where b between c and cast(d AS varchar)"),
    ("select a from cte where b between c and d",
     "line 1:27: Cannot check if double is BETWEEN double and date (1)",
     "select a from cte where cast(b AS varchar) between cast(c AS varchar) and cast(d AS varchar)")
])
def test_between(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._between]
    assert ErrorHandlerHiveToPresto._between(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select a from db.vcte",  # Table name start with v --> try with "t" as this could be a view
     "Table 'db.vcte' not found (1)",
     "select a from db.tcte"),
    ("select a from db.cte",
     "Table 'db.cte' not found (1)",
     "select a from db.cte_presto")  # Table name does not start with v
])
def test_table_not_found(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._table_not_found]
    assert ErrorHandlerHiveToPresto._table_not_found(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select concat(1, '1') from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e",
     "line 1:8: Unexpected parameters (bigint, varchar) for function concat (1)",
     "select concat(cast(1 AS varchar), cast('1' AS varchar)) from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e"),
    ("select concat(max(1), '1') from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e",
     "line 1:8: Unexpected parameters (bigint, varchar) for function concat (1)",
     "select concat(cast(max(1) AS varchar), cast('1' AS varchar)) from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e")
])
def test_unexpected_parameters(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._unexpected_parameters]
    assert ErrorHandlerHiveToPresto._unexpected_parameters(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'error_message'], [
    ("select something(1, '1') from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e",
     "line 1:8: Unexpected parameters (bigint, varchar) for function something (1)"),
    ("select concat(a - b, a or b) from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e",
     "line 1:8: Unexpected parameters (bigint, varchar) for function concat (1)")
])
def test_unexpected_parameters_NotImplementedError(statement: str, error_message: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._unexpected_parameters]
    with pytest.raises(NotImplementedError):
        ErrorHandlerHiveToPresto._unexpected_parameters(statement, re.search(pattern[0], error_message))


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select 'a' =1",
     "line 1:12: '=' cannot be applied to varchar, bigint (1)",
     "select 'a' =cast(1 AS varchar)"),
    ("select 'a' >1",
     "line 1:12: '>' cannot be applied to varchar, bigint (1)",
     "select 'a' >cast(1 AS varchar)"),
    ("select 'a' <1",
     "line 1:12: '<' cannot be applied to varchar, bigint (1)",
     "select 'a' <cast(1 AS varchar)"),
    ("select 'a' >=1",
     "line 1:12: '>=' cannot be applied to varchar, bigint (1)",
     "select 'a' >=cast(1 AS varchar)"),
    ("select 'a' <=1",
     "line 1:12: '<=' cannot be applied to varchar, bigint (1)",
     "select 'a' <=cast(1 AS varchar)"),
    ("select 'a' !=1",
     "line 1:12: '!=' cannot be applied to varchar, bigint (1)",
     "select 'a' !=cast(1 AS varchar)"),
    ("select a from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e",
     "line 2:18: '=' cannot be applied to bigint, varchar (1)",
     "select a from b inner join c\n      ON cast(a.my_col AS varchar)=b.another_col\nwhere d=e"),
    ("select a from b inner join c\n      ON a.my_col=b.another_col\nwhere d=e",
     "line 2:18: '=' cannot be applied to date, timestamp (1)",
     "select a from b inner join c\n      ON cast(a.my_col AS varchar)=cast(b.another_col AS varchar)\nwhere d=e"),
    ("select a\nfrom b\nwhere cast(event_date AS varchar)>='2021-01-21' AND event_date<='2021-01-23'",
     "line 3:63: '<=' cannot be applied to date, varchar(10) (1)",
     "select a\nfrom b\nwhere cast(event_date AS varchar)>='2021-01-21' AND cast(event_date AS varchar)<='2021-01-23'")
])
def test_cast_both_sides(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._cast_both_sides]
    assert ErrorHandlerHiveToPresto._cast_both_sides(statement, re.search(pattern[0], error_message)) == expected


@pytest.mark.parametrize(['statement', 'table_properties', 'expected'], [
    ("with cte AS (select b from cte2) select b from cte", {"columns": {"b": "bigint"}}, "with cte AS (select b from cte2) SELECT\nb\nfrom cte"),  # No wildcard
    ("with cte AS (select b from cte2) select count(*) as b from cte", {"columns": {"b": "bigint"}}, "with cte AS (select b from cte2) SELECT\ncount(*) AS b\nfrom cte"),  # No wildcard
    ("with cte AS (select b from cte2) select   * from cte",
     {"columns": {"c": "bigint", "d": "bigint", "e": "bigint", "a": "bigint"}},
     "with cte AS (select b from cte2) SELECT\nc,\nd,\ne,\na\nfrom cte"),  # select *
    ("with cte AS (select b from cte2) select  a.*, c as d from cte",  # Wildcard then regular column
     {"columns": {"b": "bigint", "d": "varchar"}},
     "with cte AS (select b from cte2) SELECT\nb,\nc AS d\nfrom cte"),
    ("with cte AS (select b from cte2) select c as d, * from cte",  # regular column then wildcard
     {"columns": {"b": "bigint", "d": "varchar"}},
     "with cte AS (select b from cte2) SELECT\nc AS d,\nb\nfrom cte"),
    ("with cte AS (select b, c from cte2) select foo(a) as d, *, cte.a, `hey yo` from cte",  # Wildcard in the middle bringing 2+ columns in
     {"columns": {"a": "bigint", "c": "varchar", "some thing": "varchar", "d": "varchar", "hey yo": "varchar"}},
     "with cte AS (select b, c from cte2) SELECT\nfoo(a) AS d,\nc,\n`some thing`,\ncte.a,\n`hey yo`\nfrom cte")  # Final column order is not sorted by * replacement is
])
def test_expand_wildcards(statement: str, table_properties: Dict[str, str], expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    assert ErrorHandlerHiveToPresto._expand_wildcards(statement, table_properties) == expected


@pytest.mark.parametrize(['statement', 'table_properties'], [
    ("with cte AS (select b from cte2) select *, cte.* from cte", {"columns": {"b": "bigint"}})  # Double wildcard
])
def test_expand_wildcards_ValueError(statement: str, table_properties: Dict[str, str]) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    with pytest.raises(ValueError):
        ErrorHandlerHiveToPresto._expand_wildcards(statement, table_properties)


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("""with abc AS (
    SELECT b
    FROM c
    GROUP BY b)
    SELECT
    d, e, f, g, CURRENT_DATE AS hhh
    FROM abc a
    LEFT JOIN def b
    ON a.b = b.b""",
     "Mismatch at column 2: 'e' is of type bigint but expression is of type double (1)",
     """with abc AS (
    SELECT b
    FROM c
    GROUP BY b)
    SELECT
d,
cast(e AS bigint) AS e,
f,
g,
CURRENT_DATE AS hhh
FROM abc a
    LEFT JOIN def b
    ON a.b = b.b"""
     ),
    ("select name.my_col a from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e",
     "Mismatch at column 1: 'my_col' is of type char(1) but expression is of type smallint (1)",
     "SELECT\ncast(cast(name.my_col AS varchar) AS char(1)) AS a\nfrom b inner join c\n      ON name.my_col=b.another_col\nwhere d=e"),
    ("select name.my_col a from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e",
     "Mismatch at column 1: 'my_col' is of type varchar but expression is of type char(1) (1)",
     "SELECT\ncast(name.my_col AS varchar) AS a\nfrom b inner join c\n      ON name.my_col=b.another_col\nwhere d=e"),
    ("select name.my_col from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e",
     "Mismatch at column 1: 'my_col' is of type varchar but expression is of type char(1) (1)",
     "SELECT\ncast(name.my_col AS varchar) AS my_col\nfrom b inner join c\n      ON name.my_col=b.another_col\nwhere d=e"),
    ("with abc as (select name.my_col from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e) select j.my_col, k.my_col from abc",
     "Mismatch at column 2: 'my_col' is of type varchar but expression is of type char(1) (1)",
     "with abc as (select name.my_col from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e) SELECT\nj.my_col,\ncast(k.my_col AS varchar) AS my_col\nfrom abc"),
    ("with abc as (select name.my_col from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e) select my_col, case when a=1 then 'Y' else 'N' end from abc",
     "Mismatch at column 2: 'unknown_col' is of type varchar but expression is of type char(1) (1)",
     "with abc as (select name.my_col from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e) SELECT\nmy_col,\ncast(case when a=1 then 'Y' else 'N' end AS varchar) AS unknown_col\nfrom abc"),
    ("with abc as (select name.my_col from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e) select my_col, concat('a', max('b')) as c, k.my_col from abc",
     "Mismatch at column 2: 'c' is of type varchar but expression is of type bigint (1)",
     "with abc as (select name.my_col from b inner join c\n      ON name.my_col=b.another_col\nwhere d=e) SELECT\nmy_col,\ncast(concat('a', max('b')) AS varchar) AS c,\nk.my_col\nfrom abc"),
    ("select name.my_col, cte.my_col from name inner join cte\n      ON name.my_col=cte.my_col\nwhere d=e",
     "Mismatch at column 1: 'my_col' is of type char(1) but expression is of type smallint (1)",
     "SELECT\ncast(cast(name.my_col AS varchar) AS char(1)) AS my_col,\ncte.my_col\nfrom name inner join cte\n      ON name.my_col=cte.my_col\nwhere d=e")
])
def test_column_type_mismatch(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    pattern = [k for k, v in ErrorHandlerHiveToPresto.known_issues.items() if v == ErrorHandlerHiveToPresto._column_type_mismatch]
    assert ErrorHandlerHiveToPresto._column_type_mismatch(statement, re.search(pattern[0], error_message), temp_tgt_table_properties={"columns": {}}) == expected


@pytest.mark.parametrize(['statement', 'error_message', 'expected'], [
    ("select cast(a AS bigint) from cte", "line 1:8: Cannot cast timestamp to bigint (1)", "select to_unixtime(a) from cte")
])
def test_handle_errors(statement: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    assert ErrorHandlerHiveToPresto.handle_errors(statement, statement, error_message) == (expected, expected)


@pytest.mark.parametrize(['statement', 'original_sql', 'error_message', 'expected'], [
    ("select a, cast(a AS bigint) from cte", "select {a}, cast(a AS bigint) from cte", "line 1:11: Cannot cast timestamp to bigint (1)", "select a, to_unixtime(a) from cte")
])
def test_handle_errors_Exception(statement: str, original_sql: str, error_message: str, expected: str) -> None:
    ErrorHandlerHiveToPresto = error_handling.ErrorHandlerHiveToPresto()
    assert ErrorHandlerHiveToPresto.handle_errors(statement, original_sql, error_message) == (expected, "")
