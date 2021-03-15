from unittest.mock import MagicMock
import pytest
import os
from typing import Dict, List, Tuple
import sqlparse
from sqlparse.tokens import Name, Literal
from sql_translate.engine import recursive_translation
from sql_translate import utils

Translation = recursive_translation.RecursiveHiveToPresto()


def test_create_parent() -> None:
    _RecursiveTranslator = recursive_translation._RecursiveTranslator()


@pytest.mark.parametrize(['query', 'has_insert_statement', 'expected'], [
    ("INSERT INTO TABLE test_db.test_table PARTITION (a='3') select * from db.table", True, {'partition_name': 'a', 'partition_value': "'3'"}),
    ("select * from db.table", False, {})
])
def test_translate_query(query: str, has_insert_statement: bool, expected: Dict) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    Translation._breakdown_query = MagicMock(return_value="")
    Translation.translate_query(query, has_insert_statement)
    assert Translation.partition_info == expected


def test_translate_query_ValueError() -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    with pytest.raises(ValueError):
        Translation.translate_query("INSERT INTO TABLE test_db.test_table select * from db1.table1;select * from db2.table2")


@pytest.mark.parametrize(['query'], [
    ("()",),
    ("(a)",),
    ("(a, b, c)",)
])
def test_breakdown_parenthesis(query: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    tokens = sqlparse.parse(query)[0][0]
    expected = query.strip("()")
    if expected == "":
        expected = []
    else:
        expected = sqlparse.parse(expected)[0][0].tokens
    result = Translation._breakdown_parenthesis(tokens)

    # Validation
    assert len(result) == len(expected)
    assert all([
        result[idx].value == expected[idx].value
        for idx in range(len(result))
    ])


@pytest.mark.parametrize(['statement', 'translation', 'output_type'], [
    ("lag   \n(page, 1)", "lag   \n(page, 1)", "any"),
    ("lag(page, 1)", "lag(page, 1)", "any"),
    ("concat(distinct a, b)", "concat(distinct cast(a AS varchar), cast(b AS varchar))", "varchar"),
    ("concat(a, timestamp '9999-12-31')", "concat(cast(a AS varchar), cast(timestamp '9999-12-31' AS varchar))", "varchar"),
    ("CONCAT(array_contains(a1, a2), cast(a2 AS varchar), '3', 4)", "concat(cast(contains(a1, a2) AS varchar), cast(a2 AS varchar), '3', '4')", "varchar"),
    ("concat(array_contains(a1, a2), cast(a2 AS varchar), '3', 4)", "concat(cast(contains(a1, a2) AS varchar), cast(a2 AS varchar), '3', '4')", "varchar"),
    ("concat_ws('_', a, b)",
     "substr(concat(case when cast(a AS varchar) is not null then '_' || cast(a AS varchar) else '' end, case when cast(b AS varchar) is not null then '_' || cast(b AS varchar) else '' end), 2)",
     "varchar"),
    ("date(a)", "date(cast(a AS timestamp))", "varchar"),
    ("date(my_array[1])", "date(cast(my_array[1] AS timestamp))", "varchar"),
    ("array_contains(concat(a1, a2), '3')", "contains(concat(cast(a1 AS varchar), cast(a2 AS varchar)), '3')", "boolean"),
    ("split('a', 'b')", "split('a', 'b')", "array(varchar)"),
    ("from_unixtime(1604355406)", "date_format(from_unixtime(1604355406), '%Y-%m-%d %H:%i:%S')", "varchar"),
    ("current_date()", "current_date", "timestamp"),
    ("current_timestamp()", "cast(current_timestamp AS timestamp)", "timestamp"),
    ("timestamp('9999-12-31')", "cast('9999-12-31' AS timestamp)", "timestamp"),
    ("year('2020-03-25')", "year(cast('2020-03-25' AS timestamp))", "bigint"),
    ("month('2020-03-25')", "month(cast('2020-03-25' AS timestamp))", "bigint"),
    ("day('2020-03-25')", "day(cast('2020-03-25' AS timestamp))", "bigint"),
    ("add_months('2020-03-25 16:32:01', 1)", "date_format(cast('2020-03-25 16:32:01' AS timestamp) + interval '1' month, '%Y-%m-%d')", "varchar"),
    ("regexp_extract('foothebar', 'foo(.*?)(bar)', 2)", "regexp_extract('foothebar', 'foo(.*?)(bar)', 2)", "varchar"),
    ("datediff('2020-03-26 01:35:01', '2020-03-25 23:32:01')",
     "date_diff('day', cast(cast('2020-03-25 23:32:01' AS timestamp) AS date), cast(cast('2020-03-26 01:35:01' AS timestamp) AS date))",
     "bigint"),
    ("date_add('2020-03-25 16:32:01', 1)",
     "date_add('day', 1, cast(cast('2020-03-25 16:32:01' AS timestamp) AS date))",
     "date"),
    ("date_sub('2020-03-25 16:32:01', 1)",
     "date_add('day', -1, cast(cast('2020-03-25 16:32:01' AS timestamp) AS date))",
     "date"),
    ("max(1)", "max(1)", "any"),
    ("min(1)", "min(1)", "any"),
    ("if(true, 1, 0)", "if(true, 1, 0)", "any"),
    ("if(a or array_contains(a, '2'), 1, 0)", "if(a or contains(a, '2'), 1, 0)", "any"),  # Boolean operator
    ("if(array_contains(a, '2')+1, 1, 0)", "if(contains(a, '2')+1, 1, 0)", "any"),  # Operation
    ("if(a=array_contains(a, '2'), 1, 0)", "if(a=contains(a, '2'), 1, 0)", "any"),  # Comparison
    ("unnest(split(cast(a AS varchar), ','))", "unnest(split(cast(a AS varchar), ','))", "any")
    # Test divisions casted to double
    # ("count(a/2)", "count(cast(a AS double)/cast(2 AS double))", "bigint")
])
def test_translate_function_regular(statement: str, translation: str, output_type: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    query = sqlparse.parse(statement)[0]  # Single query in this statement
    function = query.tokens[0]  # Single object in query
    assert Translation._translate_function(function.tokens) == (translation, output_type)   # We know it's a single query in this statement


@pytest.mark.parametrize(['statement', 'translation', 'output_type'], [
    ("cast(a as varchar)", "cast(a AS varchar)", "varchar"),
    ("cast(a as   string)", "cast(a AS varchar)", "varchar"),  # String is not supported as output by Presto
    ("cast(cast(b as varchar) as decimal(15, 2))", "cast(cast(b AS varchar) AS decimal(15, 2))", "decimal(15, 2)"),
    ("cast(array_contains(concat(a1, a2), '3') as varchar)", "cast(contains(concat(cast(a1 AS varchar), cast(a2 AS varchar)), '3') AS varchar)", "varchar"),
    ("CAST( MIN(pkey) OVER(a, b) as DATE )",
     "cast(min(pkey) over(a, b) AS date)", "date"),
    ("CAST( MIN(pkey) OVER(PARTITION BY primkey   ) as DATE )",
     "cast(min(pkey) over(PARTITION BY primkey) AS date)", "date"),
    ("CAST( MIN(pkey) OVER(order BY primkey ASC nulls first, sth DESC nulls LAST ) as DATE )",
     "cast(min(pkey) over(ORDER BY primkey ASC NULLS FIRST, sth DESC NULLS LAST) AS date)", "date"),
    ("CAST( MIN(pkey) OVER(PARTITION BY primkey, CONCAT(CAST(a AS VARCHAR(20)), CAST(b AS VARCHAR(20))) order by c desc) as DATE )",
     "cast(min(pkey) over(PARTITION BY primkey, concat(cast(cast(a AS varchar(20)) AS varchar), cast(cast(b AS varchar(20)) AS varchar)) ORDER BY c DESC) AS date)", "date"),
    ("cast(date_add(cast(a.my_column as date), 7) as varchar(10))", "cast(date_add('day', 7, cast(a.my_column AS date)) AS varchar(10))", "varchar(10)"),
    ("count(a)", "count(a)", "bigint"),
    ("count(distinct a, b)", "count(distinct cast(a AS varchar)|| ' ' ||cast(b AS varchar))", "bigint"),
    ("array(1, 2, 3)", "array[1, 2, 3]", "array"),
    ("date_format(timestamp('2020-03-25 16:32:01'), 'yyyy-MM-dd')", "date_format(cast('2020-03-25 16:32:01' AS timestamp), '%Y-%m-%d')", "varchar"),
    ("date_format(timestamp('2020-Jun-25 16:32:01'), 'yyyy-MMM-dd')", "date_format(cast('2020-Jun-25 16:32:01' AS timestamp), '%Y-%b-%d')", "varchar"),
    ("date_format(timestamp('2020-June-25 16:32:01'), 'yyyy-MMMM-dd')", "date_format(cast('2020-June-25 16:32:01' AS timestamp), '%Y-%M-%d')", "varchar"),
    ("format_number(3.1415, 2)", "cast(cast(round(3.1415, 2) AS double) AS varchar)", "varchar"),
    ("format_number(1234.5, '00000')", "lpad(cast(round(1234.5) AS varchar), 5, '0')", "varchar"),  # left padding with 0 if there are enough of them
    ("from_utc_timestamp(a, 'PST')", "cast(cast(cast(a AS timestamp) as timestamp) AT TIME ZONE 'America/Los_Angeles' AS timestamp)", "timestamp"),
    ("unix_timestamp()", "cast(to_unixtime(cast(current_timestamp AS timestamp)) AS bigint)", "bigint"),
    ("unix_timestamp('2020-03-25 16:32:01')", "cast(to_unixtime(cast('2020-03-25 16:32:01' AS timestamp)) AS bigint)", "bigint"),
    ("unix_timestamp('2020-03-25 16:32:01', 'yyyy-MM-dd HH:mm:ss')", "cast(to_unixtime(date_parse('2020-03-25 16:32:01', '%Y-%m-%d %k:%i:%s')) AS bigint)", "bigint"),
    ("isnull(1)", "1 is null", "boolean"),
    ("isnotnull(1)", "1 is not null", "boolean"),
    ("zzz_test_no_args_hive()", "zzz_test_no_args_presto()", "any"),
    ("date_format('2020-03-25 16:32:01', 'u')", "case when day_of_week(cast('2020-03-25 16:32:01' AS timestamp)) = 7 then 1 else day_of_week(cast('2020-03-25 16:32:01' AS timestamp)) + 1 end", "varchar"),
    ("extract(day from '2020-03-25 16:32:01')", "extract(day from cast('2020-03-25 16:32:01' AS timestamp))", "bigint"),
    ("extract(dayofweek from '2020-03-25 16:32:01')", "case when extract(day_of_week from cast('2020-03-25 16:32:01' AS timestamp)) = 7 then 1 else extract(day_of_week from cast('2020-03-25 16:32:01' AS timestamp)) + 1 end", "bigint"),
    ("extract(hour from '2020-03-25 16:32:01')", "extract(hour from cast('2020-03-25 16:32:01' AS timestamp))", "bigint"),
    ("extract(minute from '2020-03-25 16:32:01')", "extract(minute from cast('2020-03-25 16:32:01' AS timestamp))", "bigint"),
    ("extract(month from '2020-03-25 16:32:01')", "extract(month from cast('2020-03-25 16:32:01' AS timestamp))", "bigint"),
    ("extract(quarter from '2020-03-25 16:32:01')", "extract(quarter from cast('2020-03-25 16:32:01' AS timestamp))", "bigint"),
    ("extract(second from '2020-03-25 16:32:01')", "extract(second from cast('2020-03-25 16:32:01' AS timestamp))", "bigint"),
    ("extract(week from '2020-03-25 16:32:01')", "extract(week from cast('2020-03-25 16:32:01' AS timestamp))", "bigint"),
    ("extract(year from '2020-03-25 16:32:01')", "extract(year from cast('2020-03-25 16:32:01' AS timestamp))", "bigint")
])
def test_translate_function_specials(statement: str, translation: str, output_type: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    query = sqlparse.parse(statement)[0]  # Single query in this statement
    function = query.tokens[0]  # Single object in query
    assert Translation._translate_function(function.tokens) == (translation, output_type)   # Single query in this statement


@pytest.mark.parametrize(['statement'], [
    ("date_format(timestamp('AD 2020-03-25 16:32:01'), 'G yyyy-MM-dd')",),
    ("abcdef(1, 2, 3)",),
    ("zzz_test_not_yet_implemented(1, 2, 3)",),
    ("format_number(1234.5, '0001')",),
    ("format_number(a, b)",)
])
def test_translate_function_NotImplementedError(statement: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    query = sqlparse.parse(statement)[0]  # Single query in this statement
    function = query.tokens[0]  # Single object in query
    with pytest.raises(NotImplementedError):
        Translation._translate_function(function.tokens)


@pytest.mark.parametrize(['statement'], [
    ("date_format(timestamp('2020-June-25 16:32:01'), 'yyyy-EEE-dd')",)
])
def test_translate_function_KeyError(statement: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    query = sqlparse.parse(statement)[0]  # Single query in this statement
    function = query.tokens[0]  # Single object in query
    with pytest.raises(KeyError):
        Translation._translate_function(function.tokens)


@pytest.mark.parametrize('statement', [
    ("array_contains(a)"),
    ("array_contains()")
])
def test_translate_function_IndexError(statement: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    query = sqlparse.parse(statement)[0]  # Single query in this statement
    function = query.tokens[0]  # Single object in query
    with pytest.raises(IndexError):
        Translation._translate_function(function.tokens)


@pytest.mark.parametrize('statement', [
    ("lag(a, '3.5', c)")
])
def test_translate_function_ValueError(statement: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    query = sqlparse.parse(statement)[0]  # Single query in this statement
    function = query.tokens[0]  # Single object in query
    with pytest.raises(ValueError):
        Translation._translate_function(function.tokens)


@pytest.mark.parametrize(['statement', 'expected'], [
    ("INSERT INTO TABLE test_db.test_table partition(pkey='value') select * from a where b='c'", "INSERT INTO TABLE test_db.test_table partition(pkey='value') select * from a where b='c'"),
    ("INSERT INTO TABLE test_db.test_table select * from a where b='c'", "INSERT INTO TABLE test_db.test_table select * from a where b='c'"),
    ("INSERT INTO TABLE test_db.test_table select case when a then b end AS c from d", "INSERT INTO TABLE test_db.test_table select case when a then b end AS c from d"),
    ("INSERT INTO TABLE test_db.test_table select concat(a1)-concat(a2) from a", "INSERT INTO TABLE test_db.test_table select concat(cast(a1 AS varchar))-concat(cast(a2 AS varchar)) from a"),
    ("""INSERT INTO TABLE test_db.test_table partition
    select concat((9 - cast(substring(format_number(cast(a.my_column as bigint), '0000000000'),9,1) as bigint)),
    (9 - cast(substring(format_number(cast(a.my_column as bigint), '0000000000'),10,1) as bigint)))
    FROM cte""",
     """INSERT INTO TABLE test_db.test_table partition
    select concat(cast((9 - cast(substr(lpad(cast(round(cast(a.my_column AS bigint)) AS varchar), 10, '0'), 9, 1) AS bigint)) AS varchar), cast((9 - cast(substr(lpad(cast(round(cast(a.my_column AS bigint)) AS varchar), 10, '0'), 10, 1) AS bigint)) AS varchar))
    FROM cte"""),
    ("""
    with a AS (
        select case when array_contains(a, '2') then '4' else '3' end from b
    ),
    c AS (
        select * from b where d is between e and f 
    )
    INSERT INTO TABLE test_db.test_table
    select *
    from c
    """,
     """
    with a AS (
        select case when contains(a, '2') then '4' else '3' end from b
    ),
    c AS (
        select * from b where d is between e and f 
    )
    INSERT INTO TABLE test_db.test_table
    select *
    from c
    """),
    # Test divisions casted to double
    ("INSERT INTO TABLE test_db.test_table select 1+1", "INSERT INTO TABLE test_db.test_table select 1+1"),
    # Test array breakdown
    ("INSERT INTO TABLE test_db.test_table select 1, array('-1') as my_col, abc from cte", "INSERT INTO TABLE test_db.test_table select 1, array['-1'] as my_col, abc from cte")
])
def test_breakdown_query(statement: str, expected: str) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    query = sqlparse.parse(statement)[0]  # Single query in this statement
    _, _, _, Translation.partition_info = utils.parse_hive_insertion(query.value)  # Set the partition information
    assert Translation._breakdown_query(query.tokens) == expected


@pytest.mark.parametrize(['str_output_arguments', 'compositions', 'expected'], [
    (["1", "2"], [{"formula": "-{arg}", "args": "all"}], ["-1", "-2"]),  # Apply to all elements individually
    (["1", "2"], [{"formula": "-{arg}", "args": "all", "as_group": True}], ["-1, 2"]),  # Apply as a group, returns [str]
    (["1", "2"], [{"formula": "-{arg}", "args": [1]}], ["1", "-2"]),  # Apply to one element
    (["1", "2"], [{"formula": "-{arg}", "args": [1], "as_group": True}], ["1", "-2"]),  # No effect on single arg
    (["1", "2"], [{"formula": "-{arg}", "args": [1], "as_group": True}], ["1", "-2"])  # No effect on single arg
])
def test_apply_compositions(str_output_arguments: List, compositions: List[Dict], expected: List) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    Translation._apply_compositions(str_output_arguments, compositions) == expected


@pytest.mark.parametrize(['str_output_arguments', 'compositions'], [
    (["1", "2"], [{"formula": "{arg}", "args": "hello"}]),  # Incorrect str args
    (["1", "2"], [{"formula": "{arg}", "args": []}]),  # Empty arg list
    (["1", "2"], [{"formula": "{arg}", "args": [1, "the_end"]}]),  # Incorrect end keyword
    (["1", "2"], [{"formula": "{arg}", "args": [1, 0]}]),  # Continuous but not sorted
    (["1", "2"], [{"formula": "{arg}", "args": [0, 2]}])  # Not continuous
])
def test_apply_compositions_AssertionError(str_output_arguments: List, compositions: List[Dict]) -> None:
    Translation = recursive_translation.RecursiveHiveToPresto()
    with pytest.raises(AssertionError):
        Translation._apply_compositions(str_output_arguments, compositions)
