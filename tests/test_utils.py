from unittest.mock import patch, MagicMock
import pytest
import os
import json
from typing import Dict, List, Tuple, Optional
import sqlparse
from sqlparse.tokens import Keyword, DML, Whitespace, Newline, Punctuation
from sql_translate import utils


PATH_SAMPLES = os.path.join(os.path.dirname(__file__), "samples")


def test_get_path_active_hive_files() -> None:
    paths_job_folders = [
        os.path.join(os.path.dirname(__file__), "samples", "example_folder")
    ]
    assert utils.get_path_active_hive_files(paths_job_folders) == [
        {
            "hive": os.path.join(paths_job_folders[0], "test.hive"),
            "presto": os.path.join(paths_job_folders[0], "test.presto"),
            "config": os.path.join(paths_job_folders[0], "config.json"),
            "udf": os.path.join(paths_job_folders[0], "udf.py")
        }
    ]


@pytest.mark.parametrize(['query', 'expected'], [
    ("select regexpr(a, '^[0-9]{4}-[0-9]{2}-[0-9]{2}')", "select regexpr(a, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')"),
    ("select 1", "select 1"),
    ("select {column} from cte", "select {column} from cte")
])
def test_protect_regex_curly_brackets(query: str, expected: str) -> None:
    assert utils.protect_regex_curly_brackets(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ('select my_column as "7day" from cte', ("my_column", '"7day"')),
    ('select my_column as "some column" from cte', ("my_column", '"some column"')),
    ('select my_column as `some column (right)` from cte', ("my_column", '`some column (right)`')),
    ("select my_column as c from cte", ("my_column", "c")),
    ("select my_column from cte", ("my_column", None)),
    ("select  my_column from cte", (" ", None))  # Target a whitespace, which does not have a get_alias method
])
def test_extract_alias(query: str, expected: Tuple[str, Optional[str]]) -> None:
    token = sqlparse.parse(query)[0].tokens[2]
    assert utils.extract_alias(token) == expected


@pytest.mark.parametrize(['table_params', 'join_key', 'date_cast', 'expected'], [
    ({"latest_partitions": {"load_date": "2020-10-25"}, "partition_col_type": {"load_date": "date"}}, " AND ", False, "load_date='2020-10-25'"),
    ({"latest_partitions": {"load_date": "2020-10-25"}, "partition_col_type": {"load_date": "date"}}, " AND ", True, "load_date=date('2020-10-25')"),
    ({"latest_partitions": {"load_date": "2020-10-25"}, "partition_col_type": {"load_date": "varchar"}}, " AND ", False, "load_date='2020-10-25'"),
    ({"latest_partitions": {"load_date": "1"}, "partition_col_type": {"load_date": "integer"}}, " AND ", False, "load_date=1")
])
def test_partition_builder(table_params: Dict[str, str], join_key: str, date_cast: bool, expected: str) -> None:
    assert utils.partition_builder(table_params, join_key=join_key, date_cast=date_cast) == expected


@pytest.mark.parametrize(['column', 'expected'], [
    ('hey', 'hey'),
    ('my column', '`my column`'),
    ('my 6col(umn', '`my 6col(umn`'),  # Back ticks have priority
    ('7day', '7day')
])
def test_format_column_name_hive(column: str, expected: float) -> None:
    assert utils.format_column_name_hive(column) == expected

@pytest.mark.parametrize(['column', 'expected'], [
    ('hey', 'hey'),
    ('my column', '`my column`'),
    ('my 6col(umn', '`my 6col(umn`'),  # Back ticks have priority
    ('7day', '"7day"')
])
def test_format_column_name_presto(column: str, expected: float) -> None:
    assert utils.format_column_name_presto(column) == expected


@pytest.mark.parametrize(['value', 'required_type', 'expected'], [
    ('1', 'bigint', '1'),
    ('1.9', 'double', '1.9'),
    ("'1.9'", 'double', '1.9')
])
def test_char_to_number(value: str, required_type: str, expected: float) -> None:
    assert utils.char_to_number(value, required_type) == expected


@pytest.mark.parametrize(['value', 'required_type'], [
    ('1.0', 'float')
])
def test_char_to_number_NotImplementedError(value: str, required_type: str) -> None:
    with pytest.raises(NotImplementedError):
        utils.char_to_number(value, required_type)


@pytest.mark.parametrize(['value', 'required_type'], [
    ('1.0', 'bigint'),
    ('1', 'double')
])
def test_char_to_number_TypeError(value: str, required_type: str) -> None:
    with pytest.raises(Exception):
        utils.char_to_number(value, required_type)


@pytest.mark.parametrize(['describe_formatted_output'], [
    ("not_partitionned_table.json",),
    ("partitionned_table.json",)
])
def test_parse_describe_formatted(describe_formatted_output: str) -> None:
    with open(os.path.join(PATH_SAMPLES, "describe_formatted", describe_formatted_output)) as f:
        data = json.load(f)
    assert utils.parse_describe_formatted(data["query_output"]) == data["expected"]


def test_parse_describe_formatted_RunTimeError() -> None:
    with pytest.raises(RuntimeError):
        utils.parse_describe_formatted([])


@pytest.mark.parametrize(['input_text', 'expected'], [
    ("", ""),
    ("Hello world!", "Hello world!"),
    ("2020%2D03%2D25 16%3A25%3A17%3A%2E0", "2020-03-25 16:25:17.0")
])
def test_decode_utf8(input_text: str, expected: str) -> None:
    utils.decode_utf8(input_text) == expected


@pytest.mark.parametrize(['path_file', 'expected'], [
    ("not_partitioned_into.sql", ("into", "output_db", "output_table", {})),
    ("not_partitioned_overwrite.sql", ("overwrite", "output_db", "output_table", {})),
    (
        "partitioned_into.sql",
        ("into", "output_db", "output_table", {"partition_name": "load_date", "partition_value": "'2020-03-25'"})
    ),
    (
        "partitioned_overwrite.sql",
        ("overwrite", "output_db", "output_table", {"partition_name": "load_date", "partition_value": "'2020-03-25'"})
    ),
    (
        "dynamic_partitioning.sql",
        ("overwrite", "output_db", "output_table", {"partition_name": "load_date", "partition_value": None})
    )
])
def test_parse_hive_insertion(path_file: str, expected: Tuple[str, Dict]) -> None:
    with open(os.path.join(PATH_SAMPLES, "parse_hive_insertion", path_file)) as f:
        sql = f.read()
    assert utils.parse_hive_insertion(sql) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("select a, current_time as dt_time from cte", (False, [0, 34], [["a", None], ["current_time", "dt_time"]])),  # Masking
    ("select null, a, current_time as dt_time from cte", (False, [0, 40], [["null", None], ["a", None], ["current_time", "dt_time"]])),  # Masking
    ("select coalesce(NULL, NULL, 1) sth, a, current_time as dt_time from cte", (False, [0, 63], [["coalesce(NULL, NULL, 1)", "sth"], ["a", None], ["current_time", "dt_time"]])),  # Double masking
    ("select *, cte2.*, d from cte1, cte2", (False, [0, 20], [["*", None], ["cte2.*", None], ["d", None]])),  # Stars
    ("with a over b as (select b from c) Insert OVERWRITE table d.e PARTITION (f='g') select b, xyz f from a", (False, [80, 96], [["b", None], ["xyz", "f"]])),  # Stuff before select
    ("select distinct a from cte", (True, [0, 18], [["a", None]])),
    ("select a b, c from cte", (False, [0, 14], [["a", "b"], ["c", None]])),
    ("select a OR b c, d and e, f from cte", (False, [0, 28], [["a OR b", "c"], ["d and e", None], ["f", None]])),
    ("select a, array['-1'] as sth from cte", (False, [0, 29], [["a", None], ["array['-1']", "sth"]]))
])
def test_parse_final_select(query: str, expected: Tuple[bool, Dict[str, Optional[str]]]) -> None:
    assert utils.parse_final_select(query) == expected


@patch('sql_translate.utils.fetch')
@pytest.mark.parametrize(['partitions', 'expected'], [
    ([("a=1",), ("a=2",)], {"a": 2})
])
def test_get_latest_partitions(mock_fetch: MagicMock, partitions: List[Tuple[str]], expected: str) -> None:
    HiveTableExplorer = utils.HiveTableExplorer("")
    HiveTableExplorer.decode_utf8 = MagicMock(side_effect=lambda x: x)  # Return input without change
    mock_fetch.return_value = partitions
    HiveTableExplorer._get_latest_partitions("")


@patch('sql_translate.utils.fetch')
@pytest.mark.parametrize(['partitions'], [
    ([],),
    ([("a=1/b=0",), ("a=2/b=3",)],)
])
def test_get_latest_partitions_ValueError(mock_fetch: MagicMock, partitions: List[Tuple[str]]) -> None:
    HiveTableExplorer = utils.HiveTableExplorer("")
    HiveTableExplorer.decode_utf8 = MagicMock(side_effect=lambda x: x)  # Return input without change
    mock_fetch.return_value = partitions
    with pytest.raises(ValueError):
        HiveTableExplorer._get_latest_partitions("")


@patch('sql_translate.utils.fetch')
@patch('sql_translate.utils.parse_describe_formatted', return_value={"a", "b"})
def test_describe_formatted(mock_fetch: MagicMock, mock_parse_describe_formatted: MagicMock) -> None:
    HiveTableExplorer = utils.HiveTableExplorer("")
    mock_fetch.return_value = "b"
    HiveTableExplorer._describe_formatted("a") == {"a", "b"}


@pytest.mark.parametrize(['path_describe_formatted_output', 'expected'], [
    ("not_partitionned_table.json",
     {
         "name": "my_table",
         "table_location": "storage_location",
         "columns": {"a": "string", "b": "string", "c": "string"}
     }),
    ("partitionned_table.json", {
        "name": "my_table",
        "table_location": "storage_location",
        "columns": {"a": "varchar(50)", "b": "bigint"},
        "partition_col_type": {"date_timestamp": "char(10)"},
        "latest_partitions": {"date_timestamp": "1"}
    })
])
def test_get_table_properties(path_describe_formatted_output: Dict, expected: Dict) -> None:
    HiveTableExplorer = utils.HiveTableExplorer("")
    with open(os.path.join(os.path.dirname(__file__), "samples", "describe_formatted", path_describe_formatted_output)) as f:
        describe_formatted_output = json.load(f)["expected"]
    HiveTableExplorer._describe_formatted = MagicMock(return_value=describe_formatted_output)
    HiveTableExplorer._get_latest_partitions = MagicMock(return_value={"date_timestamp": "1"})
    HiveTableExplorer.get_table_properties("db.my_table") == expected


@pytest.mark.parametrize(['sql', 'line', 'column', 'expected'], [
    ("select * from cte", 0, 9, {"value": "from", "idx": 9}),
    ("select *\nfrom cte", 1, 0, {"value": "from", "idx": 9})
])
def test_get_problematic_token(sql: str, line: int, column: int, expected: Dict) -> None:
    ColumnCaster = utils.ColumnCaster()
    token, idx = ColumnCaster.get_problematic_token(sql, line, column)
    assert token.value == expected["value"] and idx == expected["idx"]


@pytest.mark.parametrize(['sql', 'line', 'column'], [
    ("select * from cte", 0, 10)
])
def test_get_problematic_token_AttributeError(sql: str, line: int, column: int) -> None:
    ColumnCaster = utils.ColumnCaster()
    with pytest.raises(AttributeError):
        ColumnCaster.get_problematic_token(sql, line, column)


@pytest.mark.parametrize(['sql', 'line', 'column'], [
    ("select * from cte", 1, 10)
])
def test_get_problematic_token_ValueError(sql: str, line: int, column: int) -> None:
    ColumnCaster = utils.ColumnCaster()
    with pytest.raises(ValueError):
        ColumnCaster.get_problematic_token(sql, line, column)


@pytest.mark.parametrize(['token', 'cast_to', 'data_type', 'expected'], [
    ("a", "varchar", "varchar (1)", "a"),
    ("a", "varchar", "bigint (1)", "cast(a AS varchar)"),
    ("a", "char(3)", "char(3)", "a"),
    ("a", "char(3)", "char(4)", "cast(a AS char(3))")
])
def test_light_cast(token, cast_to: str, data_type: str, expected: str) -> None:
    ColumnCaster = utils.ColumnCaster()
    assert ColumnCaster._light_cast(sqlparse.parse(token)[0].tokens[0], cast_to, data_type) == expected


@pytest.mark.parametrize(['sql', 'loc', 'cast_to', 'bck', 'fwd', 'groupdict', 'expected'], [
    ("select a  = 'a' from cte", [0, 10], "varchar", 1, 1, {"b_type_0": "", "f_type_0": "varchar"}, "select cast(a AS varchar)  = 'a' from cte"),
    ("select count(a)/2 from cte", [0, 15], "double", 1, 1, {"b_type_0": "", "f_type_0": ""}, "select cast(count(a) AS double)/cast(2 AS double) from cte"),
    ("select *\nfrom cte\nwhere max(a) = min(b)", [2, 13], "varchar", 1, 1, {"b_type_0": "", "f_type_0": ""}, "select *\nfrom cte\nwhere cast(max(a) AS varchar) = cast(min(b) AS varchar)"),
    ("select *\nfrom cte\nwhere a\nbetween b/2 and c", [3, 0], "varchar", 1, 3, {"b_type_0": "", "f_type_0": "",
                                                                                 "f_type_1": ""}, "select *\nfrom cte\nwhere cast(a AS varchar)\nbetween cast(b/2 AS varchar) and cast(c AS varchar)"),
])
def test_cast_non_trivial_tokens(sql: str, loc: List[int], cast_to: str, bck: int, fwd: int, groupdict: Dict, expected: Dict) -> None:
    ColumnCaster = utils.ColumnCaster()
    token, idx = ColumnCaster.get_problematic_token(sql, *loc)
    assert ColumnCaster.cast_non_trivial_tokens(sql, token, idx, cast_to, groupdict, count_backward_tokens=bck, count_forward_tokens=fwd) == expected


@pytest.mark.parametrize(['sql', 'loc', 'cast_to', 'groupdict'], [
    ("select a  = 'a' from cte", [0, 10], "varchar", {"b_type_0": "", "f_type_0": "varchar"})
])
def test_cast_non_trivial_tokens_ValueError(sql: str, loc: List[int], cast_to: str, groupdict: Dict) -> None:
    ColumnCaster = utils.ColumnCaster()
    token, idx = ColumnCaster.get_problematic_token(sql, *loc)
    with pytest.raises(ValueError):
        ColumnCaster.cast_non_trivial_tokens(sql, token, idx, cast_to, groupdict, count_backward_tokens=10)
