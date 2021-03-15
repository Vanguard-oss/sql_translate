import unittest
from unittest.mock import patch, MagicMock, call
import pytest
import contextlib
import os
import re
from typing import Dict, List, Tuple, Any
import sqlparse
from sql_translate import validation

TableComparator = validation.TableComparator("test_db", "", "")
PATH_SAMPLES = os.path.join(os.path.dirname(__file__), "samples")
TABLE_PROPERTIES_PARTITIONED = {
    "name": "temp_table",
    "columns": ["b"],
    "partition_col_type": {"a": "varchar"},
    "latest_partitions": {"a": "2020-03-25"}
}
TABLE_PROPERTIES_NOT_PARTITIONED = {
    "name": "temp_table",
    "columns": ["b"],
    "partition_col_type": {},
    "latest_partitions": {}
}


def test_set_paths() -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    path_src = os.path.join(PATH_SAMPLES, "translation", "complex_statement.hive")
    path_tgt = os.path.join(PATH_SAMPLES, "translation", "complex_statement.presto")
    Validator.set_paths(path_src, path_tgt)


@pytest.mark.parametrize(['config_data', 'expected'], [
    ({"query_parameters": {"test.sql": {"a": "test_udf.hello()"}}, "udf_replacements": {"b": "c"}}, ("temp_test_udf.py", {"test_udf": "temp_test_udf"}, {'a': 'test_udf.hello()'})),
    ({"query_parameters": {}}, ("test_udf.py", {"test_udf": "test_udf"}, {})),
])
def test_get_or_create_temp_udf(config_data: Dict, expected: Tuple[str, Dict, Dict]) -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.path_src_sql = "test.sql"
    with open("test_udf.py", "w") as f:
        f.write("""
def hello():
    return 1
""")
    assert Validator._get_or_create_temp_udf(config_data, "test_udf.py") == expected
    with contextlib.suppress(FileNotFoundError):
        os.remove("test_udf.py")
        os.remove("temp_test_udf.py")


def test_evaluate_udfs() -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.path_src_sql = os.path.join(PATH_SAMPLES, "translation", "complex_statement.hive")
    Validator.temp_src_table_properties = {"latest_partitions": {}}
    Validator.evaluate_udfs("")
    assert Validator.evaluated_query_parameters == {
        "a": "some_value",
        "b": 42
    }


@patch('sql_translate.validation.utils.parse_hive_insertion', return_value=("", "", "temp_table", ""))
def test_get_and_create_table_properties(mock_parse_hive_insertion: MagicMock) -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.src_sql = ""
    Validator.HiveTableExplorer.get_table_properties = MagicMock(return_value=TABLE_PROPERTIES_PARTITIONED)
    Validator.get_and_create_table_properties("")


@pytest.mark.parametrize(['data_type', 'expected'], [
    ("tinyint", "bigint"),
    ("decimal", "double"),
    ("varchar", "varchar")
])
def test_upscale_integers(data_type: str, expected: str) -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    assert Validator.upscale_integers(data_type) == expected


def test_create_sandbox_tables() -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.temp_src_table_properties = TABLE_PROPERTIES_PARTITIONED
    Validator.temp_tgt_table_properties = TABLE_PROPERTIES_PARTITIONED
    Validator._create_sandbox_table = MagicMock()
    Validator.create_sandbox_tables()


@patch('sql_translate.validation.run_query', return_value=None)
def test_create_sandbox_table(mock_run_query: MagicMock) -> None:
    Validator = validation.HiveToPresto("", "", "test_db", "")
    table_name = "my_table"
    column_info = {"7day": "varchar"}
    Validator.storage_location = "test_storage_location"
    # HIVE
    engine = "hive"
    # Partitioned table
    partition_info = {"b": "string"}
    Validator._create_sandbox_table(table_name, column_info, partition_info, engine)
    # Table not partitioned
    partition_info = {}
    Validator._create_sandbox_table(table_name, column_info, partition_info, engine)
    # PRESTO
    engine = "presto"
    # Partitioned table
    partition_info = {"b": "string"}
    Validator._create_sandbox_table(table_name, column_info, partition_info, engine)

    expected_calls = [
        call("DROP TABLE IF EXISTS test_db.my_table", ""),
        call((
            "CREATE TABLE IF NOT EXISTS test_db.my_table (\n"
            "7day varchar\n"
            ")\n"
            "COMMENT 'Validation table for my_table'\n"
            "PARTITIONED BY (b string)\n"
            "STORED AS PARQUET\n"
            "LOCATION 'test_storage_location/my_table';"
        ), ""),
        call("DROP TABLE IF EXISTS test_db.my_table", ""),
        call((
            "CREATE TABLE IF NOT EXISTS test_db.my_table (\n"
            "7day varchar\n"
            ")\n"
            "COMMENT 'Validation table for my_table'\n"
            "\nSTORED AS PARQUET\n"
            "LOCATION 'test_storage_location/my_table';"
        ), ""),
        call("DROP TABLE IF EXISTS test_db.my_table", ""),
        call((
            "CREATE TABLE IF NOT EXISTS test_db.my_table (\n"
            '"7day" varchar\n'
            ")\n"
            "COMMENT 'Validation table for my_table'\n"
            "PARTITIONED BY (b string)\n"
            "STORED AS PARQUET\n"
            "LOCATION 'test_storage_location/my_table';"
        ), "")
    ]
    mock_run_query.assert_has_calls(expected_calls)


@patch('sql_translate.validation.run_query', return_value=None)
def test_insert_into_hive_table(mock_run_query: MagicMock) -> None:
    # Set up
    Validator = validation.HiveToPresto("", "", "test_db", "")
    Validator.evaluated_query_parameters = {"my_col": "my_column"}

    # Table not partitioned
    Validator.temp_src_table_properties = TABLE_PROPERTIES_NOT_PARTITIONED
    Validator.src_sql = "select {my_col} from b\nINSERT OVERWRITE TABLE c.d"
    Validator.insert_into_hive_table()

    # Partitioned table (static)
    Validator.temp_src_table_properties = TABLE_PROPERTIES_PARTITIONED
    Validator.src_sql = "select {my_col} from b\nINSERT OVERWRITE TABLE c.d PARTITION (e='2020-03-25')"
    Validator.insert_into_hive_table()

    # Partitioned table (dynamic)
    Validator.temp_src_table_properties = TABLE_PROPERTIES_PARTITIONED
    Validator.src_sql = "select {my_col} from b\nINSERT OVERWRITE TABLE c.d PARTITION (a)"
    Validator.insert_into_hive_table()

    # Check
    expected_calls = [
        call("SET hive.exec.dynamic.partition.mode=strict", ""),
        call("select my_column from b\nINSERT OVERWRITE TABLE test_db.temp_table", ""),
        call("SET hive.exec.dynamic.partition.mode=strict", ""),
        call("select my_column from b\nINSERT OVERWRITE TABLE test_db.temp_table PARTITION (a='2020-03-25')", ""),
        call("SET hive.exec.dynamic.partition.mode=nonstrict", ""),
        call("select my_column from b\nINSERT OVERWRITE TABLE test_db.temp_table PARTITION (a)", "")
    ]
    mock_run_query.assert_has_calls(expected_calls)


@patch('sql_translate.validation.fetch')
@pytest.mark.parametrize(['fetch_output'], [
    ([(0,)],),
    ([(1,)],),
])
def test_insert_into_presto_table(mock_fetch: MagicMock, capsys, fetch_output: List[Tuple[int]]) -> None:
    mock_fetch.return_value = fetch_output
    Validator = validation.HiveToPresto("", "", "test_db", "")
    Validator.evaluated_query_parameters = {"a": "my_column"}
    Validator.temp_tgt_table_properties = TABLE_PROPERTIES_NOT_PARTITIONED
    Validator.tgt_sql = "INSERT into db.c select {a} from b"
    Validator._presto_runner = MagicMock(side_effect=lambda x, y: print(y))
    Validator.insert_into_presto_table()
    captured = capsys.readouterr()
    assert "INSERT into db.c select {a} from b" in captured.out.split("\n")


@patch('sql_translate.validation.run_query', return_value=None)
def test_presto_runner(mock_run_query: MagicMock) -> None:
    Validator = validation.HiveToPresto("", "", "test_db", "")
    Validator.temp_tgt_table_properties = {}
    assert Validator._presto_runner(
        "select my_column from b\nINSERT into TABLE test_db.test_table",
        "select {a} from b\nINSERT into TABLE db.test_table"
    ) == "select {a} from b\nINSERT into TABLE db.test_table"


@patch('sql_translate.validation.run_query')
def test_presto_runner_Exception_identical_error(mock_run_query: MagicMock) -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.temp_tgt_table_properties = {}
    msg = (
        "\"[HY000] [Teradata][Presto] (1060) Presto Query Error: line 1:11: "
        "'=' cannot be applied to integer, varchar\")----"
    )
    mock_run_query.side_effect = Exception(msg)
    with pytest.raises(RuntimeError) as err:
        Validator._presto_runner(
            "select 'a'=1",
            "select 'a'=1"
        )
        assert "'=' cannot be applied to integer, varchar" in str(err)


@patch('sql_translate.validation.run_query')
def test_presto_runner_Exception_unknown_error(mock_run_query: MagicMock) -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.temp_tgt_table_properties = {}
    msg = (
        "\"[HY000] [Teradata][Presto] (1060) Presto Query Error: "
        "UNKNOWN ERROR\")----"
    )
    mock_run_query.side_effect = Exception(msg)
    with pytest.raises(RuntimeError) as err:
        Validator._presto_runner(
            "select my_column from b\nINSERT into TABLE test_db.test_table",
            "select a from b\nINSERT into TABLE db.test_table"
        )
        assert "UNKNOWN ERROR" in str(err)


def test_validate_dml() -> None:
    with open("test_original.sql", "w") as f:
        f.write("Hello world!")
    with open("test_translation.sql", "w") as f:
        f.write("Hello world!")
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.temp_src_table_properties = {"name": ""}
    Validator.temp_tgt_table_properties = {"name": ""}
    Validator.get_and_create_table_properties = MagicMock()
    Validator.evaluate_udfs = MagicMock()
    Validator.create_sandbox_tables = MagicMock()
    Validator.insert_into_presto_table = MagicMock(return_value=("", 1.1))
    Validator.insert_into_hive_table = MagicMock(return_value=("", 1.1))
    Validator.compare_tables = MagicMock()
    Validator.validate_dml("test_original.sql", "test_translation.sql", "", "", "")
    os.remove("test_original.sql")
    os.remove("test_translation.sql")


@pytest.mark.parametrize(['iou', 'iou_output', 'printout'], [
    (1.0, 1.0, 'a and b are identical!\n'),
    (0.5, 0.5, 'WARNING: a and b are not identical!\n')
])
def test_Validator_compare_tables(capsys, iou: float, iou_output: int, printout: str) -> None:
    Validator = validation.HiveToPresto("", "", "", "")
    Validator.temp_src_table_properties = {"name": "a"}
    Validator.temp_tgt_table_properties = {"name": "b"}
    Validator.TableComparator.compare_tables = MagicMock(return_value=iou)
    assert Validator.compare_tables() == iou_output
    captured = capsys.readouterr()
    assert captured.out == printout


def test_sanity_checks() -> None:
    TableComparator = validation.TableComparator("", "", "")
    table_info_1 = {"name": "a", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}}
    table_info_2 = {"name": "b", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-02-25"}, "partition_col_type": {"c1": "varchar"}}
    TableComparator._sanity_checks(table_info_1, table_info_2)


@pytest.mark.parametrize(['table_info_1', 'table_info_2'], [
    (
        {"name": "a", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        {"name": "b", "columns": ["c1", "c3"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}}
    ),
    (
        {"name": "a", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        {"name": "b", "columns": ["c1", "c2"], "latest_partitions": {"c2": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}}
    )
])
def test_sanity_checks_AssertionError(table_info_1: Dict, table_info_2: Dict) -> None:
    TableComparator = validation.TableComparator("", "", "")
    with pytest.raises(AssertionError):
        TableComparator._sanity_checks(table_info_1, table_info_2)


@patch('sql_translate.validation.fetch', return_value=[("varchar", 10, 5)])
@pytest.mark.parametrize(['table_info_1', 'table_info_2', 'expected_calls'], [
    (
        {"name": "a", "columns": ["c1", "c2"], "latest_partitions": {}, "partition_col_type": {}},
        {"name": "b", "columns": ["c1", "c3"], "latest_partitions": {}, "partition_col_type": {}},
        [
            call("SELECT max(typeof(c1)), count(c1), count(distinct c1), max(typeof(c2)), count(c2), count(distinct c2) FROM test_db.a LIMIT 1", ""),
            call("SELECT max(typeof(c1)), count(c1), count(distinct c1), max(typeof(c2)), count(c2), count(distinct c2) FROM test_db.b LIMIT 1", "")
        ]
    ),
    (
        {"name": "a", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        {"name": "b", "columns": ["c1", "c3"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        [
            call("SELECT max(typeof(c1)), count(c1), count(distinct c1), max(typeof(c2)), count(c2), count(distinct c2) FROM test_db.a WHERE c1='2020-03-25' LIMIT 1", ""),
            call("SELECT max(typeof(c1)), count(c1), count(distinct c1), max(typeof(c2)), count(c2), count(distinct c2) FROM test_db.b WHERE c1='2020-03-25' LIMIT 1", "")
        ]
    )
])
def test_get_column_counts(mock_fetch: MagicMock, table_info_1: Dict, table_info_2: Dict, expected_calls: call) -> None:
    TableComparator = validation.TableComparator("test_db", "", "")

    TableComparator._get_column_counts(table_info_1, table_info_2)
    mock_fetch.assert_has_calls(expected_calls)


@pytest.mark.parametrize(['table_info_1', 'table_info_2', 'column_counts', 'column_differences'], [
    (
        {"name": "a", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        {"name": "b", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        (("varchar", 10, 5, "varchar", 10, 5), ("varchar", 10, 5, "varchar", 9, 4)),
        {"count_c2": {"table_1": 10, "table_2": 9}, "count_distinct_c2": {"table_1": 5, "table_2": 4}}
    )
])
def test_compare_columns_between_two_tables(table_info_1, table_info_2, column_counts, column_differences) -> None:
    TableComparator = validation.TableComparator("", "", "")

    def side_effect(table_info_1: Dict, table_info_2: Dict):
        return column_counts
    TableComparator._get_column_counts = MagicMock(side_effect=side_effect)
    assert TableComparator._compare_columns_between_two_tables(table_info_1, table_info_2) == column_differences


@patch('sql_translate.validation.fetch')
@pytest.mark.parametrize(['table_info_1', 'table_info_2', 'expected'], [
    (
        {"name": "a", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        {"name": "b", "columns": ["c1", "c2"], "latest_partitions": {"c1": "2020-03-25"}, "partition_col_type": {"c1": "varchar"}},
        {
            "1_count_table_1": 10,
            "2_count_table_2": 10,
            "3_count_distinct_table_1": 5,
            "4_count_distinct_table_2": 5,
            "5_count_distinct_table_1_minus_table_2": 1,
            "6_count_distinct_table_2_minus_table_1": 1,
            "7_count_distinct_intersection": 4
        }
    )
])
def test_compare_rows_between_two_tables(mock_fetch: MagicMock, table_info_1, table_info_2, expected) -> None:
    mock_fetch.return_value = [
        ("1_count_table_1", 10),
        ("2_count_table_2", 10),
        ("3_count_distinct_table_1", 5),
        ("4_count_distinct_table_2", 5),
        ("5_count_distinct_table_1_minus_table_2", 1),
        ("6_count_distinct_table_2_minus_table_1", 1),
        ("7_count_distinct_intersection", 4)
    ]
    TableComparator = validation.TableComparator("", "", "")
    assert TableComparator._compare_rows_between_two_tables(table_info_1, table_info_2) == expected


@pytest.mark.parametrize(['column_count_differences', 'row_differences', 'expected'], [
    (
        {},
        {
            "1_count_table_1": 10,
            "2_count_table_2": 10,
            "3_count_distinct_table_1": 5,
            "4_count_distinct_table_2": 5,
            "5_count_distinct_table_1_minus_table_2": 0,
            "6_count_distinct_table_2_minus_table_1": 0,
            "7_count_distinct_intersection": 5
        },
        1.0
    ),
    (
        {},
        {
            "1_count_table_1": 10,
            "2_count_table_2": 10,
            "3_count_distinct_table_1": 5,
            "4_count_distinct_table_2": 4,
            "5_count_distinct_table_1_minus_table_2": 2,
            "6_count_distinct_table_2_minus_table_1": 1,
            "7_count_distinct_intersection": 3
        },
        0.5
    ),
    (
        {"count_c2": {"table_1": 10, "table_2": 9}, "count_distinct_c2": {"table_1": 5, "table_2": 4}},
        {
            "1_count_table_1": 10,
            "2_count_table_2": 10,
            "3_count_distinct_table_1": 5,
            "4_count_distinct_table_2": 5,
            "5_count_distinct_table_1_minus_table_2": 0,
            "6_count_distinct_table_2_minus_table_1": 0,
            "7_count_distinct_intersection": 5
        },
        1.0
    ),
    (
        {"count_c2": {"table_1": 10, "table_2": 9}, "count_distinct_c2": {"table_1": 5, "table_2": 4}},
        {
            "1_count_table_1": 10,
            "2_count_table_2": 10,
            "3_count_distinct_table_1": 5,
            "4_count_distinct_table_2": 4,
            "5_count_distinct_table_1_minus_table_2": 2,
            "6_count_distinct_table_2_minus_table_1": 1,
            "7_count_distinct_intersection": 3
        },
        0.5
    ),
    (
        {"count_c2": {"table_1": 0, "table_2": 0}, "count_distinct_c2": {"table_1": 0, "table_2": 0}},
        {
            "1_count_table_1": 0,
            "2_count_table_2": 0,
            "3_count_distinct_table_1": 0,
            "4_count_distinct_table_2": 0,
            "5_count_distinct_table_1_minus_table_2": 0,
            "6_count_distinct_table_2_minus_table_1": 0,
            "7_count_distinct_intersection": 0
        },
        1
    )
])
def test_TableComparator_compare_tables(column_count_differences, row_differences, expected) -> None:
    TableComparator = validation.TableComparator("", "", "")
    TableComparator._sanity_checks = MagicMock(return_value=None)

    TableComparator._compare_columns_between_two_tables = MagicMock(return_value=column_count_differences)
    TableComparator._compare_rows_between_two_tables = MagicMock(return_value=row_differences)
    assert TableComparator.compare_tables({"name": "a"}, {"name": "b"}) == expected
