import pytest
import sqlparse
from typing import Tuple, List, Optional
from unittest.mock import patch, MagicMock
from sql_translate.engine import global_translation
from sql_translate import utils

GHTP = global_translation.GlobalHiveToPresto()


def test_create_parent() -> None:
    _GlobalHiveToPresto = global_translation._GlobalTranslator()


@patch('sql_translate.utils.protect_regex_curly_brackets', side_effect=lambda x: x)
def test_translate_query(mock_protect_regex_curly_brackets: MagicMock) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    GHTP._remove_dollar_sign = MagicMock(side_effect=lambda x: x)
    GHTP._replace_double_quotes = MagicMock(side_effect=lambda x: x)
    GHTP._replace_back_ticks = MagicMock(side_effect=lambda x: x)
    GHTP._add_double_quotes = MagicMock(side_effect=lambda x: x)
    GHTP._increment_array_indexes = MagicMock(side_effect=lambda x: x)
    GHTP._cast_divisions_to_double = MagicMock(side_effect=lambda x: x)
    GHTP._fix_rlike_calls = MagicMock(side_effect=lambda x: x)
    GHTP._fix_lateral_view_explode_calls = MagicMock(side_effect=lambda x: x)
    GHTP._fix_interval_formatting = MagicMock(side_effect=lambda x: x)
    GHTP._fix_aliasing_on_broadcasting = MagicMock(side_effect=lambda x: x)
    GHTP.gbt.fix_group_by_calls = MagicMock(side_effect=lambda x: x)
    assert GHTP.translate_query("select * from db.table") == "select * from db.table"


@pytest.mark.parametrize(['query', 'expected'], [
    ("", ""),
    ('select "a" from b', "select 'a' from b"),  # Would be surrounded ``
    ('RIGHT JOIN db.table b', 'RIGHT JOIN db.table b')
])
def test_replace_double_quotes(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._replace_double_quotes(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("", ""),
    ('select `a b` from b', 'select "a b" from b')
])
def test_replace_back_ticks(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._replace_back_ticks(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("select 1, 11, 1.1, '1' from cte", "select 1, 11, 1.1, '1' from cte"),
    ("select a 7day from cte", 'select a "7day" from cte'),
    ("select a hel8lo from cte", "select a hel8lo from cte"),
    ("select 123a from 18mo", 'select "123a" from "18mo"'),
    ("select case when coalesce(sth_123_bal, 0) > 0 then 1 else 0 end as 123_flag from cte",
     'select case when coalesce(sth_123_bal, 0) > 0 then 1 else 0 end as "123_flag" from cte')
])
def test_add_double_quotes(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._add_double_quotes(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("", ""),
    ('select ab from ${b}', 'select ab from {b}'),
    ('select regex_like(a, "abc$") from b', 'select regex_like(a, "abc$") from b')
])
def test_remove_dollar_sign(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._remove_dollar_sign(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("", ""),
    ("select split(a, '_')[1], split(b, '_')[0] from c", "select split(a, '_')[2], split(b, '_')[1] from c"),
    ("select a from b", "select a from b")
])
def test_increment_array_indexes(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._increment_array_indexes(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("select 1", "select 1"),
    ("select a/2", "select cast(a AS double)/cast(2 AS double)"),
    ("select count(a/2)/3", "select cast(count(cast(a AS double)/cast(2 AS double)) AS double)/cast(3 AS double)")
])
def test_cast_divisions_to_double(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._cast_divisions_to_double(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("select a rlike\nb", "select a like\nb"),
    ("select 1", "select 1")
])
def test_fix_rlike_calls(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._fix_rlike_calls(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("""select a from cte where LATERAL VIEW EXPLODE(split(b, ',')) "7day" AS score""",
     f"""select a from cte where CROSS JOIN unnest(split(b, ',')) AS "7day" {utils.function_placeholder}(score)"""),
    ("select 1", "select 1")
])
def test_fix_lateral_view_explode_calls(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._fix_lateral_view_explode_calls(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("select 1==1", "select 1=1"),
    ("select 1", "select 1")
])
def test_fix_double_equals(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._fix_double_equals(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("select a from cte where a BETWEEN (CURRENT_DATE - interval '1' as YEAR) AND CURRENT_DATE",
     "select a from cte where a BETWEEN (CURRENT_DATE - interval '1' YEAR) AND CURRENT_DATE"),
    ("select 1", "select 1")
])
def test_fix_interval_formatting(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._fix_interval_formatting(query) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    ("", ""),
    # I. select statements
    # I.1. No issue
    ("select a from cte", "select a from cte"),
    ("select '1' from cte", "select '1' from cte"),  # Corner case: from is a Keyword, not alias!
    ("select '1' as fff from cte", "select '1' as fff from cte"),
    # I.2. Problematic parsing by sqlparse
    ("select '1' sth from cte", "select '1' as sth from cte"),
    ('select "7day" sth from cte', 'select "7day" as sth from cte'),  # Double quotes
    ('select 1 `from` from cte', 'select 1 as `from` from cte'),  # Backticks
    ("select 1 hello from cte", "select 1 as hello from cte"),
    ("select 1.5 alias from cte", "select 1.5 as alias from cte"),
    ("select '1' a, 1 b, 1.5 c from cte", "select '1' as a, 1 as b, 1.5 as c from cte"),
    # II. group by statements
    # II.1. No issue
    ("select a from cte group by a", "select a from cte group by a"),
    ("select a from cte group by '1', '2' order by '1' sth", "select a from cte group by '1', '2' order by '1' as sth"),  # order by is a Keyword
    ("select a from cte group by '1', '2' sth", "select a from cte group by '1', '2' as sth"),  # But order is not
    ("select a from cte group by '1' as sth order \n\t by '1' sth2", "select a from cte group by '1' as sth order \n\t by '1' as sth2"),  # \s+
    # II.2. Problematic parsing
    ("select a from cte group by '1' sth", "select a from cte group by '1' as sth"),
    ('select "7day" sth from cte group by "7day" sth', 'select "7day" as sth from cte group by "7day" as sth'),  # Double quotes
    ('select 1 `from` from cte group by 1 `from`', 'select 1 as `from` from cte group by 1 as `from`'),  # Backticks
    ("select 1 from cte group by 1 sth", "select 1 from cte group by 1 as sth"),
    ("select 1.5 from cte group by 1.5 sth", "select 1.5 from cte group by 1.5 as sth"),
    ("select '1', 1, 1.5 from cte group by '1' a, 1 b, 1.5 c", "select '1', 1, 1.5 from cte group by '1' as a, 1 as b, 1.5 as c"),
])
def test__fix_aliasing_on_broadcasting(query: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP._fix_aliasing_on_broadcasting(query) == expected


@pytest.mark.parametrize(['query_section', 'expected'], [
    ("with a as (select b, my_column from c) Insert OVERWRITE table d.e PARTITION (my_column) select b from a",
     "INSERT INTO d.e\nwith a as (select b, my_column from c)  SELECT\nb,\nmy_column\nfrom a"),
    ("with a as (select b from c) Insert OVERWRITE table d.e PARTITION (f='g') select b from a",
     "INSERT INTO d.e\nwith a as (select b from c)  SELECT\nb,\n'g'\nfrom a"),
    ("with a as (select b from c) insert into table \"5_6f\".\"d5 6f\" PARTITION (f='g') select b from a",
     "INSERT INTO \"5_6f\".\"d5 6f\"\nwith a as (select b from c)  SELECT\nb,\n'g'\nfrom a"),
    ("with a as (select b from c) insert overwrite table \"5_6f\".\"d5 6f\" select b from a",
     "INSERT INTO \"5_6f\".\"d5 6f\"\nwith a as (select b from c)  SELECT\nb\nfrom a"),
    ("with a as (select b from c) Insert OVERWRITE table d.e PARTITION (f) select b, f from a",
     "INSERT INTO d.e\nwith a as (select b from c)  SELECT\nb,\nf\nfrom a"),
    ("with a as (select b from c) Insert OVERWRITE table d.e PARTITION (f) select b, xyz f from a",
     "INSERT INTO d.e\nwith a as (select b from c)  SELECT\nb,\nxyz AS f\nfrom a"),
    ("with a as (select b from c) Insert OVERWRITE table d.e PARTITION (f='g') select b, xyz from a",
     "INSERT INTO d.e\nwith a as (select b from c)  SELECT\nb,\nxyz,\n'g'\nfrom a"),
    ("with a as (select b from c) Insert OVERWRITE table d.e PARTITION (f) select b, xyz from a",
     "INSERT INTO d.e\nwith a as (select b from c)  SELECT\nb,\nxyz,\nf\nfrom a"),
    ("""with a as (select 1) Insert OVERWRITE table d.e PARTITION (ld_dt) select d.aaa as bbb
        ,min(date(t1.ccc)) as task_date
        ,current_date as ld_dt  from cte""",
     """INSERT INTO d.e\nwith a as (select 1)  SELECT\nd.aaa AS bbb,\nmin(date(t1.ccc)) AS task_date,\ncurrent_date AS ld_dt\nfrom cte"""),
    ("""INSERT OVERWRITE TABLE db.table PARTITION ( pkey = '${pkey}') 
select distinct 
        a.sth,
        case when f(b.g) - unix_timestamp(a.h) between 0 and 7200 then 1 else 0 end as my_alias
from z a
left join zz b 
on a.zzz=b.zzz""",
     """INSERT INTO db.table
SELECT DISTINCT
a.sth,
case when f(b.g) - unix_timestamp(a.h) between 0 and 7200 then 1 else 0 end AS my_alias,
'${pkey}'
from z a
left join zz b 
on a.zzz=b.zzz""")
])
def test_move_insert_statement(query_section: str, expected: str) -> None:
    GHTP = global_translation.GlobalHiveToPresto()
    assert GHTP.move_insert_statement(query_section) == expected


@pytest.mark.parametrize(['query', 'expected'], [
    # Single statements
    # CTE management
    ("select cte.a from cte group by cte.a",
     "select cte.a from cte group by cte.a"),
    ("select cte.a from cte group by a",
     "select cte.a from cte group by a"),
    ("select a from cte group by cte.a",
     "select a from cte group by cte.a"),
    # Corner cases
    ("select cte.a a from cte group by cte.a",
     "select cte.a a from cte group by cte.a"),
    ("select cte.a a from cte group by a",
     "select cte.a a from cte group by cte.a"),
    ("select a a from cte group by cte.a",
     "select a a from cte group by cte.a"),
    ("select cte.a a from cte group by a a",
     "select cte.a a from cte group by cte.a as a"),
    # Others
    ("select a b from cte group by b c",
     "select a b from cte group by a as c"),
    ("select cast(year(my_date) as bigint) b from cte group by year(my_date)",
     "select cast(year(my_date) as bigint) b from cte group by year(my_date)"),
    ("select cast(year(a) as bigint) b from cte group by a",
     "select cast(year(a) as bigint) b from cte group by a"),
    ("select a from cte group by cte.a",
     "select a from cte group by cte.a"),  # Test cte in table description
    ("select 1, 1.1, '1' from cte group by 1",
     "select 1, 1.1, '1' from cte group by cast(1 as tinyint)"),
    ("select 1, 1.1, '1' from cte group by 1 as kk",
     "select 1, 1.1, '1' from cte group by cast(1 as tinyint) as kk"),
    ("select 1, 1.1, '1' from cte group by 1.1",
     "select 1, 1.1, '1' from cte group by 1.1"),
    ("select 1, 1.1, '1' from cte group by '1'",
     "select 1, 1.1, '1' from cte group by '1'"),
    ("select a, cast(b as bigint) from cte group by cast(b as bigint)",
     "select a, cast(b as bigint) from cte group by cast(b as bigint)"),
    # Keyword follows, check that the code exits properly
    ("select a from cte group by a union all select a from cte group by a",
     "select a from cte group by a union all select a from cte group by a"),  # union all is a Keyword
    ("select a from cte group by a order by a select a from cte group by a",
     "select a from cte group by a order by a select a from cte group by a"),  # order by is a Keyword
    # 2+ columns (comes as IdentifierList)
    ("with cte (select a from db.table group by a) select a from cte group by a",
     "with cte (select a from db.table group by a) select a from cte group by a"),
    ("select a, b from cte group by cte.a, cte.b",
     "select a, b from cte group by cte.a, cte.b"),  # Test cte in table description
    ("select cte1.a, cte2.b b from cte1, cte2 group by b",
     "select cte1.a, cte2.b b from cte1, cte2 group by cte2.b"),  # Test cte in table description
    ("select mycol, myothercol, max(sth) from cte group by mycol, myothercol",
     "select mycol, myothercol, max(sth) from cte group by mycol, myothercol"),  # No change needed
    ("select mycol, myothercol + 1 as yo, max(sth) from cte group by mycol, myothercol + 1 as useless_name",
     "select mycol, myothercol + 1 as yo, max(sth) from cte group by mycol, myothercol + 1 as useless_name"),  # No change needed
    ("select mycol, myothercol as yo, max(sth) from cte group by mycol, yo",
     "select mycol, myothercol as yo, max(sth) from cte group by mycol, myothercol"),
    ("select a, 1 as sth, 1.1 as sth2, '1' from cte group by a, 1, 1.1, '1'",
     "select a, 1 as sth, 1.1 as sth2, '1' from cte group by a, cast(1 as tinyint), 1.1, '1'"),  # All together
    ("select a, 1, 1.1, '1' from cte group by a, 1 as kk",
     "select a, 1, 1.1, '1' from cte group by a, cast(1 as tinyint) as kk"),
    ("select a, cast(b as bigint) from cte group by a, cast(b as bigint)",
     "select a, cast(b as bigint) from cte group by a, cast(b as bigint)"),
    # select * statements should only result in warnings
    ("select * from db.table", "select * from db.table"),
    ("select * from db.table group by b", "select * from db.table group by b"),
    ("select *, 1 from db.table group by b", "select *, 1 from db.table group by b"),
    # Fails, but do nothing with it
    ("select a, b from db.table group by c", "select a, b from db.table group by c")
])
def test_fix_group_by_calls(query: str, expected: str) -> None:
    GBT = global_translation.GroupByTranslator()
    assert GBT.fix_group_by_calls(query) == expected


@pytest.mark.parametrize(['query'], [
    ("select a from cte group by *",),
    ("select a from cte group by *, a",),
    ("select a from cte group by c.*",),
    ("select a from cte group by c.*, a",),
    ("select a from cte group by having a=3",),
    ("group by having a=3",)
])
def test_fix_group_by_calls_SyntaxError(query: str) -> None:
    GBT = global_translation.GroupByTranslator()
    with pytest.raises(SyntaxError):
        GBT.fix_group_by_calls(query)


@pytest.mark.parametrize(['query', 'expected'], [
    # Single column to extract
    ("select cte.mycol from cte group by", [({"cte.mycol", "cte", "mycol"}, None)]),
    ("select max(cte.b, 1.5) from cte group by", [({'max(cte.b, 1.5)', 'cte.b', 'cte.b, 1.5', 'cte', 'b', 'max', '1.5'}, None)]),
    ("select mycol from cte group by", [({"mycol"}, None)]),
    ("select mycol as yo from cte group by", [({"mycol"}, "yo")]),
    ("select cast(a AS bigint) from cte group by", [({'cast', 'a', 'cast(a AS bigint)', 'a AS bigint'}, None)]),  # Function
    ("select cast(a AS bigint) b from cte group by", [({'cast', 'cast(a AS bigint)', 'a', 'a AS bigint'}, "b")]),  # Identifier
    ("select '1' from cte group by", [({"'1'"}, None)]),  # Single
    ("select '1'   as     sth from cte group by", [({"'1'"}, "sth")]),  # Identifier
    ("select 1 from cte group by", [({"1"}, None)]),  # Integer
    ("select 1.5 from cte group by", [({"1.5"}, None)]),  # Float
    # 2+ columns (comes as IdentifierList)
    ("select cte.a, max(cte.b) c from cte group by", [({'cte.a', 'cte', 'a'}, None), ({'max(cte.b)', 'max', 'b', 'cte.b', 'cte'}, "c")]),
    ("select a, b as c, d  e from cte group by", [({"a"}, None), ({"b"}, "c"), ({"d"}, "e")]),
    ("select a, cast(b as bigint) from cte group by", [({"a"}, None), ({'cast', 'b', 'b as bigint', 'cast(b as bigint)'}, None)]),
    ("select a, '1', 1, 1.1 from cte group by", [({"a"}, None), ({"'1'"}, None), ({"1"}, None), ({"1.1"}, None)]),
    # Wildcard: return [] must be triggered
    ("select * from cte group by", []),  # First level
    ("select *, a from cte group by", []),  # From IdentifierList
    ("select c.* from cte group by", []),  # As part of Identifier
    ("select c.*, a from cte group by", []),  # From IdentifierList as part of an Identifier
    # Union/Union all management
    ("select a from cte1 group by a union select b from cte2 group by", [({"b"}, None)]),
    ("select a from cte1 group by a union all select b, c from cte2 group by", [({"b"}, None), ({"c"}, None)])
])
def test_get_columns_in_select(query: str, expected: Optional[List[Tuple[str, Optional[str]]]]) -> None:
    GBT = global_translation.GroupByTranslator()
    assert GBT.get_columns_in_select(sqlparse.parse(query)[0].tokens[-1]) == expected


@pytest.mark.parametrize(['query'], [
    ("select from cte group by sth group by",),
    ("select group by",)
])
def test_get_columns_in_select_SyntaxError(query: str) -> None:
    GBT = global_translation.GroupByTranslator()
    with pytest.raises(SyntaxError):
        GBT.get_columns_in_select(sqlparse.parse(query)[0].tokens[-1])


@pytest.mark.parametrize(['string_rep_integer', "expected"], [
    ("127", "tinyint"),
    ("-128", "tinyint"),
    ("32767", "smallint"),
    ("-32768", "smallint"),
    ("2147483647", "integer"),
    ("-2147483648", "integer"),
    ("2147483648", "bigint"),
    ("-2147483649", "bigint")
])
def test_least_integer_data_type(string_rep_integer: str, expected: str) -> None:
    GBT = global_translation.GroupByTranslator()
    assert GBT.least_integer_data_type(string_rep_integer) == expected
