from sql_translate.engine import special_functions_handling
import pytest

_SpecialFunctionHandler = special_functions_handling._SpecialFunctionHandler()


@pytest.mark.parametrize(['datetime_format', 'expected'], [
    ('yyyy-MM-dd HH:mm:ss', '%Y-%m-%d %k:%i:%s'),
    ('yyyy-MM-dd', '%Y-%m-%d'),
    ('yyyyMM', '%Y%m'),
    ('yyyyMMdd', '%Y%m%d'),
    ('yMMdd', '%Y%m%d'),
    ('yMdd', '%Y%c%d'),
    ('y-MM-01', '%Y-%m-01')
])
def test_translate_datetime_format(datetime_format: str, expected: str) -> None:
    SpecialFunctionHandlerHiveToPresto = special_functions_handling.SpecialFunctionHandlerHiveToPresto()
    assert SpecialFunctionHandlerHiveToPresto._translate_datetime_format(datetime_format) == expected
