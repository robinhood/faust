import pytest
from datetime import datetime, timezone
from faust.utils._iso8601_python import InvalidTZ, parse, parse_tz


def test_python():
    dt1 = datetime.now().astimezone(timezone.utc)
    dt2 = parse(dt1.isoformat())
    assert dt1 == dt2


def test_timezone_no_sep():
    dt = parse('2018-12-04T19:36:08-0500')
    assert dt.tzinfo
    assert str(dt.tzinfo) == 'UTC-05:00'


def test_parse_error():
    with pytest.raises(ValueError):
        parse('foo')


@pytest.mark.parametrize('tz', [
    'Z',
    '+00:10',
    '-01:20',
    '+0300',
    '-0600',
])
def test_parse_tz(tz):
    assert parse_tz(tz) is not None


def test_parse_tz__no_match():
    with pytest.raises(InvalidTZ):
        parse_tz('foo')
