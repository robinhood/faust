from datetime import datetime, timezone
from faust.utils._iso8601_python import parse


def test_python():
    dt1 = datetime.now().astimezone(timezone.utc)
    dt2 = parse(dt1.isoformat())
    assert dt1 == dt2


def test_timezone_no_sep():
    dt = parse('2018-12-04T19:36:08-0500')
    assert dt.tzinfo
    assert str(dt.tzinfo) == 'UTC-05:00'
