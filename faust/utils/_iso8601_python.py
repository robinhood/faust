"""Parsing ISO dates.

Originally taken from pyiso8601 (http://code.google.com/p/pyiso8601/)

Modified to match the behavior of dateutil.parser:

    - raise ValueError instead of ParseError
    - return naive datetimes by default
    - uses pytz.FixedOffset

This is the original License:

Copyright (c) 2007 Michael Twomey

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import re
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Mapping, Match, Optional, Pattern, cast

__all__ = ['parse']

# Adapted from http://delete.me.uk/2005/03/iso8601.html
RE_ISO8601: Pattern = re.compile(  # noqa
    r'(?P<year>[0-9]{4})(-(?P<month>[0-9]{1,2})(-(?P<day>[0-9]{1,2})'
    r'((?P<separator>.)(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2})'
    '(:(?P<second>[0-9]{2})(\.(?P<microsecond>[0-9]+))?)?'
    r'(?P<timezone>Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?')
RE_TIMEZONE: Pattern = re.compile(
    '(?P<prefix>[+-])(?P<hours>[0-9]{2}).(?P<minutes>[0-9]{2})')


class InvalidTZ(Exception):
    """Isoformat date does not have a valid timezone."""


def parse(datestring: str) -> datetime:
    """Parse and convert ISO 8601 string into a datetime object."""
    m = RE_ISO8601.match(datestring)
    if not m:
        raise ValueError('unable to parse date string %r' % datestring)
    groups = cast(Mapping[str, str], m.groupdict())
    return datetime(
        int(groups['year']),
        int(groups['month']),
        int(groups['day']),
        int(groups['hour'] or 0),
        int(groups['minute'] or 0),
        int(groups['second'] or 0),
        int(groups['microsecond'] or 0),
        parse_tz(groups['timezone']) if groups['timezone'] else None,
    )


def parse_tz(tz: str) -> tzinfo:
    if tz == 'Z':
        return timezone.utc
    match: Optional[Match] = RE_TIMEZONE.match(tz)
    if match is not None:
        prefix, hours, minutes = match.groups()
        return _apply_tz_prefix(prefix, int(hours), int(minutes))
    raise InvalidTZ(f'Missing or invalid timezone information: {tz!r}')


def _apply_tz_prefix(prefix: str, hours: int, minutes: int) -> tzinfo:
    if prefix == '-':
        hours = -hours
        minutes = -minutes
    return timezone(timedelta(minutes=(minutes + (hours * 60))))
