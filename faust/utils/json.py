"""JSON utilities."""
import datetime
import enum
import typing
import uuid
from collections import deque
from decimal import Decimal
from typing import Any, Callable, List, Optional, Tuple, Type

__all__ = [
    'JSONEncoder',
    'dumps',
    'loads',
    'str_to_decimal',
]

if typing.TYPE_CHECKING:
    import orjson
else:  # pragma: no cover
    try:
        import orjson
    except ImportError:
        orjson = None  # noqa

DEFAULT_TEXTUAL_TYPES: List[Type] = [Decimal, uuid.UUID, bytes]

try:  # pragma: no cover
    from django.utils.functional import Promise
    DJANGO_TEXTUAL_TYPES = [Promise]
except ImportError:
    DJANGO_TEXTUAL_TYPES = []

TEXTUAL_TYPES: Tuple[Type, ...] = tuple(
    DEFAULT_TEXTUAL_TYPES + DJANGO_TEXTUAL_TYPES)

try:  # pragma: no cover
    import simplejson as json

    # simplejson converts Decimal to float by default, i.e. before
    # we have a chance to override it using Encoder.default.
    _JSON_DEFAULT_KWARGS = {
        'use_decimal': False,
        'namedtuple_as_object': False,
    }

except ImportError:  # pragma: no cover
    import json  # type: ignore
    _JSON_DEFAULT_KWARGS = {}

#: Max length for string to be converted to decimal.
DECIMAL_MAXLEN = 1000

#: Types that we convert to lists.
SEQUENCE_TYPES: Tuple[type, ...] = (set, deque)

#: Types that are datetimes and dates (-> .isoformat())
DATE_TYPES: Tuple[type, ...] = (datetime.date, datetime.time)

#: Types we use `return obj.value` for (Enum)
VALUE_DELEGATE_TYPES: Tuple[type, ...] = (enum.Enum,)

HAS_TIME: Tuple[type, ...] = (datetime.datetime, datetime.time)


def str_to_decimal(s: str, maxlen: int = DECIMAL_MAXLEN) -> Optional[Decimal]:
    """Convert string to :class:`~decimal.Decimal`.

    Args:
        s (str): Number to convert.
        maxlen (int): Max length of string.  Default is 100.

    Raises:
        ValueError: if length exceeds maximum length, or if value is not
            a valid number (e.g. Inf, NaN or sNaN).

    Returns:
        Decimal: Converted number.
    """
    if s is None:
        return None
    if len(s) > maxlen:
        raise ValueError(
            f'string of length {len(s)} is longer than limit ({maxlen})')
    v = Decimal(s)
    if not v.is_finite():  # check for Inf/NaN/sNaN/qNaN
        raise ValueError(f'Illegal value in decimal: {s!r}')
    return v


def on_default(o: Any,
               *,
               sequences: Tuple[type, ...] = SEQUENCE_TYPES,
               dates: Tuple[type, ...] = DATE_TYPES,
               value_delegate: Tuple[type, ...] = VALUE_DELEGATE_TYPES,
               has_time: Tuple[type, ...] = HAS_TIME,
               _isinstance: Callable = isinstance,
               _str: Callable = str,
               _list: Callable = list,
               textual: Tuple[type, ...] = TEXTUAL_TYPES) -> Any:
    if _isinstance(o, textual):
        return _str(o)
    elif _isinstance(o, dates):
        if not _isinstance(o, has_time):
            o = datetime.datetime(o.year, o.month, o.day, 0, 0, 0, 0)
        r = o.isoformat()
        if r.endswith('+00:00'):
            r = r[:-6] + 'Z'
        return r
    elif isinstance(o, value_delegate):
        return o.value
    elif isinstance(o, sequences):
        return _list(o)
    else:
        to_json = getattr(o, '__json__', None)
        if to_json is not None:
            return to_json()
        raise TypeError(
            f'JSON cannot serialize {type(o).__name__!r}: {o!r}')


class JSONEncoder(json.JSONEncoder):
    """Faust customized :class:`json.JSONEncoder`.

    Our version supports additional types like :class:`~uuid.UUID`, and
    importantly includes microsecond information in datetimes.
    """

    def default(self, o: Any, *,
                callback: Callable[[Any], Any] = on_default) -> Any:
        return callback(o)


if orjson is not None:
    ErrorTuple = Tuple[Type[BaseException], ...]

    def dumps(obj: Any,
              json_dumps: Callable = orjson.dumps,
              errors: ErrorTuple = (orjson.JSONEncodeError,),
              cls: Type[JSONEncoder] = JSONEncoder,
              **kwargs: Any) -> str:
        """Serialize to json."""
        try:
            return json_dumps(
                obj,
                default=on_default,
                options=orjson.OPT_NAIVE_UTC,
            )
        except errors as exc:
            raise TypeError(str(exc)) from None

    def loads(s: str,
              json_loads: Callable = orjson.loads,
              errors: ErrorTuple = (orjson.JSONDecodeError,),
              **kwargs: Any) -> Any:
        """Deserialize json string."""
        try:
            return json_loads(s)
        except errors as exc:
            raise ValueError(str(exc)) from None
else:

    def dumps(obj: Any,
              json_dumps: Callable = json.dumps,
              errors: ErrorTuple = (),
              cls: Type[JSONEncoder] = JSONEncoder,
              **kwargs: Any) -> str:
        """Serialize to json.  See :func:`json.dumps`."""
        return json_dumps(obj, cls=cls, **dict(_JSON_DEFAULT_KWARGS, **kwargs))

    def loads(s: str,
              json_loads: Callable = json.loads,
              errors: ErrorTuple = (),
              **kwargs: Any) -> Any:
        """Deserialize json string.  See :func:`json.loads`."""
        return json_loads(s, **kwargs)
