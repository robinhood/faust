"""Wrappers for windowed tables."""
import operator
import typing
from datetime import datetime
from typing import Any, Callable, Iterator, Optional, cast

from mode import Seconds
from mode.utils.collections import FastUserDict

from faust.exceptions import ImproperlyConfigured
from faust.streams import current_event
from faust.types import EventT, FieldDescriptorT
from faust.types.tables import (
    RecoverCallback,
    RelativeArg,
    RelativeHandler,
    TableT,
    WindowSetT,
    WindowWrapperT,
)

if typing.TYPE_CHECKING:  # pragma: no cover
    from .table import Table
else:
    class Table: ...  # noqa

__all__ = ['WindowSet', 'WindowWrapper']


class WindowSet(WindowSetT, FastUserDict):
    """Represents the windows available for table key.

    ``Table[k]`` returns WindowSet since ``k`` can exist in multiple
    windows, and to retrieve an actual item we need a timestamp.

    The timestamp of the current event (if this is executing in a stream
    processor), can be used by accessing ``.current()``::

        Table[k].current()

    similarly the most recent value can be accessed using ``.now()``::

        Table[k].now()

    from delta of the time of the current event::

        Table[k].delta(timedelta(hours=3))

    or delta from time of other event::

        Table[k].delta(timedelta(hours=3), other_event)

    """

    def __init__(self,
                 key: Any,
                 table: TableT,
                 wrapper: WindowWrapperT,
                 event: EventT = None) -> None:
        self.key = key
        self.table = cast(Table, table)
        self.wrapper = wrapper
        self.event = event
        self.data = table  # provides underlying mapping in FastUserDict

    def apply(self,
              op: Callable[[Any, Any], Any],
              value: Any,
              event: EventT = None) -> WindowSetT:
        table = cast(Table, self.table)
        timestamp = self.wrapper.get_timestamp(event or self.event)
        table._apply_window_op(op, self.key, value, timestamp)
        return self

    def value(self, event: EventT = None) -> Any:
        return cast(Table, self.table)._windowed_timestamp(
            self.key, self.wrapper.get_timestamp(event or self.event))

    def now(self) -> Any:
        return cast(Table, self.table)._windowed_now(self.key)

    def current(self, event: EventT = None) -> Any:
        t = cast(Table, self.table)
        return t._windowed_timestamp(
            self.key, t._relative_event(event or self.event))

    def delta(self, d: Seconds, event: EventT = None) -> Any:
        table = cast(Table, self.table)
        return table._windowed_delta(self.key, d, event or self.event)

    def __getitem__(self, w: Any) -> Any:
        # wrapper[key][event] returns WindowSet with event already set.
        if isinstance(w, EventT):
            return type(self)(self.key, self.table, self.wrapper, w)
        # wrapper[key][window_range] returns value for that range.
        return self.table[self.key, w]

    def __setitem__(self, w: Any, value: Any) -> None:
        if isinstance(w, EventT):
            raise NotImplementedError(
                'Cannot set WindowSet key, when key is an event')
        self.table[self.key, w] = value

    def __delitem__(self, w: Any) -> None:
        if isinstance(w, EventT):
            raise NotImplementedError(
                'Cannot delete WindowSet key, when key is an event')
        del self.table[self.key, w]

    def __iadd__(self, other: Any) -> Any:
        return self.apply(operator.add, other)

    def __isub__(self, other: Any) -> Any:
        return self.apply(operator.sub, other)

    def __imul__(self, other: Any) -> Any:
        return self.apply(operator.mul, other)

    def __itruediv__(self, other: Any) -> Any:
        return self.apply(operator.truediv, other)

    def __ifloordiv__(self, other: Any) -> Any:
        return self.apply(operator.floordiv, other)

    def __imod__(self, other: Any) -> Any:
        return self.apply(operator.mod, other)

    def __ipow__(self, other: Any) -> Any:
        return self.apply(operator.pow, other)

    def __ilshift__(self, other: Any) -> Any:
        return self.apply(operator.lshift, other)

    def __irshift__(self, other: Any) -> Any:
        return self.apply(operator.rshift, other)

    def __iand__(self, other: Any) -> Any:
        return self.apply(operator.and_, other)

    def __ixor__(self, other: Any) -> Any:
        return self.apply(operator.xor, other)

    def __ior__(self, other: Any) -> Any:
        return self.apply(operator.or_, other)

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: table={self.table}>'


class WindowWrapper(WindowWrapperT):
    """Windowed table wrapper.

    A windowed table does not return concrete values when keys are
    accessed, instead :class:`WindowSet` is returned so that
    the values can be further reduced to the wanted time period.
    """

    def __init__(self, table: TableT, *,
                 relative_to: RelativeArg = None) -> None:
        self.table = table
        self._get_relative_timestamp = self._relative_handler(relative_to)

    def clone(self, relative_to: RelativeArg) -> WindowWrapperT:
        return type(self)(
            table=self.table,
            relative_to=relative_to or self._get_relative_timestamp,
        )

    @property
    def name(self) -> str:
        return self.table.name

    def relative_to(self, ts: RelativeArg) -> WindowWrapperT:
        return self.clone(relative_to=ts)

    def relative_to_now(self) -> WindowWrapperT:
        return self.clone(relative_to=self.table._relative_now)

    def relative_to_field(self, field: FieldDescriptorT) -> WindowWrapperT:
        return self.clone(relative_to=self.table._relative_field(field))

    def relative_to_stream(self) -> WindowWrapperT:
        return self.clone(relative_to=self.table._relative_event)

    def get_timestamp(self, event: EventT = None) -> float:
        event = event or current_event()
        if event is None:
            raise TypeError('Operation outside of stream iteration')
        get_relative_timestamp = self.get_relative_timestamp
        if get_relative_timestamp:
            timestamp = get_relative_timestamp(event)
            if isinstance(timestamp, datetime):
                return timestamp.timestamp()
            return timestamp
        return event.message.timestamp

    def on_recover(self, fun: RecoverCallback) -> RecoverCallback:
        return self.table.on_recover(fun)

    def __contains__(self, key: Any) -> bool:
        return self.table._windowed_contains(key, self.get_timestamp())

    def __getitem__(self, key: Any) -> WindowSetT:
        return WindowSet(key, self.table, self)

    def __setitem__(self, key: Any, value: Any) -> None:
        if not isinstance(value, WindowSetT):
            table = cast(Table, self.table)
            table._set_windowed(key, value, self.get_timestamp())

    def __delitem__(self, key: Any) -> None:
        cast(Table, self.table)._del_windowed(key, self.get_timestamp())

    def __iter__(self) -> Iterator:
        return iter(self.table)

    def __len__(self) -> int:
        return len(self.table)

    def _relative_handler(
            self, relative_to: RelativeArg) -> Optional[RelativeHandler]:
        if relative_to is None:
            return None
        elif isinstance(relative_to, datetime):
            return self.table._relative_timestamp(relative_to.timestamp())
        elif isinstance(relative_to, float):
            return self.table._relative_timestamp(relative_to)
        elif isinstance(relative_to, FieldDescriptorT):
            return self.table._relative_field(relative_to)
        elif callable(relative_to):
            return relative_to
        raise ImproperlyConfigured(
            f'Relative cannot be type {type(relative_to)}')

    @property
    def get_relative_timestamp(self) -> Optional[RelativeHandler]:
        return self._get_relative_timestamp

    @get_relative_timestamp.setter
    def get_relative_timestamp(self, relative_to: RelativeArg) -> None:
        self._get_relative_timestamp = self._relative_handler(relative_to)
