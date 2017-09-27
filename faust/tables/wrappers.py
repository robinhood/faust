import operator
import typing
from typing import Any, Callable, Iterator, cast
from mode import Seconds
from ..types import EventT
from ..types.tables import TableT, WindowSetT, WindowWrapperT
from ..utils.collections import FastUserDict

if typing.TYPE_CHECKING:
    from .table import Table
else:
    class Table: ...  # noqa

__all__ = ['WindowSet', 'WindowWrapper']


class WindowSet(WindowSetT, FastUserDict):

    def __init__(self,
                 key: Any,
                 table: TableT,
                 event: EventT = None) -> None:
        self.key = key
        self.table = cast(Table, table)
        self.event = event
        self.data = table  # provides underlying mapping in FastUserDict

    def apply(self, op: Callable[[Any, Any], Any], value: Any,
              event: EventT = None) -> WindowSetT:
        cast(Table, self.table)._apply_window_op(
            op, self.key, value, event or self.event)
        return self

    def now(self) -> Any:
        return cast(Table, self.table)._windowed_now(self.key)

    def current(self, event: EventT = None) -> Any:
        return cast(Table, self.table)._windowed_current(
            self.key, event or self.event)

    def delta(self, d: Seconds, event: EventT = None) -> Any:
        return cast(Table, self.table)._windowed_delta(
            self.key, d, event or self.event)

    def __getitem__(self, w: Any) -> Any:
        # wrapper[key][event] returns WindowSet with event already set.
        if isinstance(w, EventT):
            return type(self)(self.key, self.table, w)
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

    def __init__(self, table: TableT) -> None:
        self.table = table

    def __getitem__(self, key: Any) -> WindowSetT:
        return WindowSet(key, self.table)

    def __setitem__(self, key: Any, value: Any) -> None:
        if not isinstance(value, WindowSetT):
            cast(Table, self.table)._set_windowed(key, value)

    def __delitem__(self, key: Any) -> None:
        cast(Table, self.table)._del_windowed(key)

    def __iter__(self) -> Iterator:
        return iter(self.table)

    def __len__(self) -> int:
        return len(self.table)
