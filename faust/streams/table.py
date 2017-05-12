"""Tables (changelog stream)."""
import operator
from datetime import timedelta
from typing import Any, Callable, Mapping, Type, cast
from .. import stores
from .. import windows
from ..types import AppT, Event
from ..types.stores import StoreT
from ..types.tables import TableT
from ..types.windows import WindowT
from ..utils.collections import ManagedUserDict
from .stream import Stream, current_event

__all__ = ['Table']


class Table(Stream, TableT, ManagedUserDict):

    _store: str

    def __init__(self, app: AppT,
                 *,
                 table_name: str = None,
                 default: Callable[[], Any] = None,
                 store: str = None,
                 window: WindowT = None,
                 key_type: Type = None,
                 value_type: Type = None,
                 **kwargs: Any) -> None:
        Stream.__init__(self, app, **kwargs)
        assert not self._coroutine  # Table cannot have generator callback.
        self.table_name = table_name
        self.default = default
        self._store = store
        self.data = {}
        self.window = window
        self.key_type = key_type
        self.value_type = value_type

        if self.StateStore is not None:
            self.data = self.StateStore(url=None, app=app, loop=self.loop)
        else:
            url = self._store or self.app.store
            self.data = stores.by_url(url)(
                url, app,
                table_name=self.table_name,
                loop=self.loop)

        # Table.start() also starts Store
        self.add_dependency(cast(StoreT, self.data))
        self.changelog_topic = self.app.topic(
            self._changelog_topic_name(),
            key_type=self.key_type,
            value_type=self.value_type,
        )
        self._sensor_on_get = self.app.sensors.on_table_get
        self._sensor_on_set = self.app.sensors.on_table_set
        self._sensor_on_del = self.app.sensors.on_table_del

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app._tables mapping.
        return Stream.__hash__(self)

    def __missing__(self, key: Any) -> Any:
        if self.default is not None:
            value = self[key] = self.default()
            return value
        raise KeyError(key)

    def hopping(self, size: float, step: float,
                expires: float = None) -> 'WindowWrapper':
        return WindowWrapper(self, windows.HoppingWindow(size, step, expires))

    def tumbling(self, size: float, expires: float = None) -> 'WindowWrapper':
        return WindowWrapper(self, windows.TumblingWindow(size, expires))

    def sliding(self,
                before: float,
                after: float,
                expires: float) -> 'WindowWrapper':
        return WindowWrapper(
            self, windows.SlidingWindow(before, after, expires))

    def info(self) -> Mapping[str, Any]:
        # Used to recreate object in .clone()
        return {**super().info(), **{
            'table_name': self.table_name,
            'store': self._store,
            'default': self.default,
            'window': self.window,
        }}

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any, value: Any) -> None:
        self._send_changelog(key, value)
        self._sensor_on_set(self, key, value)

    def on_key_del(self, key: Any) -> None:
        self._send_changelog(key, value=None)
        self._sensor_on_del(self, key)

    def _send_changelog(self, key: Any, value: Any) -> None:
        event = current_event()
        partition: int = None
        if event is not None:
            send = event.attach
            partition = event.req.message.partition
        else:
            send = self.app.send_soon
        send(self.changelog_topic, key, value,
             key_serializer='json',
             value_serializer='json',
             partition=partition)

    def _changelog_topic_name(self) -> str:
        return '{0.app.id}-{0.table_name}-changelog'.format(self)

    def __repr__(self) -> str:
        return Stream.__repr__(self)

    @property
    def label(self) -> str:
        return '{}: {}@{}'.format(
            type(self).__name__, self.table_name, self._store,
        )


class WindowWrapper:
    table: TableT
    window: WindowT

    def __init__(self, table: TableT, window: WindowT) -> None:
        self.table = table
        self.window = window

    def __getitem__(self, key: Any) -> 'WindowSet':
        return WindowSet(key, self.table, self.window)


class WindowSet:
    key: Any
    table: TableT
    window: WindowT

    def __init__(self, key: Any, table: TableT, window: WindowT):
        self.key = key
        self.table = table
        self.window = window

    def _op(self, op: Callable[[Any, Any], Any], other: Any):
        table = self.table
        key = self.key
        timestamp = current_event().req.message.timestamp
        for window_range in self.window.windows(timestamp):
            table[(key, window_range)] = op(table[key], other)

    def current(self, event: Event = None) -> Any:
        event = event or current_event()
        timestamp = event.req.message.timestamp
        return self.table[(self.key, self.window.current_window(timestamp))]

    def delta(self, d: timedelta, event: Event = None) -> Any:
        event = event or current_event()
        timestamp = event.req.message.timestamp
        return self.table[(self.key, self.window.delta(timestamp, d))]

    def __getitem__(self, w: Any) -> Any:
        return self.table[(self.key, w)]

    def __setitem__(self, w: Any, value: Any) -> None:
        self.table[(self.key, w)] = value

    def __delitem__(self, w: Any) -> None:
        del self.table[(self.key, w)]

    def __iadd__(self, other: Any) -> Any:
        return self._op(operator.add, other)

    def __isub__(self, other: Any) -> Any:
        return self._op(operator.sub, other)

    def __imul__(self, other: Any) -> Any:
        return self._op(operator.mul, other)

    def __idiv__(self, other: Any) -> Any:
        return self._op(operator.div, other)

    def __itruediv__(self, other: Any) -> Any:
        return self._op(operator.truediv, other)

    def __ifloordiv__(self, other: Any) -> Any:
        return self._op(operator.floordiv, other)

    def __imod__(self, other: Any) -> Any:
        return self._op(operator.mod, other)

    def __ipow__(self, other: Any) -> Any:
        return self._op(operator.pow, other)

    def __ilshift__(self, other: Any) -> Any:
        return self._op(operator.lshift, other)

    def __irshift__(self, other: Any) -> Any:
        return self._op(operator.rshift, other)

    def __iand__(self, other: Any) -> Any:
        return self._op(operator.and_, other)

    def __ixor__(self, other: Any) -> Any:
        return self._op(operator.xor, other)

    def __ior__(self, other: Any) -> Any:
        return self._op(operator.or_, other)
