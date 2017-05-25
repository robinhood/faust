"""Tables (changelog stream)."""
import operator
from typing import Any, Callable, Iterator, Mapping, Type, cast
from . import stores
from . import windows
from .types import AppT, EventT, FieldDescriptorT, JoinT
from .types.stores import StoreT
from .types.streams import JoinableT, StreamT
from .types.tables import TableT, WindowSetT, WindowWrapperT
from .types.windows import WindowT
from .utils.collections import FastUserDict, ManagedUserDict
from .utils.services import Service
from .utils.times import Seconds
from .streams import current_event
from .streams import joins
from .transport import aiokafka

__all__ = ['Table']


class Table(Service, TableT, ManagedUserDict):

    _store: str

    def __init__(self, app: AppT,
                 *,
                 table_name: str = None,
                 default: Callable[[], Any] = None,
                 store: str = None,
                 key_type: Type = None,
                 value_type: Type = None,
                 **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self.table_name = table_name
        self.default = default
        self._store = store
        self.key_type = key_type
        self.value_type = value_type
        self.changelog_topic = self.app.topic(
            self._changelog_topic_name(),
            key_type=self.key_type,
            value_type=self.value_type,
            partitions=self.partitions,
            config=aiokafka.changelog_config(),
        )

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

        # Aliases
        self._sensor_on_get = self.app.sensors.on_table_get
        self._sensor_on_set = self.app.sensors.on_table_set
        self._sensor_on_del = self.app.sensors.on_table_del

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app.tables mapping.
        return object.__hash__(self)

    def __missing__(self, key: Any) -> Any:
        if self.default is not None:
            value = self[key] = self.default()
            return value
        raise KeyError(key)

    async def on_start(self) -> None:
        await self.changelog_topic.maybe_declare()

    def using_window(self, window: WindowT) -> WindowWrapperT:
        config = aiokafka.changelog_config(retention=window.expires)
        self.changelog_topic = self.changelog_topic.derive(config=config)
        return WindowWrapper(self, window)

    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.HoppingWindow(size, step, expires))

    def tumbling(self, size: Seconds,
                 expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.TumblingWindow(size, expires))

    def info(self) -> Mapping[str, Any]:
        # Used to recreate object in .clone()
        return {
            'app': self.app,
            'table_name': self.table_name,
            'store': self._store,
            'default': self.default,
            'key_type': self.key_type,
            'value_type': self.value_type,
        }

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
            partition = event.message.partition
        else:
            send = self.app.send_soon
        send(self.changelog_topic, key, value,
             key_serializer='json',
             value_serializer='json',
             partition=partition)

    def _changelog_topic_name(self) -> str:
        return f'{self.app.id}-{self.table_name}-changelog'

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.LeftJoin(stream=self, fields=fields))

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.InnerJoin(stream=self, fields=fields))

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.OuterJoin(stream=self, fields=fields))

    def _join(self, join_strategy: JoinT) -> StreamT:
        # TODO
        raise NotImplementedError('TODO')

    def clone(self, **kwargs: Any) -> Any:
        return self.__class__(**{**self.info(), **kwargs})

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        # TODO
        raise NotImplementedError('TODO')

    def __copy__(self) -> Any:
        return self.clone()

    def __and__(self, other: JoinableT) -> StreamT:
        return self.combine(self, other)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self.table_name}@{self._store}'


class WindowSet(WindowSetT, FastUserDict):

    def __init__(self,
                 key: Any,
                 table: TableT,
                 window: WindowT,
                 event: EventT = None) -> None:
        self.key = key
        self.table = table
        self.window = window
        self.event = event
        self.data = table  # provides underlying mapping in FastUserDict

    def apply(self,
              op: Callable[[Any, Any], Any],
              value: Any,
              event: EventT = None) -> WindowSetT:
        table = self.table
        key = self.key
        timestamp = self.timestamp(event)
        for window_range in self.window.windows(timestamp):
            table[key, window_range] = op(table[key], value)
        return self

    def timestamp(self, event: EventT = None) -> float:
        return (event or self.event or current_event()).message.timestamp

    def current(self, event: EventT = None) -> Any:
        timestamp = self.timestamp(event)
        return self.table[self.key, self.window.current(timestamp)]

    def delta(self, d: Seconds, event: EventT = None) -> Any:
        timestamp = self.timestamp(event)
        return self.table[self.key, self.window.delta(timestamp, d)]

    def __getitem__(self, w: Any) -> Any:
        # wrapper[key][event] returns WindowSet with event already set.
        if isinstance(w, EventT):
            return type(self)(self.key, self.table, self.window, w)
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


class WindowWrapper(WindowWrapperT):

    def __init__(self, table: TableT, window: WindowT) -> None:
        self.table = table
        self.window = window

    def __getitem__(self, key: Any) -> WindowSetT:
        return WindowSet(key, self.table, self.window)

    def __iter__(self) -> Iterator:
        return iter(self.table)

    def __len__(self) -> int:
        return len(self.table)
