"""Tables (changelog stream)."""
import asyncio
import operator
from collections import defaultdict
from heapq import heappush, heappop
from itertools import cycle
from typing import (
    Any, AsyncIterator, Callable, Iterable, Iterator, List, Mapping,
    MutableMapping, MutableSet, Type, cast,
)
from . import stores
from . import windows
from .types import (
    AppT, EventT, FieldDescriptorT, JoinT, TopicT, TopicPartition,
)
from .types.models import ModelT
from .types.stores import StoreT
from .types.streams import JoinableT, StreamT
from .types.tables import TableT, WindowSetT, WindowWrapperT, TableManagerT
from .types.topics import SourceT
from .types.windows import WindowRange, WindowT
from .utils.aiter import aiter
from .utils.collections import FastUserDict, ManagedUserDict
from .utils.logging import get_logger
from .utils.services import Service
from .utils.times import Seconds
from .streams import current_event
from .streams import joins

__all__ = ['Table', 'TableManager']

logger = get_logger(__name__)


class TableManager(Service, TableManagerT, FastUserDict):
    _sources: MutableMapping[TableT, SourceT]
    _changelogs: MutableMapping[str, TableT]
    _new_assignments: asyncio.Queue

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.data = {}
        self._sources = {}
        self._changelogs = {}
        self._new_assignments = asyncio.Queue(loop=self.loop)

    async def on_start(self) -> None:
        self._sources.update({
            table: cast(SourceT, aiter(table.changelog_topic))
            for table in self.values()
        })
        self._changelogs.update({
            table.changelog_topic.topics[0]: table
            for table in self.values()
        })
        await self.app.consumer.pause_partitions(
            self._get_changelog_partitions())

    def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        self._new_assignments.put_nowait(assigned)

    async def _recover_from_changelog(self, table: TableT) -> None:
        cast(Table, table).raw_update({
            e.key: e.value
            async for e in self._until_highwater(table, self._sources[table])
        })

    def _get_changelog_partitions(self) -> Iterable[TopicPartition]:
        return {
            tp for tp in self.app.consumer.assignment()
            if tp.topic in self._changelogs
        }

    async def _until_highwater(
            self, table: TableT, source: SourceT) -> AsyncIterator[EventT]:
        consumer = self.app.consumer

        # Wait for TopicManager to finish any new subscriptions
        await self.app.sources.wait_for_subscriptions()

        tps: Sequence[TopicPartition]
        for delay in cycle([.1, .2, .3, .5, .8, 1.0]):
            tps = self._get_changelog_partitions()
            if tps:
                break
            await self.sleep(delay)
        # Set offset of partition to beginning
        # TODO Change to seek_to_beginning once implmented in
        await consumer.reset_offset_earliest(*tps)
        highwater = {tp: consumer.highwater(tp) for tp in tps}
        print('HIGHWATER: %r' % (highwater,))
        pending_tps = {tp for tp in tps if highwater[tp] is not None}
        print('PENDING TPS: %r' % (pending_tps,))
        if pending_tps:
            logger.info('[Table %r]: Recover from changelog (%r entries)',
                        table.name, sum(h for h in highwater.values() if h))
            await consumer.resume_partitions(pending_tps)
            try:
                async for event in source:
                    message = event.message
                    cur_hw = highwater.get(message.tp)
                    if cur_hw is None or message.offset > cur_hw:
                        pending_tps.discard(message.tp)
                        if not pending_tps:
                            break
                    yield event
            finally:
                await consumer.pause_partitions(tps)
            logger.info('[Table %r]: Recovery completed', table.name)

    @Service.task
    async def _new_assignments_handler(self) -> None:
        while not self.should_stop:
            assigned = await self._new_assignments.get()
            logger.info('[TableManager]: New assignments found')
            changelog_topics = set()
            for table in self.values():
                changelog_topics.update(set(table.changelog_topic.topics))
            await self.app.consumer.pause_partitions(assigned)
            for table in self.values():
                # TODO If standby ready, just swap and continue.
                await self._recover_from_changelog(table)
            await self.app.consumer.resume_partitions({
                tp for tp in assigned
                if tp.topic not in changelog_topics
            })
            logger.info('[TableManager]: New assignments handled')
            print('FETCHABLE: %r' %
                    (self.app.consumer._consumer._subscription.fetchable_partitions(),))


class Table(Service, TableT, ManagedUserDict):

    _store: str
    _changelog_topic: TopicT
    _timestamp_keys: MutableMapping[float, MutableSet]
    _timestamps: List[float]

    def __init__(self, app: AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: str = None,
                 key_type: Type[ModelT] = None,
                 value_type: Type[ModelT] = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self.name = name
        self.default = default
        self._store = store
        self.key_type = key_type
        self.value_type = value_type
        self.partitions = partitions
        self.window = window
        self.changelog_topic = changelog_topic

        # Table key expiration
        self._timestamp_keys = defaultdict(set)
        self._timestamps = []

        if self.StateStore is not None:
            self.data = self.StateStore(url=None, app=app, loop=self.loop)
        else:
            url = self._store or self.app.store
            self.data = stores.by_url(url)(
                url, app,
                table_name=self.name,
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
        self.window = window
        self.changelog_topic = self._new_changelog_topic(
            retention=window.expires,
            compacting=True,
            deleting=True,
        )
        return WindowWrapper(self)

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
            'name': self.name,
            'store': self._store,
            'default': self.default,
            'key_type': self.key_type,
            'value_type': self.value_type,
            'changelog_topic': self.changelog_topic,
            'window': self.window,
        }

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any, value: Any) -> None:
        self._send_changelog(key, value)
        self._maybe_set_key_ttl(key)
        self._sensor_on_set(self, key, value)

    def on_key_del(self, key: Any) -> None:
        self._send_changelog(key, value=None)
        self._maybe_del_key_ttl(key)
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

    @Service.task
    async def _clean_data(self) -> None:
        if self._should_expire_keys():
            timestamps = self._timestamps
            window = self.window
            while not self.should_stop:
                while timestamps and window.stale(timestamps[0]):
                    timestamp = heappop(timestamps)
                    for key in self._timestamp_keys[timestamp]:
                        del self.data[key]
                    del self._timestamp_keys[timestamp]
                await self.sleep(self.app.table_cleanup_interval)

    def _should_expire_keys(self) -> bool:
        window = self.window
        return not (window is None or window.expires is None)

    def _maybe_set_key_ttl(self, key: Any) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        heappush(self._timestamps, window_range.end)
        self._timestamp_keys[window_range.end].add(key)

    def _maybe_del_key_ttl(self, key: Any) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        ts_keys = self._timestamp_keys.get(window_range.end)
        ts_keys.discard(key)

    def _changelog_topic_name(self) -> str:
        return f'{self.app.id}-{self.name}-changelog'

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

    def _new_changelog_topic(self, *,
                             retention: Seconds = None,
                             compacting: bool = True,
                             deleting: bool = None) -> TopicT:
        return self.app.topic(
            self._changelog_topic_name(),
            key_type=self.key_type,
            value_type=self.value_type,
            partitions=self.partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
        )

    def __copy__(self) -> Any:
        return self.clone()

    def __and__(self, other: JoinableT) -> StreamT:
        return self.combine(self, other)

    def _apply_window_op(self,
                         op: Callable[[Any, Any], Any],
                         key: Any,
                         value: Any,
                         event: EventT = None) -> None:
        for window_range in self._window_ranges(event):
            self[key, window_range] = op(self[key, window_range], value)

    def _set_windowed(self, key: Any, value: Any,
                      event: EventT = None) -> None:
        for window_range in self._window_ranges(event):
            self[key, window_range] = value

    def _del_windowed(self, key: Any, event: EventT = None) -> None:
        for window_range in self._window_ranges(event):
            del self[key, window_range]

    def _window_ranges(self, event: EventT = None) -> Iterator[WindowRange]:
        timestamp = self._get_timestamp(event)
        for window_range in self.window.ranges(timestamp):
            yield window_range

    def _windowed_current(self, key: Any, event: EventT = None) -> Any:
        return self[key, self.window.current(self._get_timestamp(event))]

    def _windowed_delta(self, key: Any, d: Seconds,
                        event: EventT = None) -> Any:
        return self[key, self.window.delta(self._get_timestamp(event), d)]

    def _get_timestamp(self, event: EventT = None) -> float:
        return (event or current_event()).message.timestamp

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self.name}@{self._store}'

    @property
    def changelog_topic(self) -> TopicT:
        if self._changelog_topic is None:
            self._changelog_topic = self._new_changelog_topic(compacting=True)
        return self._changelog_topic

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        self._changelog_topic = topic


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
