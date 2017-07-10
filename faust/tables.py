"""Tables (changelog stream)."""
import abc
import asyncio
import operator
import shelve
from collections import defaultdict
from heapq import heappop, heappush
from pathlib import Path
from typing import (
    Any, AsyncIterable, Callable, Iterable, Iterator, List, Mapping,
    MutableMapping, MutableSet, Sequence, Set as _Set, Tuple, cast,
)
from . import stores
from . import windows
from .streams import current_event
from .streams import joins
from .types import (
    AppT, EventT, FieldDescriptorT, JoinT, TopicPartition, TopicT,
)
from .types.models import ModelArg
from .types.stores import StoreT
from .types.streams import JoinableT, StreamT
from .types.tables import (
    CollectionT, SetT, TableManagerT, TableT, WindowSetT, WindowWrapperT,
)
from .types.topics import SourceT
from .types.windows import WindowRange, WindowT
from .utils.aiter import aiter
from .utils.collections import FastUserDict, ManagedUserDict, ManagedUserSet
from .utils.logging import get_logger
from .utils.services import Service
from .utils.times import Seconds

__all__ = [
    'Collection',
    'Set',
    'Table',
    'TableManager',
    'WindowSet',
    'WindowWrapper',
]

__flake8_Sequence_is_used: Sequence  # XXX flake8 bug
__flake8_Set_is_used: _Set

logger = get_logger(__name__)


class Collection(Service, CollectionT):
    logger = logger

    _store: str
    _changelog_topic: TopicT
    _timestamp_keys: MutableMapping[float, MutableSet]
    _timestamps: List[float]

    @abc.abstractmethod
    def _get_key(self, key: Any) -> Any:
        ...

    @abc.abstractmethod
    def _set_key(self, key: Any, value: Any) -> None:
        ...

    @abc.abstractmethod
    def _del_key(self, key: Any) -> None:
        ...

    def __init__(self, app: AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: str = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self.name = name
        self.default = default
        self._store = store
        self.key_type = key_type or 'json'
        self.value_type = value_type or 'json'
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

    async def on_start(self) -> None:
        await self.changelog_topic.maybe_declare()

    def info(self) -> Mapping[str, Any]:
        # Used to recreate object in .clone()
        return {
            'app': self.app,
            'name': self.name,
            'store': self._store,
            'default': self.default,
            'key_type': self.key_type,
            'value_type': self.value_type,
            'changelog_topic': self._changelog_topic,
            'window': self.window,
        }

    def _send_changelog(self, key: Any, value: Any) -> None:
        event = current_event()
        partition: int = None
        if event is not None:
            send = event.attach
            partition = event.message.partition
        else:
            send = self.app.send_soon
        send(self.changelog_topic, key, value,
             partition=partition,
             key_serializer='json',
             value_serializer='json')

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

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def _apply_window_op(self,
                         op: Callable[[Any, Any], Any],
                         key: Any,
                         value: Any,
                         event: EventT = None) -> None:
        get_ = self._get_key
        set_ = self._set_key
        for window_range in self._window_ranges(event):
            set_((key, window_range), op(get_((key, window_range)), value))

    def _set_windowed(self, key: Any, value: Any,
                      event: EventT = None) -> None:
        for window_range in self._window_ranges(event):
            self._set_key((key, window_range), value)

    def _del_windowed(self, key: Any, event: EventT = None) -> None:
        for window_range in self._window_ranges(event):
            self._del_key((key, window_range))

    def _window_ranges(self, event: EventT = None) -> Iterator[WindowRange]:
        timestamp = self._get_timestamp(event)
        for window_range in self.window.ranges(timestamp):
            yield window_range

    def _windowed_current(self, key: Any, event: EventT = None) -> Any:
        return self._get_key(
            (key, self.window.current(self._get_timestamp(event))))

    def _windowed_delta(self, key: Any, d: Seconds,
                        event: EventT = None) -> Any:
        return self._get_key(
            (key, self.window.delta(self._get_timestamp(event), d)))

    def _get_timestamp(self, event: EventT = None) -> float:
        return (event or current_event()).message.timestamp

    @property
    def label(self) -> str:
        return f'{self.shortlabel}@{self._store}'

    @property
    def shortlabel(self) -> str:
        return f'{type(self).__name__}: {self.name}'

    @property
    def changelog_topic(self) -> TopicT:
        if self._changelog_topic is None:
            self._changelog_topic = self._new_changelog_topic(compacting=True)
        return self._changelog_topic

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        self._changelog_topic = topic


class Table(Collection, TableT, ManagedUserDict):

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

    def __missing__(self, key: Any) -> Any:
        if self.default is not None:
            value = self.data[key] = self.default()
            return value
        raise KeyError(key)

    def _get_key(self, key: Any) -> Any:
        return self[key]

    def _set_key(self, key: Any, value: Any) -> None:
        self[key] = value

    def _del_key(self, key: Any) -> None:
        del self[key]

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


class Set(Collection, SetT, ManagedUserSet):

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any) -> None:
        self._send_changelog(key, value=True)
        self._maybe_set_key_ttl(key)
        self._sensor_on_set(self, key, value=True)

    def on_key_del(self, key: Any) -> None:
        self._send_changelog(key, value=None)
        self._maybe_del_key_ttl(key)
        self._sensor_on_del(self, key)


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


class TableManager(Service, TableManagerT, FastUserDict):
    logger = logger

    _sources: MutableMapping[CollectionT, SourceT]
    _changelogs: MutableMapping[str, CollectionT]
    _table_offsets: MutableMapping[TopicPartition, int]
    _diskcache: MutableMapping[TopicPartition, shelve.Shelf]
    _recovery_started: asyncio.Event
    _recovery_completed: asyncio.Event

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.cache_path = self.app.table_cache_path
        self.data = {}
        self._sources = {}
        self._changelogs = {}
        self._table_offsets = {}
        self._diskcache = {}
        self._recovery_started = asyncio.Event(loop=self.loop)
        self._recovery_completed = asyncio.Event(loop=self.loop)

    def __setitem__(self, key: str, value: CollectionT) -> None:
        if self._recovery_started.is_set():
            raise RuntimeError('Too late to add tables at this point')
        super().__setitem__(key, value)

    def _get_cache_for(self, tp: TopicPartition) -> shelve.Shelf:
        try:
            return self._diskcache[tp]
        except KeyError:
            path = self._get_cache_path_for(tp)
            c = self._diskcache[tp] = shelve.open(str(path), writeback=True)
            c.setdefault('offset_left', None)
            c.setdefault('offset_right', None)
            c.setdefault('items', {})
            return c

    def _get_cache_path_for(self, tp: TopicPartition) -> Path:
        return self.cache_path / f'{tp.topic}-{tp.partition}-cache.db'

    def _cleanup_cache(self) -> None:
        for shelf in self._diskcache.values():
            shelf.sync()
            shelf.close()
        self._diskcache.clear()

    async def _update_sources(self) -> None:
        for table in self.values():
            if table not in self._sources:
                self._sources[table] = cast(SourceT, aiter(
                    table.changelog_topic))
        self._changelogs.update({
            table.changelog_topic.topics[0]: table
            for table in self.values()
        })
        await self.app.consumer.pause_partitions({
            tp for tp in self.app.consumer.assignment()
            if tp.topic in self._changelogs
        })

    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        # Wait for TopicManager to finish any new subscriptions
        await self.app.sources.wait_for_subscriptions()
        self.log.info('New assignments found')
        await self._on_recovery_started()
        await self.app.consumer.pause_partitions(assigned)
        # TODO Recover multiple tables at the same time.
        for table in self.values():
            # TODO If standby ready, just swap and continue.
            await self._recover_from_changelog(table, assigned)
        await self.app.consumer.resume_partitions({
            tp for tp in assigned
            if tp.topic not in self._changelogs
        })
        self.log.info('New assignments handled')
        await self._on_recovery_completed()

    async def _recover_from_changelog(
            self,
            table: CollectionT,
            assigned: Iterable[TopicPartition]) -> None:
        consumer = self.app.consumer

        # Get assigned partitions for this tables changelog topic.
        tps: _Set[TopicPartition] = {
            tp for tp in assigned
            if tp.topic in table.changelog_topic.topics
        }
        if tps:
            # Seek partitions to appropriate offsets
            if await self._seek_changelog(tps):
                # at this point we know there are messages in at least
                # one of the topic partitions:

                try:
                    to_remove: _Set[TopicPartition] = set()
                    for tp in tps:
                        cache = self._get_cache_for(tp)
                        loff = cache['offset_left']
                        roff = cache['offset_right']
                        if loff is not None:
                            print(f'READ FROM CACHE: {loff} {roff}')
                            await self.table_update_from_iterable(
                                table,
                                self._read_cache(cache, tp, loff, roff))
                            self._table_offsets[tp] = roff
                            rem_left, rem_right = await self._get_border(tp)
                            # Skip this tp if there are no more messages to
                            # receive
                            if roff == rem_right - 1:
                                print(f'{tp} ALREADY HAD EVERYTHING IN CACHE')
                                to_remove.add(tp)
                            await consumer.seek(tp, roff)
                    for tp in to_remove:
                        tps.discard(tp)

                    if tps:
                        # resume changelog partitions for this table
                        await consumer.resume_partitions(tps)
                        try:
                            await self.table_update_from_iterable(
                                table, self._read_changelog(
                                    table, tps, self._sources[table]))
                        finally:
                            await consumer.pause_partitions(tps)
                finally:
                    # close and sync all cache shelves we have open.
                    self._cleanup_cache()
                self.log.info('Table %r: Recovery completed!', table.name)
            else:
                self.log.info('Table %r: Table empty', table.name)

    async def _seek_changelog(self, tps: Iterable[TopicPartition]) -> bool:
        # Set offset of partition to beginning
        earliest: _Set[TopicPartition] = set()  # tps to seek from earliest
        latest: _Set[TopicPartition] = set()
        consumer = self.app.consumer
        has_positions = False  # set if there are any messages in topics
        for tp in tps:
            try:
                # see if we have read this changelog before
                offset = self._table_offsets[tp]
            except KeyError:
                # if not, add it to partitions we seek to beginning
                earliest.add(tp)
            else:
                latest.add(tp)
                has_positions = True
                # if we have read it, seek to that offset
                await consumer.seek(tp, offset)
        if earliest:
            # find end positions of all partitions
            await consumer.seek_to_latest(*tps)
            border_right = {tp: await consumer.position(tp) for tp in earliest}
            await consumer.seek_to_beginning(*tps)
            border_left = {tp: await consumer.position(tp) for tp in earliest}
            # If the topic is newly created and has no messages
            # the beginning position will be 0, and the latest position will
            # be 0.  If there is a message in the topic the beginning will be
            # 0 and the end will be 1.
            #
            # We cannot look for the number 0 as with log compaction
            # the starting point may have a different value, so
            # we simply assume that if the numbers are different there
            # are messages in the topic.
            has_positions = any(
                border_left[tp] != border_right[tp]
                for tp in earliest
            )
            # at this point the topics are rewound at the beginning.
        return has_positions

    async def _get_border(self, tp: TopicPartition) -> Tuple[int, int]:
        await self.app.consumer.seek_to_latest(tp)
        border_right = await self.app.consumer.position(tp)
        await self.app.consumer.seek_to_beginning(tp)
        border_left = await self.app.consumer.position(tp)
        return border_left, border_right

    async def _read_changelog(
            self,
            table: CollectionT,
            tps: Iterable[TopicPartition],
            source: SourceT) -> AsyncIterable[Tuple[bytes, bytes]]:
        offsets = self._table_offsets
        pending_tps = set(tps)
        self.log.info('Recover %r from changelog', table.name)
        async for event in source:
            message = event.message
            tp = message.tp
            offset = message.offset
            seen_offset = offsets.get(tp)

            if seen_offset is None or offset > seen_offset:
                highwater = self.app.consumer.highwater(tp)
                remaining = highwater - offset
                if not offset % 10_000 and remaining > 1000:
                    self.log.info('Table %r, still waiting for %r values',
                                  table.name, remaining)
                offsets[tp] = offset

                cache = self._get_cache_for(tp)

                # Write entry to cache
                lod = cache.setdefault('offset_left', offset)
                if lod is None:
                    cache['offset_left'] = offset
                cache['offset_right'] = offset
                cache['items'][offset] = {
                    'key': event.message.key,
                    'value': event.message.value,
                }

                # See if we are done reading from this changelog
                if highwater is None or offset >= highwater - 1:
                    # we have read up till highwater, so this partition is
                    # up to date.
                    pending_tps.discard(tp)
                    if not pending_tps:
                        break
                yield event.key, event.value

    async def _read_cache(
            self,
            cache: shelve.Shelf,
            tp: TopicPartition,
            start: int,
            end: int) -> AsyncIterable[Tuple[bytes, bytes]]:
        contents = cache['items']
        offsets = self._table_offsets
        if tp in offsets:
            start = max(offsets[tp], start)
        for i in range(start, end):
            entry = contents[i]
            offsets[tp] = i
            yield entry['key'], entry['value']

    async def table_update_from_iterable(
            self,
            table: CollectionT,
            it: AsyncIterable[Tuple[bytes, bytes]]) -> None:
        buf: MutableMapping[Any, Any] = {}
        to_key, to_value = self._to_key, self._to_value
        async for k, v in it:
            buf[to_key(k)] = to_value(v)
            if len(buf) > 1000:
                cast(Table, table).raw_update(buf)
                buf.clear()
        if buf:
            cast(Table, table).raw_update(buf)
            buf.clear()

    def _to_key(self, k: Any) -> Any:
        if isinstance(k, list):
            # Lists are not hashable, and windowed-keys are json
            # serialized into a list.
            return tuple(tuple(v) if isinstance(v, list) else v for v in k)
        return k

    def _to_value(self, v: Any) -> Any:
        return v

    async def _on_recovery_started(self) -> None:
        self._recovery_started.set()
        await self._update_sources()

    async def _on_recovery_completed(self) -> None:
        for table in self.values():
            await table.maybe_start()
        self._recovery_completed.set()

    async def on_start(self) -> None:
        await self.sleep(1.0)
        await self._update_sources()

    async def on_stop(self) -> None:
        if self._recovery_completed.is_set():
            for table in self.values():
                await table.stop()
        self._cleanup_cache()
