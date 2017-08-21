"""Tables (changelog stream)."""
import abc
import asyncio
import errno
import operator
import shelve
from collections import defaultdict
from datetime import datetime
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
    AppT, EventT, FieldDescriptorT, FutureMessage, JoinT, K, PendingMessage,
    RecordMetadata, TopicPartition, TopicT, V,
)
from .types.models import ModelArg
from .types.stores import StoreT
from .types.streams import JoinableT, StreamT
from .types.tables import (
    ChangelogReaderT, CollectionT, CollectionTps, SetT,
    TableManagerT, TableT, WindowSetT, WindowWrapperT,
)
from .types.topics import ChannelT
from .types.windows import WindowRange, WindowT
from .utils.aiter import aiter
from .utils.collections import FastUserDict, ManagedUserDict, ManagedUserSet
from .utils.logging import get_logger
from .utils.objects import cached_property
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
__flake8_PendingMessage_is_used: PendingMessage  # XXX flake8 bug
__flake8_RecordMetadata_is_used: RecordMetadata  # XXX flake8 bug
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
             value_serializer='json',
             callback=self._on_changelog_sent)

    async def _on_changelog_sent(self, fut: FutureMessage) -> None:
        tables = cast(TableManager, self.app.tables)
        response: RecordMetadata = fut.result()
        message: PendingMessage = fut.message
        if tables._diskcache:
            await tables._write_cache(
                response.topic_partition, response.offset,
                cast(bytes, message.key), cast(bytes, message.value))

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

    def _windowed_now(self, key: Any) -> Any:
        now = datetime.utcnow().timestamp()
        return self._get_key((key, self.window.current(now)))

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

    def apply_changelog_kv(self, k: K, v: V) -> None:
        self.data[self._to_key(k)] = self._to_value(v)

    def _to_key(self, k: Any) -> Any:
        if isinstance(k, list):
            # Lists are not hashable, and windowed-keys are json
            # serialized into a list.
            return tuple(tuple(v) if isinstance(v, list) else v for v in k)
        return k

    def _to_value(self, v: Any) -> Any:
        return v


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


class ChangelogReader(Service, ChangelogReaderT):

    wait_for_shutdown = True
    shutdown_timeout = None

    _started_reading: asyncio.Event

    def __init__(self, table: CollectionT,
                 app: AppT,
                 tps: Iterable[TopicPartition],
                 offsets: MutableMapping[TopicPartition, int] = {},
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.app = app
        self.tps = tps
        self.offsets = {tp: offsets.get(tp, 0) for tp in self.tps}
        self._started_reading = asyncio.Event(loop=self.loop)

    def update_tps(self, tps: Iterable[TopicPartition],
                   tp_offsets: MutableMapping[TopicPartition, int]) -> None:
        self.tps = tps
        self.offsets = {tp: tp_offsets.get(tp, 0) for tp in self.tps}

    @cached_property
    def _highwaters(self) -> MutableMapping[TopicPartition, int]:
        consumer = self.app.consumer
        # FIXME the -1 is due to how we deal with offsets currently
        highwaters = {
            tp: consumer.highwater(tp) - 1
            for tp in self.tps
        }
        return highwaters

    async def _should_stop_reading(self) -> bool:
        return self._highwaters == self.offsets

    async def _seek_tps(self) -> None:
        consumer = self.app.consumer
        tps = self.tps
        for tp in tps:
            print('Seeking', tp, 'to offset:', self.offsets[tp])
            await consumer.seek(tp, self.offsets[tp])

    @Service.task
    async def _read(self) -> None:
        table = self.table
        consumer = self.app.consumer
        await self._seek_tps()
        await consumer.resume_partitions(self.tps)
        async for k, v in self._read_changelog():
            table.apply_changelog_kv(k, v)
            # print(table._changelog_topic_name())
            if await self._should_stop_reading():
                await consumer.pause_partitions(self.tps)
                self.set_shutdown()
                break

    async def _read_changelog(self) -> AsyncIterable[Tuple[K, V]]:
        offsets = self.offsets
        table = self.table
        channel = cast(ChannelT, aiter(table.changelog_topic))

        async for event in channel:
            message = event.message
            tp = message.tp
            offset = message.offset
            seen_offset = offsets.get(tp)
            if seen_offset is None or offset > seen_offset:
                offsets[tp] = offset
                yield event.key, event.value


class StandbyReader(ChangelogReader):

    async def _should_stop_reading(self) -> bool:
        return self.should_stop


class TableManager(Service, TableManagerT, FastUserDict):
    logger = logger

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    _table_offsets: MutableMapping[TopicPartition, int]
    _diskcache: MutableMapping[TopicPartition, shelve.Shelf]
    _standbys: MutableMapping[CollectionT, ChangelogReaderT]
    _changelog_readers: MutableMapping[CollectionT, ChangelogReaderT]
    _recovery_started: asyncio.Event
    _recovery_completed: asyncio.Event

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.cache_path = self.app.table_cache_path
        self.data = {}
        self._channels = {}
        self._changelogs = {}
        self._table_offsets = {}
        self._diskcache = {}
        self._standbys = {}
        self._changelog_readers = {}
        self._recovery_started = asyncio.Event(loop=self.loop)
        self._recovery_completed = asyncio.Event(loop=self.loop)

    @property
    def changelog_topics(self) -> _Set[str]:
        return set(self._changelogs.keys())

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
            return c
        except Exception as exc:
            if getattr(exc, 'errno', None) == errno.EAGAIN:
                raise RuntimeError(f'Cache already in use: {exc!r}')
            raise

    def _get_cache_path_for(self, tp: TopicPartition) -> Path:
        return self.cache_path / f'{tp.topic}-{tp.partition}-cache'

    def _cleanup_cache(self) -> None:
        for shelf in self._diskcache.values():
            shelf.sync()
            shelf.close()
        self._diskcache.clear()

    def _prune_cache(self, cache: shelve.Shelf, new_offset: int) -> None:
        current_offset = cache.get('offset_left')
        if current_offset is not None:
            while current_offset < new_offset:
                offset, current_offset = current_offset, current_offset + 1
                try:
                    del cache[str(offset)]
                except KeyError:
                    pass
                else:
                    cache['offset_left'] = current_offset

    async def _write_cache(
        self,
        tp: TopicPartition,
        offset: int,
        key: bytes,
        value: bytes) -> None:
        cache = self._get_cache_for(tp)
        offset_left = cache.get('offset_left')
        offset_right = cache.get('offset_right')
        cache.update({
            'offset_left': offset if offset_left is None else offset_left,
            'offset_right': max(offset, offset_right or 0),
            str(offset): {
                'key': key,
                'value': value,
            },
        })

    async def _update_channels(self) -> None:
        for table in self.values():
            if table not in self._channels:
                self._channels[table] = cast(ChannelT, aiter(
                    table.changelog_topic))
        self._changelogs.update({
            table.changelog_topic.topics[0]: table
            for table in self.values()
        })
        await self.app.consumer.pause_partitions({
            tp for tp in self.app.consumer.assignment()
            if tp.topic in self._changelogs
        })

    def _sync_offsets(self, reader: ChangelogReaderT) -> None:
        print('Syncing offsets', reader.offsets)
        self._table_offsets.update(reader.offsets)

    async def _stop_standbys(self) -> None:
        for coll, standby in self._standbys.items():
            print('Stopping standby for tps:', standby.tps)
            await standby.stop()
            self._sync_offsets(standby)

    def _group_table_tps(self, tps: Iterable[TopicPartition]) -> CollectionTps:
        table_tps: CollectionTps = defaultdict(list)
        for tp in tps:
            if self._is_changelog_tp(tp):
                table_tps[self._changelogs[tp.topic]].append(tp)
        return table_tps

    async def _start_standbys(self,
                              tps: Iterable[TopicPartition]) -> None:
        table_stanby_tps = self._group_table_tps(tps)
        offsets = self._table_offsets
        for table, tps in table_stanby_tps.items():
            print('Starting standbys for tps:', tps)
            tp_offsets = {tp: offsets[tp] for tp in tps if tp in offsets}
            if table in self._standbys:
                standby = self._standbys[table]
                standby.update_tps(tps, tp_offsets)
                await standby.start()
            else:
                standby = StandbyReader(table, self.app, tps, tp_offsets)
                self._standbys[table] = standby
                await standby.start()

    def _is_changelog_tp(self, tp: TopicPartition) -> bool:
        return tp.topic in self.changelog_topics

    async def _on_recovery_started(self) -> None:
        self._recovery_started.set()
        await self._update_channels()

    async def _on_recovery_completed(self) -> None:
        for table in self.values():
            await table.maybe_start()
        self._recovery_completed.set()

    async def on_start(self) -> None:
        await self.sleep(1.0)
        await self._update_channels()

    async def on_stop(self) -> None:
        if self._recovery_completed.is_set():
            for table in self.values():
                await table.stop()

    async def _recover_changelogs(self, tps: Iterable[TopicPartition]) -> None:
        table_recoverers: List[ChangelogReader] = []
        offsets = self._table_offsets
        for table in self.values():
            table_tps = {tp for tp in tps
                         if tp.topic == table._changelog_topic_name()}
            tp_offsets = {tp: offsets[tp] for tp in table_tps if tp in offsets}
            table_recoverers.append(
                ChangelogReader(table, self.app, table_tps, tp_offsets))
        [await recoverer.start() for recoverer in table_recoverers]
        await self.sleep(5)
        for recoverer in table_recoverers:
            await recoverer.stop()
            self._sync_offsets(recoverer)
            self.log.info('Done recovering')

    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        standby_tps = self.app.assignor.assigned_standbys()
        assigned_tps = self.app.assignor.assigned_actives()
        assert set(assigned_tps).issubset(set(assigned))
        # Wait for TopicManager to finish any new subscriptions
        await self.app.channels.wait_for_subscriptions()
        self.log.info('New assignments found')
        await self._on_recovery_started()
        self.log.info('Attempting to stop standbys')
        await self._stop_standbys()
        await self.app.consumer.pause_partitions(assigned)
        await self._recover_changelogs(assigned_tps)
        await self.app.consumer.resume_partitions({
            tp for tp in assigned
            if not self._is_changelog_tp(tp)
        })
        print('Attempting to start standbys:', standby_tps)
        await self._start_standbys(standby_tps)
        self.log.info('New assignments handled')
        await self._on_recovery_completed()
