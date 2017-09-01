"""Tables (changelog stream)."""
import abc
import asyncio
import json
import operator
from collections import defaultdict
from datetime import datetime
from heapq import heappop, heappush
from typing import (
    Any, AsyncIterable, Callable, Iterable, Iterator, List, Mapping,
    MutableMapping, MutableSet, Optional, Sequence, Set as _Set, cast,
)
from . import stores
from . import windows
from .streams import current_event
from .streams import joins
from .types import (
    AppT, EventT, FieldDescriptorT, FutureMessage, JoinT, PendingMessage,
    RecordMetadata, TopicPartition, TopicT,
)
from .types.models import ModelArg
from .types.stores import StoreT
from .types.streams import JoinableT, StreamT
from .types.tables import (
    ChangelogReaderT, CheckpointManagerT, CollectionT, CollectionTps, SetT,
    TableManagerT, TableT, WindowSetT, WindowWrapperT,
)
from .types.topics import ChannelT
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

CHANGELOG_SEEKING = 'SEEKING'
CHANGELOG_STARTING = 'STARTING'
CHANGELOG_READING = 'READING'
TABLE_CLEANING = 'CLEANING'
TABLEMAN_UPDATE = 'UPDATE'
TABLEMAN_START_STANDBYS = 'START_STANDBYS'
TABLEMAN_STOP_STANDBYS = 'STOP_STANDBYS'
TABLEMAN_RECOVER = 'RECOVER'
TABLEMAN_PARTITIONS_ASSIGNED = 'PARTITIONS_ASSIGNED'

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

    def persisted_offset(self, tp: TopicPartition) -> Optional[int]:
        return self.data.persisted_offset(tp)

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

    def _on_changelog_sent(self, fut: FutureMessage) -> None:
        res: RecordMetadata = fut.result()
        self.app.checkpoints.set_offset(res.topic_partition, res.offset)

    @Service.task
    @Service.transitions_to(TABLE_CLEANING)
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
            acks=False,
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

    def apply_changelog_batch(self, batch: Iterable[EventT]) -> None:
        self.data.apply_changelog_batch(
            batch,
            to_key=self._to_key,
            to_value=self._to_value,
        )

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

    _highwaters: MutableMapping[TopicPartition, int] = None

    def __init__(self, table: CollectionT,
                 app: AppT,
                 tps: Iterable[TopicPartition],
                 offsets: MutableMapping[TopicPartition, int] = None,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.app = app
        self.tps = tps
        offsets = {} if offsets is None else offsets
        self.offsets = {tp: offsets.get(tp, -1) for tp in self.tps}

    def update_tps(self, tps: Iterable[TopicPartition],
                   tp_offsets: MutableMapping[TopicPartition, int]) -> None:
        self.tps = tps
        self.offsets = {tp: tp_offsets.get(tp, -1) for tp in self.tps}

    async def _build_highwaters(self) -> None:
        consumer = self.app.consumer
        tps = self.tps
        await consumer.seek_to_latest(*tps)
        # FIXME the -1 here is because of the way we commit offsets
        self._highwaters = {
            tp: await consumer.position(tp) - 1
            for tp in tps
        }

    async def _should_stop_reading(self) -> bool:
        return self._highwaters == self.offsets

    @Service.transitions_to(CHANGELOG_SEEKING)
    async def _seek_tps(self) -> None:
        consumer = self.app.consumer
        tps = self.tps
        for tp in tps:
            offset = max(self.offsets[tp], 0)
            self.log.info(f'Seeking {tp} to offset: {offset}')
            await consumer.seek(tp, offset)
            assert await consumer.position(tp) == offset

    async def _should_start_reading(self) -> bool:
        await self._build_highwaters()
        return self._highwaters != self.offsets

    @Service.task
    @Service.transitions_to(CHANGELOG_STARTING)
    async def _read(self) -> None:
        table = self.table
        consumer = self.app.consumer
        await consumer.pause_partitions(self.tps)
        if not await self._should_start_reading():
            self.log.info('Not reading')
            self.set_shutdown()
            return
        await self._seek_tps()
        await consumer.resume_partitions(self.tps)
        self.log.info('Started reading')
        buf: List[EventT] = []
        self.diag.set_flag(CHANGELOG_READING)
        try:
            async for event in self._read_changelog():
                buf.append(event)
                if not len(buf) % 1000:
                    table.apply_changelog_batch(buf)
                    buf.clear()
                if await self._should_stop_reading():
                    break
        finally:
            self.diag.unset_flag(CHANGELOG_READING)
            if buf:
                table.apply_changelog_batch(buf)
                buf.clear()
            await consumer.pause_partitions(self.tps)
            self.set_shutdown()

    async def _read_changelog(self) -> AsyncIterable[EventT]:
        offsets = self.offsets
        table = self.table
        channel = cast(ChannelT, aiter(table.changelog_topic))

        async for event in channel:
            message = event.message
            tp = message.tp
            offset = message.offset
            seen_offset = offsets.get(tp, -1)
            if offset > seen_offset:
                offsets[tp] = offset
                yield event


class StandbyReader(ChangelogReader):

    async def _should_start_reading(self) -> bool:
        return True

    async def _should_stop_reading(self) -> bool:
        return self.should_stop


class TableManager(Service, TableManagerT, FastUserDict):
    logger = logger

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    _table_offsets: MutableMapping[TopicPartition, int]
    _standbys: MutableMapping[CollectionT, ChangelogReaderT]
    _changelog_readers: MutableMapping[CollectionT, ChangelogReaderT]
    _recovery_started: asyncio.Event
    _recovery_completed: asyncio.Event

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.data = {}
        self._channels = {}
        self._changelogs = {}
        self._table_offsets = {}
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

    @Service.transitions_to(TABLEMAN_UPDATE)
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

    def _sync_persisted_offsets(self, table: CollectionT,
                                tps: Iterable[TopicPartition]) -> None:
        for tp in tps:
            persisted_offset = table.persisted_offset(tp)
            if persisted_offset is not None:
                curr_offset = self._table_offsets.get(tp, -1)
                self._table_offsets[tp] = max(curr_offset, persisted_offset)

    def _sync_offsets(self, reader: ChangelogReaderT) -> None:
        self.log.info(f'Syncing offsets {reader.offsets}')
        self._table_offsets.update(reader.offsets)

    @Service.transitions_to(TABLEMAN_STOP_STANDBYS)
    async def _stop_standbys(self) -> None:
        for _, standby in self._standbys.items():
            self.log.info(f'Stopping standby for tps: {standby.tps}')
            await standby.stop()
            self._sync_offsets(standby)
        self._standbys = {}

    def _group_table_tps(self, tps: Iterable[TopicPartition]) -> CollectionTps:
        table_tps: CollectionTps = defaultdict(list)
        for tp in tps:
            if self._is_changelog_tp(tp):
                table_tps[self._changelogs[tp.topic]].append(tp)
        return table_tps

    @Service.transitions_to(TABLEMAN_START_STANDBYS)
    async def _start_standbys(self,
                              tps: Iterable[TopicPartition]) -> None:
        assert not self._standbys
        table_stanby_tps = self._group_table_tps(tps)
        offsets = self._table_offsets
        for table, tps in table_stanby_tps.items():
            self.log.info(f'Starting standbys for tps: {tps}')
            self._sync_persisted_offsets(table, tps)
            tp_offsets = {tp: offsets[tp] for tp in tps if tp in offsets}
            standby = StandbyReader(
                table, self.app, tps, tp_offsets,
                loop=self.loop,
                beacon=self.beacon,
            )
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

    @Service.transitions_to(TABLEMAN_RECOVER)
    async def _recover_changelogs(self, tps: Iterable[TopicPartition]) -> None:
        self.log.info('Recovering from changelog topics...')
        table_recoverers: List[ChangelogReaderT] = [
            self._create_recoverer(table, tps)
            for table in self.values()
        ]
        await self.join_services(table_recoverers)
        for recoverer in table_recoverers:
            self._sync_offsets(recoverer)
        self.log.info('Done recovering from changelog topics')

    def _create_recoverer(self,
                          table: CollectionT,
                          tps: Iterable[TopicPartition]) -> ChangelogReaderT:
        table = cast(Table, table)
        offsets = self._table_offsets
        table_tps = {tp for tp in tps
                     if tp.topic == table._changelog_topic_name()}
        self._sync_persisted_offsets(table, table_tps)
        tp_offsets = {tp: offsets[tp] for tp in table_tps if tp in offsets}
        return ChangelogReader(
            table, self.app, table_tps, tp_offsets,
            loop=self.loop,
            beacon=self.beacon,
        )

    @Service.transitions_to(TABLEMAN_PARTITIONS_ASSIGNED)
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
        await self._recover_changelogs(assigned_tps)
        await self.app.consumer.resume_partitions({
            tp for tp in assigned
            if not self._is_changelog_tp(tp)
        })
        await self._start_standbys(standby_tps)
        self.log.info('New assignments handled')
        await self._on_recovery_completed()


class CheckpointManager(CheckpointManagerT, Service):
    _offsets: MutableMapping[TopicPartition, int]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self._offsets = {}
        Service.__init__(self, **kwargs)

    async def on_start(self) -> None:
        try:
            with open(self.app.checkpoint_path, 'r') as fh:
                data = json.load(fh)
            self._offsets.update((
                (TopicPartition(*k.split('\0')), int(v))
                for k, v in data.items()
            ))
        except FileNotFoundError:
            pass

    async def on_stop(self) -> None:
        with open(self.app.checkpoint_path, 'w') as fh:
            json.dump({
                f'{tp.topic}\0{tp.partition}': v
                for tp, v in self._offsets.items()
            }, fh)

    def get_offset(self, tp: TopicPartition) -> Optional[int]:
        return self._offsets.get(tp)

    def set_offset(self, tp: TopicPartition, offset: int) -> None:
        self._offsets[tp] = offset
