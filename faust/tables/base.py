"""Base class Collection for Table and future data structures."""
import abc
import time
from contextlib import suppress
from collections import defaultdict
from datetime import datetime
from heapq import heappop, heappush
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Set,
    Union,
    cast,
    no_type_check,
)

from mode import Seconds, Service
from yarl import URL

from faust import stores
from faust import joins
from faust.events import Event
from faust.exceptions import PartitionsMismatch
from faust.streams import current_event
from faust.types import (
    AppT,
    CodecArg,
    EventT,
    FieldDescriptorT,
    FutureMessage,
    JoinT,
    RecordMetadata,
    TP,
    TopicT,
)
from faust.types.models import ModelArg, ModelT
from faust.types.stores import StoreT
from faust.types.streams import JoinableT, StreamT
from faust.types.tables import (
    ChangelogEventCallback,
    CollectionT,
    RecoverCallback,
    RelativeHandler,
)
from faust.types.windows import WindowRange, WindowT

__all__ = ['Collection']

TABLE_CLEANING = 'CLEANING'

E_SOURCE_PARTITIONS_MISMATCH = '''\
The source topic {source_topic!r} for table {table_name!r}
has {source_n} partitions, but the changelog
topic {change_topic!r} has {change_n} partitions.

Please make sure the topics have the same number of partitions
by configuring Kafka correctly.
'''


class Collection(Service, CollectionT):
    """Base class for changelog-backed data structures stored in Kafka."""

    _store: Optional[URL]
    _changelog_topic: Optional[TopicT]
    _partition_timestamp_keys: MutableMapping[tuple, MutableSet]
    _partition_timestamps: MutableMapping[int, List[float]]
    _partition_latest_timestamp: MutableMapping[int, float]
    _recover_callbacks: MutableSet[RecoverCallback]
    _data: Optional[StoreT] = None
    _changelog_compacting: Optional[bool] = True
    _changelog_deleting: Optional[bool] = None

    @abc.abstractmethod
    def _has_key(self, key: Any) -> bool:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _get_key(self, key: Any) -> Any:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _set_key(self, key: Any, value: Any) -> None:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _del_key(self, key: Any) -> None:  # pragma: no cover
        ...

    def __init__(self,
                 app: AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: Union[str, URL] = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 help: str = None,
                 on_recover: RecoverCallback = None,
                 on_changelog_event: ChangelogEventCallback = None,
                 recovery_buffer_size: int = 1000,
                 standby_buffer_size: int = None,
                 extra_topic_configs: Mapping[str, Any] = None,
                 **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self.name = cast(str, name)  # set lazily so CAN BE NONE!
        self.default = default
        self._store = URL(store) if store else None
        self.key_type = key_type
        self.value_type = value_type
        self.partitions = partitions
        self.window = window
        self._changelog_topic = changelog_topic
        self.extra_topic_configs = extra_topic_configs or {}
        self.help = help or ''
        self._on_changelog_event = on_changelog_event
        self.recovery_buffer_size = recovery_buffer_size
        self.standby_buffer_size = standby_buffer_size or recovery_buffer_size
        assert self.recovery_buffer_size > 0 and self.standby_buffer_size > 0

        # Setting Serializers from key_type and value_type
        # Possible values json and raw
        # Fallback to json
        self.key_serializer = self._serializer_from_type(self.key_type)
        self.value_serializer = self._serializer_from_type(self.value_type)

        # Table key expiration
        self._partition_timestamp_keys = defaultdict(set)
        self._partition_timestamps = defaultdict(list)
        self._partition_latest_timestamp = defaultdict(int)

        self._recover_callbacks = set()
        if on_recover:
            self.on_recover(on_recover)

        # Aliases
        self._sensor_on_get = self.app.sensors.on_table_get
        self._sensor_on_set = self.app.sensors.on_table_set
        self._sensor_on_del = self.app.sensors.on_table_del

    def _serializer_from_type(
            self, typ: Optional[ModelArg]) -> Optional[CodecArg]:
        if typ is bytes:
            return 'raw'
        serializer = None
        with suppress(AttributeError):
            serializer = typ._options.serializer  # type: ignore
        return serializer or 'json'

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app.tables mapping.
        return object.__hash__(self)

    def _new_store(self) -> StoreT:
        return self._new_store_by_url(self._store or self.app.conf.store)

    def _new_store_by_url(self, url: Union[str, URL]) -> StoreT:
        return cast(StoreT, stores.by_url(url)(
            url, self.app, self,
            table_name=self.name,
            key_type=self.key_type,
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
            value_type=self.value_type,
            loop=self.loop,
        ))

    @property  # type: ignore
    @no_type_check  # XXX https://github.com/python/mypy/issues/4125
    def data(self) -> StoreT:
        if self._data is None:
            self._data = self._new_store()
        return self._data

    async def on_start(self) -> None:
        await self.add_runtime_dependency(self.data)
        await self.changelog_topic.maybe_declare()

    def on_recover(self, fun: RecoverCallback) -> RecoverCallback:
        """Add function as callback to be called on table recovery."""
        assert fun not in self._recover_callbacks
        self._recover_callbacks.add(fun)
        return fun

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

    def persisted_offset(self, tp: TP) -> Optional[int]:
        return self.data.persisted_offset(tp)

    async def need_active_standby_for(self, tp: TP) -> bool:
        return await self.data.need_active_standby_for(tp)

    def reset_state(self) -> None:
        self.data.reset_state()

    def _send_changelog(self,
                        event: Optional[EventT],
                        key: Any,
                        value: Any,
                        key_serializer: CodecArg = None,
                        value_serializer: CodecArg = None) -> None:
        if event is None:
            raise RuntimeError('Cannot modify table outside of agent/stream.')
        self._verify_source_topic_partitions(event)
        if key_serializer is None:
            key_serializer = self.key_serializer
        if value_serializer is None:
            value_serializer = self.value_serializer
        cast(Event, event)._attach(
            self.changelog_topic,
            key,
            value,
            partition=event.message.partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=self._on_changelog_sent,
        )

    def _verify_source_topic_partitions(self, event: EventT) -> None:
        source_topic = event.message.topic
        change_topic = self.changelog_topic.get_topic_name()
        source_n = self.app.consumer.topic_partitions(source_topic)
        if source_n is not None:
            change_n = self.app.consumer.topic_partitions(change_topic)
            if change_n is not None:
                if source_n != change_n:
                    raise PartitionsMismatch(
                        E_SOURCE_PARTITIONS_MISMATCH.format(
                            source_topic=source_topic,
                            table_name=self.name,
                            source_n=source_n,
                            change_topic=change_topic,
                            change_n=change_n,
                        ),
                    )

    def _on_changelog_sent(self, fut: FutureMessage) -> None:
        # This is what keeps the offset in RocksDB so that at startup
        # we know what offsets we already have data for in the database.
        #
        # Kafka Streams has a global ".checkpoint" file, but we store
        # it in each individual RocksDB database file.
        # Every partition in the table will have its own database file,
        #  this is required as partitions can easily move from and to
        #  machine as nodes die and recover.
        res: RecordMetadata = fut.result()
        if self.app.in_transaction:
            # for exactly-once semantics we only write the
            # persisted offset to RocksDB on disk when that partition
            # is committed.
            self.app.tables.persist_offset_on_commit(
                self.data, res.topic_partition, res.offset)
        else:
            # for normal processing (at-least-once) we just write
            # the persisted offset immediately.
            self.data.set_persisted_offset(res.topic_partition, res.offset)

    @Service.task
    @Service.transitions_to(TABLE_CLEANING)
    async def _clean_data(self) -> None:
        if self._should_expire_keys():
            while not self.should_stop:
                self._del_old_keys()
                await self.sleep(self.app.conf.table_cleanup_interval)

    def _del_old_keys(self) -> None:
        window = cast(WindowT, self.window)
        assert window
        for partition, timestamps in self._partition_timestamps.items():
            while timestamps and window.stale(
                    timestamps[0],
                    self._partition_latest_timestamp[partition]):
                timestamp = heappop(timestamps)
                keys_to_remove = self._partition_timestamp_keys.pop(
                    (partition, timestamp), None)
                if keys_to_remove:
                    for key in keys_to_remove:
                        self.data.pop(key, None)

    def _should_expire_keys(self) -> bool:
        window = self.window
        return not (window is None or window.expires is None)

    def _maybe_set_key_ttl(self, key: Any, partition: int) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        _, range_end = window_range
        heappush(self._partition_timestamps[partition], range_end)
        self._partition_latest_timestamp[partition] = max(
            self._partition_latest_timestamp[partition], range_end)
        self._partition_timestamp_keys[(partition, range_end)].add(key)

    def _maybe_del_key_ttl(self, key: Any, partition: int) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        ts_keys = self._partition_timestamp_keys.get(
            (partition, window_range[1]))
        if ts_keys is not None:
            ts_keys.discard(key)

    def _changelog_topic_name(self) -> str:
        return f'{self.app.conf.id}-{self.name}-changelog'

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

    def contribute_to_stream(self, active: StreamT) -> None:
        # TODO  See Stream.contribute_to_stream()
        # Should probably connect to Table changelog.
        ...

    async def remove_from_stream(self, stream: StreamT) -> None:
        # TODO See Stream.remove_from_stream()
        # Should stop any services started to support joining this table
        # with one or more streams.
        ...

    def _new_changelog_topic(self,
                             *,
                             retention: Seconds = None,
                             compacting: bool = None,
                             deleting: bool = None) -> TopicT:
        if compacting is None:
            compacting = self._changelog_compacting
        if deleting is None:
            deleting = self._changelog_deleting
        if retention is None and self.window:
            retention = self.window.expires
        return self.app.topic(
            self._changelog_topic_name(),
            key_type=self.key_type,
            value_type=self.value_type,
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
            partitions=self.partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            acks=False,
            internal=True,
            config=self.extra_topic_configs,
            # use large buffer size as we do not commit attached messages
            # when reading changelog streams.
            maxsize=131_072,
            allow_empty=True,
        )

    def __copy__(self) -> Any:
        return self.clone()

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def _apply_window_op(self, op: Callable[[Any, Any], Any], key: Any,
                         value: Any, timestamp: float) -> None:
        get_ = self._get_key
        set_ = self._set_key
        for window_range in self._window_ranges(timestamp):
            set_((key, window_range), op(get_((key, window_range)), value))

    def _set_windowed(self, key: Any, value: Any, timestamp: float) -> None:
        for window_range in self._window_ranges(timestamp):
            self._set_key((key, window_range), value)

    def _del_windowed(self, key: Any, timestamp: float) -> None:
        for window_range in self._window_ranges(timestamp):
            self._del_key((key, window_range))

    def _window_ranges(self, timestamp: float) -> Iterator[WindowRange]:
        window = cast(WindowT, self.window)
        for window_range in window.ranges(timestamp):
            yield window_range

    def _relative_now(self, event: EventT = None) -> float:
        # get current timestamp
        event = event if event is not None else current_event()
        if event is None:
            return time.time()
        return self._partition_latest_timestamp[event.message.partition]

    def _relative_event(self, event: EventT = None) -> float:
        event = event if event is not None else current_event()
        # get event timestamp
        if event is None:
            raise RuntimeError('Operation outside of stream iteration')
        return event.message.timestamp

    def _relative_field(self, field: FieldDescriptorT) -> RelativeHandler:
        def to_value(event: EventT = None) -> Union[float, datetime]:
            if event is None:
                raise RuntimeError('Operation outside of stream iteration')
            return field.getattr(cast(ModelT, event.value))

        return to_value

    def _relative_timestamp(self, timestamp: float) -> RelativeHandler:
        def handler(event: EventT = None) -> Union[float, datetime]:
            return timestamp

        return handler

    def _windowed_now(self, key: Any) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key((key, window.earliest(self._relative_now())))

    def _windowed_timestamp(self, key: Any, timestamp: float) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key((key, window.current(timestamp)))

    def _windowed_contains(self, key: Any, timestamp: float) -> bool:
        window = cast(WindowT, self.window)
        return self._has_key((key, window.current(timestamp)))

    def _windowed_delta(self, key: Any, d: Seconds,
                        event: EventT = None) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key(
            (key,
             window.delta(self._relative_event(event), d)),
        )

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        await self.data.on_rebalance(self, assigned, revoked, newly_assigned)

    async def on_recovery_completed(self,
                                    active_tps: Set[TP],
                                    standby_tps: Set[TP]) -> None:
        await self.data.on_recovery_completed(active_tps, standby_tps)
        await self.call_recover_callbacks()

    async def call_recover_callbacks(self) -> None:
        for fun in self._recover_callbacks:
            await fun()

    async def on_changelog_event(self, event: EventT) -> None:
        if self._on_changelog_event:
            await self._on_changelog_event(event)

    @property
    def label(self) -> str:
        return f'{self.shortlabel}@{self._store}'

    @property
    def shortlabel(self) -> str:
        return f'{type(self).__name__}: {self.name}'

    @property
    def changelog_topic(self) -> TopicT:
        if self._changelog_topic is None:
            self._changelog_topic = self._new_changelog_topic()
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

    def _human_channel(self) -> str:
        return f'{type(self).__name__}: {self.name}'

    def _repr_info(self) -> str:
        return self.name
