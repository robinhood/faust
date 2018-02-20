"""Base class Collection for Tables/Sets/etc."""
import abc
from collections import defaultdict
from heapq import heappop, heappush
from typing import (
    Any, Callable, Iterable, Iterator, List, Mapping,
    MutableMapping, MutableSet, Optional, Set, Union, cast, no_type_check,
)

from mode import Seconds, Service
from yarl import URL

from .. import stores
from ..channels import Event
from ..streams import current_event
from ..streams import joins
from ..types import (
    AppT, EventT, FieldDescriptorT, FutureMessage, JoinT,
    RecordMetadata, TP, TopicT,
)
from ..types.models import ModelArg, ModelT
from ..types.stores import StoreT
from ..types.streams import JoinableT, StreamT
from ..types.tables import (
    ChangelogEventCallback, CollectionT, RecoverCallback, RelativeHandler,
)
from ..types.windows import WindowRange, WindowT

__all__ = ['Collection']

TABLE_CLEANING = 'CLEANING'


class Collection(Service, CollectionT):
    """Base class for changelog-backed data structures stored in Kafka."""

    _store: URL
    _changelog_topic: TopicT
    _timestamp_keys: MutableMapping[float, MutableSet]
    _timestamps: List[float]
    _latest_timestamp: float
    _recover_callbacks: MutableSet[RecoverCallback]
    _data: StoreT = None

    @abc.abstractmethod
    def _has_key(self, key: Any) -> bool:
        ...

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
        self.name = name
        self.default = default
        self._store = URL(store) if store else None
        self.key_type = key_type
        self.value_type = value_type
        self.partitions = partitions
        self.window = window
        self.changelog_topic = changelog_topic
        self.extra_topic_configs = extra_topic_configs or {}
        self.help = help
        self._on_changelog_event = on_changelog_event
        self.recovery_buffer_size = recovery_buffer_size
        self.standby_buffer_size = standby_buffer_size or recovery_buffer_size
        assert self.recovery_buffer_size > 0 and self.standby_buffer_size > 0

        # Table key expiration
        self._timestamp_keys = defaultdict(set)
        self._timestamps = []
        self._latest_timestamp = 0

        self._recover_callbacks = set()
        if on_recover:
            self.on_recover(on_recover)

        # Aliases
        self._sensor_on_get = self.app.sensors.on_table_get
        self._sensor_on_set = self.app.sensors.on_table_set
        self._sensor_on_del = self.app.sensors.on_table_del

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app.tables mapping.
        return object.__hash__(self)

    def _get_store(self) -> StoreT:
        if self._data is None:
            app = self.app
            if self.StateStore is not None:
                self._data = self.StateStore(
                    url=None, app=app, loop=self.loop)
            else:
                url = self._store or self.app.conf.store
                self._data = stores.by_url(url)(
                    url, app,
                    table_name=self.name,
                    key_type=self.key_type,
                    value_type=self.value_type,
                    loop=self.loop)
            self.add_dependency(self._data)
        return self._data

    @property  # type: ignore
    @no_type_check  # XXX https://github.com/python/mypy/issues/4125
    def data(self) -> StoreT:
        return self._get_store()

    async def on_start(self) -> None:
        await self.changelog_topic.maybe_declare()

    def on_recover(self, fun: RecoverCallback) -> RecoverCallback:
        """Add function as callback to be called on table recovery."""
        assert fun not in self._recover_callbacks
        self._recover_callbacks.add(fun)
        return fun

    async def call_recover_callbacks(self) -> None:
        for fun in self._recover_callbacks:
            await fun()

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

    def _send_changelog(self, key: Any, value: Any) -> None:
        event = current_event()
        if event is None:
            raise RuntimeError('Cannot modify table outside of agent/stream.')
        cast(Event, event)._attach(
            self.changelog_topic, key, value,
            partition=event.message.partition,
            key_serializer='json',
            value_serializer='json',
            callback=self._on_changelog_sent,
        )

    def _on_changelog_sent(self, fut: FutureMessage) -> None:
        res: RecordMetadata = fut.result()
        self.data.set_persisted_offset(res.topic_partition, res.offset)

    @Service.task
    @Service.transitions_to(TABLE_CLEANING)
    async def _clean_data(self) -> None:
        if self._should_expire_keys():
            timestamps = self._timestamps
            window = self.window
            while not self.should_stop:
                while timestamps and window.stale(
                        timestamps[0], self._latest_timestamp):
                    timestamp = heappop(timestamps)
                    for key in self._timestamp_keys[timestamp]:
                        del self.data[key]
                    del self._timestamp_keys[timestamp]
                await self.sleep(self.app.conf.table_cleanup_interval)

    def _should_expire_keys(self) -> bool:
        window = self.window
        return not (window is None or window.expires is None)

    def _maybe_set_key_ttl(self, key: Any) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        heappush(self._timestamps, window_range.end)
        self._latest_timestamp = max(self._latest_timestamp, window_range.end)
        self._timestamp_keys[window_range.end].add(key)

    def _maybe_del_key_ttl(self, key: Any) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        ts_keys = self._timestamp_keys.get(window_range.end)
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

    def _new_changelog_topic(self, *,
                             retention: Seconds = None,
                             compacting: bool = True,
                             deleting: bool = None) -> TopicT:
        return self.app.topic(
            self._changelog_topic_name(),
            key_type=self.key_type,
            value_type=self.value_type,
            key_serializer='json',
            value_serializer='json',
            partitions=self.partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            acks=False,
            internal=True,
            config=self.extra_topic_configs,
        )

    def __copy__(self) -> Any:
        return self.clone()

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def _apply_window_op(self,
                         op: Callable[[Any, Any], Any],
                         key: Any,
                         value: Any,
                         timestamp: float) -> None:
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
        for window_range in self.window.ranges(timestamp):
            yield window_range

    def _relative_now(self, event: EventT = None) -> float:
        # get current timestamp
        return self._latest_timestamp

    def _relative_event(self, event: EventT = None) -> float:
        # get event timestamp
        return event.message.timestamp

    def _relative_field(self, field: FieldDescriptorT) -> RelativeHandler:
        def to_value(event: EventT) -> float:
            return field.getattr(cast(ModelT, event.value))
        return to_value

    def _relative_timestamp(self, timestamp: float) -> RelativeHandler:
        def handler(event: EventT) -> float:
            return timestamp
        return handler

    def _windowed_now(self, key: Any) -> Any:
        return self._windowed_timestamp(key, self._relative_now())

    def _windowed_timestamp(self, key: Any, timestamp: float) -> Any:
        return self._get_key((key, self.window.current(timestamp)))

    def _windowed_contains(self, key: Any, timestamp: float) -> bool:
        return self._has_key((key, self.window.current(timestamp)))

    def _windowed_delta(self, key: Any, d: Seconds,
                        event: EventT = None) -> Any:
        return self._get_key(
            (key, self.window.delta(self._relative_event(event), d)))

    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        await self.data.on_partitions_assigned(self, assigned)

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        await self.data.on_partitions_revoked(self, revoked)

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
