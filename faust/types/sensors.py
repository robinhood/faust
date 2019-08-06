import abc
import typing
from typing import Any, Dict, Iterable, Optional

from mode import ServiceT

from . import web
from .assignor import PartitionAssignorT
from .events import EventT
from .streams import StreamT
from .tables import CollectionT
from .topics import TopicT
from .transports import ConsumerT, ProducerT
from .tuples import Message, PendingMessage, RecordMetadata, TP

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
else:
    class _AppT: ...  # noqa

__all__ = ['SensorInterfaceT', 'SensorT', 'SensorDelegateT']


class SensorInterfaceT(abc.ABC):

    @abc.abstractmethod
    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        ...

    @abc.abstractmethod
    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> Optional[Dict]:
        ...

    @abc.abstractmethod
    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT, state: Dict = None) -> None:
        ...

    @abc.abstractmethod
    def on_topic_buffer_full(self, topic: TopicT) -> None:
        ...

    @abc.abstractmethod
    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        ...

    @abc.abstractmethod
    def on_table_get(self, table: CollectionT, key: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        ...

    @abc.abstractmethod
    def on_table_del(self, table: CollectionT, key: Any) -> None:
        ...

    @abc.abstractmethod
    def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        ...

    @abc.abstractmethod
    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        ...

    @abc.abstractmethod
    def on_send_initiated(self, producer: ProducerT, topic: str,
                          message: PendingMessage,
                          keysize: int, valsize: int) -> Any:
        ...

    @abc.abstractmethod
    def on_send_completed(self,
                          producer: ProducerT,
                          state: Any,
                          metadata: RecordMetadata) -> None:
        ...

    @abc.abstractmethod
    def on_send_error(self,
                      producer: ProducerT,
                      exc: BaseException,
                      state: Any) -> None:
        ...

    @abc.abstractmethod
    def on_assignment_start(self, assignor: PartitionAssignorT) -> Dict:
        ...

    @abc.abstractmethod
    def on_assignment_error(self,
                            assignor: PartitionAssignorT,
                            state: Dict,
                            exc: BaseException) -> None:
        ...

    @abc.abstractmethod
    def on_assignment_completed(self,
                                assignor: PartitionAssignorT,
                                state: Dict) -> None:
        ...

    @abc.abstractmethod
    def on_rebalance_start(self, app: _AppT) -> Dict:
        ...

    @abc.abstractmethod
    def on_rebalance_return(self, app: _AppT, state: Dict) -> None:
        ...

    @abc.abstractmethod
    def on_rebalance_end(self, app: _AppT, state: Dict) -> None:
        ...

    @abc.abstractmethod
    def on_web_request_start(self, app: _AppT, request: web.Request, *,
                             view: web.View = None) -> Dict:
        ...

    @abc.abstractmethod
    def on_web_request_end(self,
                           app: _AppT,
                           request: web.Request,
                           response: Optional[web.Response],
                           state: Dict,
                           *,
                           view: web.View = None) -> None:
        ...


class SensorT(SensorInterfaceT, ServiceT):
    ...


class SensorDelegateT(SensorInterfaceT, Iterable):

    # Delegate calls to many sensors.

    @abc.abstractmethod
    def add(self, sensor: SensorT) -> None:
        ...

    @abc.abstractmethod
    def remove(self, sensor: SensorT) -> None:
        ...
