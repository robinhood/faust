import abc
import typing
from typing import (
    Any, Iterable,
    Mapping, MutableSet, Optional, Pattern, Sequence, Union,
)
from .channels import ChannelT
from .tuples import TopicPartition
from ..utils.times import Seconds
from ..utils.types.services import ServiceT

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelArg
    from .streams import StreamT
    from .transports import ConsumerT, TPorTopicSet
else:
    class AppT: ...             # noqa
    class ModelArg: ...         # noqa
    class StreamT: ...          # noqa
    class ConsumerT: ...        # noqa
    class TPorTopicSet: ...     # noqa

__all__ = ['TopicT', 'ConductorT']


class TopicT(ChannelT):
    topics: Sequence[str]
    pattern: Pattern
    retention: Seconds
    compacting: bool
    deleting: bool
    replicas: int
    config: Mapping[str, Any]
    acks: bool

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Union[str, Pattern] = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 retention: Seconds = None,
                 compacting: bool = None,
                 deleting: bool = None,
                 replicas: int = None,
                 acks: bool = True,
                 config: Mapping[str, Any] = None) -> None:
        ...

    @property
    @abc.abstractmethod
    def pattern(self) -> Optional[Pattern]:
        ...

    @pattern.setter
    def pattern(self, pattern: Union[str, Pattern]) -> None:
        ...

    @property
    @abc.abstractmethod
    def partitions(self) -> int:
        ...

    @partitions.setter
    def partitions(self, partitions: int) -> None:
        ...

    @property
    @abc.abstractmethod
    def replicas(self) -> int:
        ...

    @replicas.setter
    def replicas(self, replicas: int) -> None:
        ...

    @abc.abstractmethod
    def derive(self,
               *,
               topics: Sequence[str] = None,
               key_type: ModelArg = None,
               value_type: ModelArg = None,
               partitions: int = None,
               retention: Seconds = None,
               compacting: bool = None,
               deleting: bool = None,
               config: Mapping[str, Any] = None,
               prefix: str = '',
               suffix: str = '') -> 'TopicT':
        ...

    @abc.abstractmethod
    def get_topic_name(self) -> str:
        ...


class ConductorT(ServiceT, MutableSet[ChannelT]):

    app: AppT

    @abc.abstractmethod
    def __init__(self, app: AppT, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def acks_enabled_for(self, topic: str) -> bool:
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet) -> bool:
        ...

    @abc.abstractmethod
    async def wait_for_subscriptions(self) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(
            self, revoked: Iterable[TopicPartition]) -> None:
        ...
