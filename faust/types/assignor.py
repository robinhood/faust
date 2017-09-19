import abc
import typing
from typing import Iterable, List, MutableMapping
from .topics import TopicPartition
from ..utils.types.services import ServiceT


TopicPartitionsMap = MutableMapping[str, List[int]]
HostPartitionsMap = MutableMapping[str, TopicPartitionsMap]


if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...      # noqa


class PartitionAssignorT(abc.ABC):

    replicas: int
    app: AppT

    @abc.abstractmethod
    def assigned_standbys(self) -> Iterable[TopicPartition]:
        ...

    @abc.abstractmethod
    def assigned_actives(self) -> Iterable[TopicPartition]:
        ...

    @abc.abstractmethod
    def key_store(self, topic: str, key: bytes) -> str:
        ...

    @abc.abstractmethod
    def table_metadata(self, topic: str) -> HostPartitionsMap:
        ...

    @abc.abstractmethod
    def tables_metadata(self) -> HostPartitionsMap:
        ...


class MasterAssignorT(ServiceT):

    app: AppT

    @abc.abstractmethod
    def is_master(self) -> bool:
        ...
