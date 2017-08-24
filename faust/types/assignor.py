import abc
import typing
from typing import Iterable
from .topics import TopicPartition


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
