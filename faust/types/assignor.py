import abc
from typing import MutableMapping, Sequence


class PartitionAssignorT(abc.ABC):

    replicas: int

    @abc.abstractmethod
    def assigned_standbys(self) -> MutableMapping[str, Sequence[int]]:
        ...

    @abc.abstractmethod
    def assigned_actives(self) -> MutableMapping[str, Sequence[int]]:
        ...
