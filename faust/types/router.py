import abc
import typing
from typing import List, MutableMapping
from .core import K


TopicPartitionsMap = MutableMapping[str, List[int]]
HostPartitionsMap = MutableMapping[str, TopicPartitionsMap]


if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...      # noqa


class RouterT(abc.ABC):

    app: AppT

    @abc.abstractmethod
    def key_store(self, table_name: str, key: K) -> str:
        ...

    @abc.abstractmethod
    def table_metadata(self, table_name: str) -> HostPartitionsMap:
        ...

    @abc.abstractmethod
    def tables_metadata(self) -> HostPartitionsMap:
        ...
