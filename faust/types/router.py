"""Types for module :mod:`faust.router`."""
import abc
import typing
from .assignor import HostToPartitionMap
from .core import K


if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...      # noqa


class RouterT(abc.ABC):
    """Router type class."""

    app: AppT

    @abc.abstractmethod
    def __init__(self, app: AppT) -> None:
        ...

    @abc.abstractmethod
    def key_store(self, table_name: str, key: K) -> str:
        ...

    @abc.abstractmethod
    def table_metadata(self, table_name: str) -> HostToPartitionMap:
        ...

    @abc.abstractmethod
    def tables_metadata(self) -> HostToPartitionMap:
        ...
