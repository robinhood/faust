"""Types for module :mod:`faust.router`."""
import abc
import typing

from yarl import URL

from .assignor import HostToPartitionMap
from .core import K
from . import web

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
else:
    class _AppT: ...      # noqa


class RouterT(abc.ABC):
    """Router type class."""

    app: _AppT

    @abc.abstractmethod
    def __init__(self, app: _AppT) -> None:
        ...

    @abc.abstractmethod
    def key_store(self, table_name: str, key: K) -> URL:
        ...

    @abc.abstractmethod
    def table_metadata(self, table_name: str) -> HostToPartitionMap:
        ...

    @abc.abstractmethod
    def tables_metadata(self) -> HostToPartitionMap:
        ...

    @abc.abstractmethod
    async def route_req(self, table_name: str, key: K, web: web.Web,
                        request: web.Request) -> web.Response:
        ...
