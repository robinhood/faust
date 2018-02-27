"""Route messages to Faust nodes by partitioning."""
from typing import Tuple
from yarl import URL
from .exceptions import SameNode
from .types.app import AppT, Request, Response, Web
from .types.assignor import PartitionAssignorT
from .types.core import K
from .types.router import HostToPartitionMap, RouterT
from .types.tables import CollectionT


class Router(RouterT):
    """Router for ``app.router``."""

    _assignor: PartitionAssignorT

    def __init__(self, app: AppT) -> None:
        self.app = app
        self._assignor = self.app.assignor

    def key_store(self, table_name: str, key: K) -> str:
        table = self._get_table(table_name)
        topic = self._get_table_topic(table)
        k = self._get_serialized_key(table, key)
        return self._assignor.key_store(topic, k)

    def table_metadata(self, table_name: str) -> HostToPartitionMap:
        table = self._get_table(table_name)
        topic = self._get_table_topic(table)
        return self._assignor.table_metadata(topic)

    def tables_metadata(self) -> HostToPartitionMap:
        return self._assignor.tables_metadata()

    @classmethod
    def _get_table_topic(cls, table: CollectionT) -> str:
        return table.changelog_topic.get_topic_name()

    @classmethod
    def _get_serialized_key(cls, table: CollectionT, key: K) -> bytes:
        return table.changelog_topic.prepare_key(key, None)

    def _get_table(self, name: str) -> CollectionT:
        return self.app.tables[name]

    async def route_req(self,
                        table_name: str,
                        key: K,
                        web: Web,
                        request: Request) -> Response:
        app = self.app
        dest_url = app.router.key_store(table_name, key)
        dest_ident = (host, port) = self._urlident(dest_url)
        if dest_ident == self._urlident(app.conf.canonical_url):
            raise SameNode()
        routed_url = request.url.with_host(host).with_port(int(port))
        async with app.http_client.get(routed_url) as response:
            return web.text(await response.text(),
                            content_type=response.content_type)

    def _urlident(self, url: URL) -> Tuple[str, int]:
        return (
            url.host if url.scheme else url.path,
            int(url.port or 80),
        )
