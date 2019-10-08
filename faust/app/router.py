"""Route messages to Faust nodes by partitioning."""
from typing import Tuple

from yarl import URL

from faust.exceptions import SameNode
from faust.types.app import AppT
from faust.types.assignor import PartitionAssignorT
from faust.types.core import K
from faust.types.router import HostToPartitionMap, RouterT
from faust.types.tables import CollectionT
from faust.types.web import Request, Response, Web
from faust.web.exceptions import ServiceUnavailable


class Router(RouterT):
    """Router for ``app.router``."""

    _assignor: PartitionAssignorT

    def __init__(self, app: AppT) -> None:
        self.app = app
        self._assignor = self.app.assignor

    def key_store(self, table_name: str, key: K) -> URL:
        """Return the URL of web server that hosts key in table."""
        table = self._get_table(table_name)
        topic = self._get_table_topic(table)
        k = self._get_serialized_key(table, key)
        return self._assignor.key_store(topic, k)

    def table_metadata(self, table_name: str) -> HostToPartitionMap:
        """Return metadata stored for table in the partition assignor."""
        table = self._get_table(table_name)
        topic = self._get_table_topic(table)
        return self._assignor.table_metadata(topic)

    def tables_metadata(self) -> HostToPartitionMap:
        """Return metadata stored for all tables in the partition assignor."""
        return self._assignor.tables_metadata()

    @classmethod
    def _get_table_topic(cls, table: CollectionT) -> str:
        return table.changelog_topic.get_topic_name()

    @classmethod
    def _get_serialized_key(cls, table: CollectionT, key: K) -> bytes:
        return table.changelog_topic.prepare_key(key, None)[0]

    def _get_table(self, name: str) -> CollectionT:
        return self.app.tables[name]

    async def route_req(self, table_name: str, key: K, web: Web,
                        request: Request) -> Response:
        """Route request to worker having key in table.

        Arguments:
            table_name: Name of the table.
            key: The key that we want.
            web: The currently sued web driver,
            request: The web request currently being served.
        """
        app = self.app
        try:
            dest_url: URL = app.router.key_store(table_name, key)
        except KeyError:
            raise ServiceUnavailable()
        dest_ident = (host, port) = self._urlident(dest_url)
        if dest_ident == self._urlident(app.conf.canonical_url):
            raise SameNode()
        routed_url = request.url.with_host(host).with_port(int(port))
        async with app.http_client.get(routed_url) as response:
            return web.text(
                await response.text(), content_type=response.content_type)

    def _urlident(self, url: URL) -> Tuple[str, int]:
        return (
            (url.host if url.scheme else url.path) or '',
            int(url.port or 80),
        )
