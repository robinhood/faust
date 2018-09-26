import socket
import typing
from typing import Any, ClassVar, Mapping, Optional, Type, Union

from mode.utils.compat import want_bytes
from mode.utils.objects import cached_property
from yarl import URL

from faust.exceptions import ImproperlyConfigured
from faust.types import AppT
from . import base

try:
    import aredis
    import aredis.exceptions
except ImportError:
    aredis = None  # noqa

if typing.TYPE_CHECKING:
    from aredis import StrictRedis as RedisClientT
else:
    class RedisClientT: ...  # noqa


class CacheBackend(base.CacheBackend):
    _client: Optional[RedisClientT] = None

    _client_by_scheme: ClassVar[Mapping[str, Type[RedisClientT]]] = {}

    if aredis is not None:
        operational_errors = (
            socket.error,
            IOError,
            OSError,
            aredis.exceptions.ConnectionError,
            aredis.exceptions.TimeoutError,
        )
        invalidating_errors = (
            aredis.exceptions.DataError,
            aredis.exceptions.InvalidResponse,
            aredis.exceptions.ResponseError,
        )
        irrecoverable_errors = (
            aredis.exceptions.AuthenticationError,
        )

        _client_by_scheme = {
            'redis': aredis.StrictRedis,
            'rediscluster': aredis.StrictRedisCluster,
        }

    def __init__(self,
                 app: AppT,
                 url: Union[URL, str],
                 *,
                 max_connections: int = None,
                 max_connections_per_node: int = None,
                 **kwargs: Any) -> None:
        super().__init__(app, url, **kwargs)
        if max_connections is not None:
            self.max_connections = max_connections
        if max_connections_per_node is not None:
            self.max_connections_per_node = max_connections_per_node

    async def _get(self, key: str) -> Optional[bytes]:
        value: Optional[bytes] = await self.client.get(key)
        if value is not None:
            return want_bytes(value)
        return None

    async def _set(self, key: str, value: bytes, timeout: float) -> None:
        await self.client.setex(key, int(timeout), value)

    async def _delete(self, key: str) -> None:
        await self.client.delete(key)

    async def on_start(self) -> None:
        if aredis is None:
            raise ImproperlyConfigured(
                'Redis cache backend requires `pip install aredis`')
        await self.connect()

    async def connect(self) -> None:
        if self._client is None:
            self._client = self._new_client()
        await self.client.ping()

    def _new_client(self) -> RedisClientT:
        url = self.url
        Client = self._client_by_scheme[url.scheme]
        return Client(
            host=url.host,
            port=url.port,
            max_connections=int(
                url.query.get('max_connections') or
                self.max_connections,
            ),
            max_connections_per_node=int(
                url.query.get('max_connections_per_node') or
                self.max_connections_per_node,
            ),
        )
        return aredis.StrictRedisCluster(
            host=self.host,
            port=self.port,
            max_connections=self.max_connections,
            max_connections_per_node=self.max_connections_per_node,
            skip_full_coverage_check=True,
        )

    @cached_property
    def client(self) -> RedisClientT:
        if self._client is None:
            raise RuntimeError('Cache backend not started')
        return self._client
