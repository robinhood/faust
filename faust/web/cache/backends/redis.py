"""Redis cache backend."""
import socket
import typing
from enum import Enum
from typing import Any, Mapping, Optional, Type, Union

from mode.utils.compat import want_bytes
from mode.utils.objects import cached_property
from yarl import URL

from faust.exceptions import ImproperlyConfigured
from faust.types import AppT
from . import base

try:
    import aredis
    import aredis.exceptions
except ImportError:  # pragma: no cover
    aredis = None  # noqa

if typing.TYPE_CHECKING:  # pragma: no cover
    from aredis import StrictRedis as _RedisClientT
else:
    class _RedisClientT: ...  # noqa


class RedisScheme(Enum):
    """Types of Redis configurations."""

    SINGLE_NODE = 'redis'
    CLUSTER = 'rediscluster'


class CacheBackend(base.CacheBackend):
    """Backend for cache operations using Redis."""

    connect_timeout: Optional[float]
    stream_timeout: Optional[float]
    max_connections: Optional[int]
    max_connections_per_node: Optional[int]

    _client: Optional[_RedisClientT] = None
    _client_by_scheme: Mapping[str, Type[_RedisClientT]]

    if aredis is None:  # pragma: no cover
        ...
    else:
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

    def __init__(self,
                 app: AppT,
                 url: Union[URL, str],
                 *,
                 connect_timeout: float = None,
                 stream_timeout: float = None,
                 max_connections: int = None,
                 max_connections_per_node: int = None,
                 **kwargs: Any) -> None:
        super().__init__(app, url, **kwargs)
        self.connect_timeout = connect_timeout
        self.stream_timeout = stream_timeout
        self.max_connections = max_connections
        self.max_connections_per_node = max_connections_per_node
        self._client_by_scheme = self._init_schemes()

    def _init_schemes(self) -> Mapping[str, Type[_RedisClientT]]:
        if aredis is None:  # pragma: no cover
            return {}
        else:
            return {
                RedisScheme.SINGLE_NODE.value: aredis.StrictRedis,
                RedisScheme.CLUSTER.value: aredis.StrictRedisCluster,
            }

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

    def _new_client(self) -> _RedisClientT:
        return self._client_from_url_and_query(self.url, **self.url.query)

    def _client_from_url_and_query(
            self,
            url: URL,
            *,
            connect_timeout: str = None,
            stream_timeout: str = None,
            max_connections: str = None,
            max_connections_per_node: str = None,
            **kwargs: Any) -> _RedisClientT:
        Client = self._client_by_scheme[url.scheme]
        return Client(**self._prepare_client_kwargs(
            url,
            host=url.host,
            port=url.port,
            db=self._db_from_path(url.path),
            password=url.password,
            connect_timeout=self._float_from_str(
                connect_timeout, self.connect_timeout),
            stream_timeout=self._float_from_str(
                stream_timeout, self.stream_timeout),
            max_connections=self._int_from_str(
                max_connections, self.max_connections),
            max_connections_per_node=self._int_from_str(
                max_connections_per_node, self.max_connections_per_node),
            skip_full_coverage_check=True,
        ))

    def _prepare_client_kwargs(self, url: URL, **kwargs: Any) -> Mapping:
        if url.scheme == RedisScheme.CLUSTER.value:
            return self._as_cluster_kwargs(**kwargs)
        return kwargs

    def _as_cluster_kwargs(self,
                           db: str = None,
                           **kwargs: Any) -> Mapping:
        # Redis Cluster does not support db as argument.
        return kwargs

    def _int_from_str(self,
                      val: str = None,
                      default: int = None) -> Optional[int]:
        return int(val) if val else default

    def _float_from_str(self,
                        val: str = None,
                        default: float = None) -> Optional[float]:
        return float(val) if val else default

    def _db_from_path(self, path: str) -> int:
        if not path or path == '/':
            return 0  # default db
        try:
            return int(path.strip('/'))
        except ValueError:
            raise ValueError(
                f'Database is int between 0 and limit - 1, not {path!r}')

    @cached_property
    def client(self) -> _RedisClientT:
        if self._client is None:
            raise RuntimeError('Cache backend not started')
        return self._client
