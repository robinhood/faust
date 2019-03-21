"""Cache backend - base implementation."""
import abc
from typing import Any, ClassVar, Optional, Tuple, Type, Union

from mode import Service
from mode.utils.contexts import asynccontextmanager
from mode.utils.logging import get_logger
from mode.utils.typing import AsyncContextManager
from yarl import URL

from faust.types import AppT
from faust.types.web import CacheBackendT
from faust.web.cache.exceptions import CacheUnavailable

logger = get_logger(__name__)

E_CACHE_IRRECOVERABLE = 'Cache disabled for irrecoverable error: %r'
E_CACHE_INVALIDATING = 'Destroying cache for key %r caused error: %r'
E_CANNOT_INVALIDATE = 'Unable to invalidate key %r: %r'
E_CACHE_INOPERATIONAL = 'Cache operational error: %r'


class CacheBackend(CacheBackendT, Service):
    """Backend for cache operations."""

    logger = logger

    Unavailable: Type[BaseException] = CacheUnavailable

    operational_errors: ClassVar[Tuple[Type[BaseException], ...]] = ()
    invalidating_errors: ClassVar[Tuple[Type[BaseException], ...]] = ()
    irrecoverable_errors: ClassVar[Tuple[Type[BaseException], ...]] = ()

    def __init__(self,
                 app: AppT,
                 url: Union[URL, str] = 'memory://',
                 **kwargs: Any) -> None:
        self.app = app
        self.url = URL(url)
        Service.__init__(self, **kwargs)

    @abc.abstractmethod
    async def _get(self, key: str) -> Optional[bytes]:
        ...

    @abc.abstractmethod
    async def _set(self, key: str, value: bytes, timeout: float) -> None:
        ...

    @abc.abstractmethod
    async def _delete(self, key: str) -> None:
        ...

    async def get(self, key: str) -> Optional[bytes]:
        async with self._recovery_context(key):
            return await self._get(key)

    async def set(self, key: str, value: bytes, timeout: float) -> None:
        assert timeout is not None
        async with self._recovery_context(key):
            await self._set(key, value, timeout)

    async def delete(self, key: str) -> None:
        async with self._recovery_context(key):
            await self._delete(key)

    @asynccontextmanager
    async def _recovery_context(self, key: str) -> AsyncContextManager:
        try:
            yield
        except self.irrecoverable_errors as exc:
            self.log.exception(E_CACHE_IRRECOVERABLE, exc)  # noqa: G200
            raise self.Unavailable(exc)
        except self.invalidating_errors as exc:
            self.log.warning(  # noqa: G200
                E_CACHE_INVALIDATING, key, exc, exc_info=1)
            try:
                await self._delete(key)
            except self.operational_errors + self.invalidating_errors as exc:
                self.log.exception(  # noqa: G200
                    E_CANNOT_INVALIDATE, key, exc)
            raise self.Unavailable()
        except self.operational_errors as exc:
            self.log.warning(  # noqa: G200
                E_CACHE_INOPERATIONAL, exc, exc_info=1)
            raise self.Unavailable()

    def _repr_info(self) -> str:
        return f'url={self.url!r}'
