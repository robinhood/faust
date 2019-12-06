"""In-memory cache backend."""
import sys
import time

from contextlib import suppress
from typing import Callable, Dict, Generic, Optional, TypeVar

from mode.utils.compat import want_bytes

from . import base

KT = TypeVar('KT')
VT = TypeVar('VT')

TIME_MONOTONIC: Callable[[], float]
if sys.platform == 'win32':
    TIME_MONOTONIC = time.time
else:
    TIME_MONOTONIC = time.monotonic


class CacheStorage(Generic[KT, VT]):
    """In-memory storage for cache."""

    def __init__(self) -> None:
        self._data: Dict[KT, VT] = {}
        self._time_index: Dict[KT, float] = {}
        self._expires: Dict[KT, float] = {}

    def get(self, key: KT) -> Optional[VT]:
        """Get value for key, or :const:`None` if missing."""
        with suppress(KeyError):
            expires = self._expires[key]
            time_set = self._time_index[key]
            now = TIME_MONOTONIC()
            if time_set is None or now - time_set > expires:
                self.delete(key)
                return None

        with suppress(KeyError):
            return self._data[key]
        return None

    def last_set_ttl(self, key: KT) -> Optional[float]:
        """Return the last set TTL for key, or :const:`None` if missing."""
        return self._expires.get(key)

    def expire(self, key: KT) -> None:
        """Expire value for key immediately."""
        self.delete(key)

    def set(self, key: KT, value: VT) -> None:
        """Set value for key."""
        self._data[key] = value

    def setex(self, key: KT, timeout: float, value: VT) -> None:
        """Set value & set timeout for key."""
        self._expires[key] = timeout
        self._time_index[key] = TIME_MONOTONIC()
        self.set(key, value)

    def ttl(self, key: KT) -> Optional[float]:
        """Return the remaining TTL for key."""
        try:
            return (
                self._expires[key] - TIME_MONOTONIC() - self._time_index[key])
        except KeyError:
            return None

    def delete(self, key: KT) -> None:
        """Delete value for key."""
        self._expires.pop(key, None)
        self._data.pop(key, None)  # type: ignore
        self._time_index.pop(key, None)

    def clear(self) -> None:
        """Clear all data."""
        self._expires.clear()
        self._data.clear()
        self._time_index.clear()


class CacheBackend(base.CacheBackend):
    """In-memory backend for cache operations."""

    def __post_init__(self) -> None:
        # we reuse this in t/conftest to mock a Redis server :D
        self.storage: CacheStorage[str, bytes] = CacheStorage()

    async def _get(self, key: str) -> Optional[bytes]:
        return self.storage.get(key)

    async def _set(self, key: str, value: bytes,
                   timeout: float = None) -> None:
        if timeout is not None:
            self.storage.setex(key, timeout, want_bytes(value))
        else:
            self.storage.set(key, want_bytes(value))

    async def _delete(self, key: str) -> None:
        self.storage.delete(key)
