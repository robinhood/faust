from contextlib import suppress
from time import monotonic
from typing import Dict, Generic, Optional, TypeVar

from mode.utils.compat import want_bytes

from . import base

KT = TypeVar('KT')
VT = TypeVar('VT')


class CacheStorage(Generic[KT, VT]):

    def __init__(self) -> None:
        self._data: Dict[KT, VT] = {}
        self._time_index: Dict[KT, float] = {}
        self._expires: Dict[KT, float] = {}

    def get(self, key: KT) -> Optional[VT]:
        with suppress(KeyError):
            expires = self._expires[key]
            now = monotonic()
            if now - self._time_index[key] > expires:
                self.delete(key)
                return None
            self._time_index[key] = now

        with suppress(KeyError):
            return self._data[key]

    def last_set_ttl(self, key: KT) -> Optional[float]:
        return self._expires.get(key)

    def expire(self, key: KT) -> None:
        self._time_index[key] -= self._expires[key]

    def set(self, key: KT, value: VT) -> None:
        self._data[key] = value

    def setex(self, key: KT, timeout: float, value: VT) -> None:
        self._expires[key] = timeout
        self._time_index[key] = monotonic()
        self.set(key, value)

    def ttl(self, key: KT) -> Optional[float]:
        try:
            return self._expires[key] - monotonic() - self._time_index[key]
        except KeyError:
            return None

    def delete(self, key: KT) -> None:
        self._expires.pop(key, None)
        self._data.pop(key, None)  # type: ignore
        self._time_index.pop(key, None)

    def clear(self) -> None:
        self._expires.clear()
        self._data.clear()
        self._time_index.clear()


class CacheBackend(base.CacheBackend):

    def on_init(self) -> None:
        # we reuse this in t/conftest to mock a Redis server :D
        self.storage: CacheStorage[str, bytes] = CacheStorage()

    async def _get(self, key: str) -> Optional[bytes]:
        return self.storage.get(key)

    async def _set(self, key: str, value: bytes, timeout: float) -> None:
        self.storage.setex(key, timeout, want_bytes(value))

    async def _delete(self, key: str) -> None:
        self.storage.delete(key)
