from time import monotonic
from typing import Dict, Optional

from mode.utils.compat import want_bytes

from . import base


class CacheBackend(base.CacheBackend):
    _data: Dict[str, bytes]
    _time_index: Dict[str, float]
    _expires: Dict[str, float]

    def on_init(self) -> None:
        self._data = {}
        self._time_index = {}
        self._expires = {}

    async def _get(self, key: str) -> Optional[bytes]:
        try:
            expires = self._expires[key]
            now = monotonic()
            if now - self._time_index[key] > expires:
                self._delete(key)
                return None
            self._time_index[key] = now
            return self._data[key]
        except KeyError:
            return None

    async def _set(self, key: str, value: bytes, timeout: float) -> None:
        self._expires[key] = timeout
        self._data[key] = want_bytes(value)
        self._time_index[key] = monotonic()

    async def _delete(self, key: str) -> None:
        self._expires.pop(key, None)
        self._data.pop(key, None)
        self._time_index.pop(key, None)
