"""Set stored as table with True values."""
from typing import Any
from .base import Collection
from ..types.tables import SetT
from ..utils.collections import ManagedUserSet

__all__ = ['Set']


class Set(Collection, SetT, ManagedUserSet):
    """Table where keys are either present, or not present."""

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any) -> None:
        self._send_changelog(key, value=True)
        self._maybe_set_key_ttl(key)
        self._sensor_on_set(self, key, value=True)

    def on_key_del(self, key: Any) -> None:
        self._send_changelog(key, value=None)
        self._maybe_del_key_ttl(key)
        self._sensor_on_del(self, key)
