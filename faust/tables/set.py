"""Set stored as table with True values."""
from typing import Any
from faust.streams import current_event
from faust.types.tables import SetT
from .base import Collection

__all__ = ['Set']


class Set(SetT, Collection):
    """Table where keys are either present, or not present."""

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any) -> None:
        self._send_changelog(key, value=True)
        event = current_event()
        partition = event.message.partition
        self._maybe_set_key_ttl(key, partition)
        self._sensor_on_set(self, key, value=True)

    def on_key_del(self, key: Any) -> None:
        self._send_changelog(key, value=None)
        event = current_event()
        partition = event.message.partition
        self._maybe_del_key_ttl(key, partition)
        self._sensor_on_del(self, key)
