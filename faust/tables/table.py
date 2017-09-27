"""Table (key/value changelog stream)."""
from typing import Any
from mode import Seconds
from .base import Collection
from .wrappers import WindowWrapper
from .. import windows
from ..types.tables import TableT, WindowWrapperT
from ..types.windows import WindowT
from ..utils.collections import ManagedUserDict

__all__ = ['Table']


class Table(Collection, TableT, ManagedUserDict):

    def using_window(self, window: WindowT) -> WindowWrapperT:
        self.window = window
        self.changelog_topic = self._new_changelog_topic(
            retention=window.expires,
            compacting=True,
            deleting=True,
        )
        return WindowWrapper(self)

    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.HoppingWindow(size, step, expires))

    def tumbling(self, size: Seconds,
                 expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.TumblingWindow(size, expires))

    def __missing__(self, key: Any) -> Any:
        if self.default is not None:
            value = self.data[key] = self.default()
            return value
        raise KeyError(key)

    def _get_key(self, key: Any) -> Any:
        return self[key]

    def _set_key(self, key: Any, value: Any) -> None:
        self[key] = value

    def _del_key(self, key: Any) -> None:
        del self[key]

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any, value: Any) -> None:
        self._send_changelog(key, value)
        self._maybe_set_key_ttl(key)
        self._sensor_on_set(self, key, value)

    def on_key_del(self, key: Any) -> None:
        self._send_changelog(key, value=None)
        self._maybe_del_key_ttl(key)
        self._sensor_on_del(self, key)
