"""Table (key/value changelog stream)."""
from typing import Any

from mode import Seconds
from mode.utils.collections import ManagedUserDict

from faust import windows
from faust.streams import current_event
from faust.types.tables import TableT, WindowWrapperT
from faust.types.windows import WindowT
from faust.utils.terminal.tables import dict_as_ansitable

from .base import Collection
from .wrappers import WindowWrapper

__all__ = ['Table']


class Table(TableT, Collection, ManagedUserDict):
    """Table (non-windowed)."""

    def using_window(self, window: WindowT, *,
                     key_index: bool = False) -> WindowWrapperT:
        self.window = window
        self._changelog_compacting = True
        self._changelog_deleting = True
        self._changelog_topic = None  # will reset on next property access
        return WindowWrapper(self, key_index=key_index)

    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None,
                key_index: bool = False) -> WindowWrapperT:
        return self.using_window(
            windows.HoppingWindow(size, step, expires),
            key_index=key_index,
        )

    def tumbling(self, size: Seconds,
                 expires: Seconds = None,
                 key_index: bool = False) -> WindowWrapperT:
        return self.using_window(
            windows.TumblingWindow(size, expires),
            key_index=key_index,
        )

    def __missing__(self, key: Any) -> Any:
        if self.default is not None:
            return self.default()
        raise KeyError(key)

    def _has_key(self, key: Any) -> bool:
        return key in self

    def _get_key(self, key: Any) -> Any:
        return self[key]

    def _set_key(self, key: Any, value: Any) -> None:
        self[key] = value

    def _del_key(self, key: Any) -> None:
        del self[key]

    def on_key_get(self, key: Any) -> None:
        self._sensor_on_get(self, key)

    def on_key_set(self, key: Any, value: Any) -> None:
        event = current_event()
        self._send_changelog(event, key, value)
        if event is not None:
            partition = event.message.partition
            self._maybe_set_key_ttl(key, partition)
            self._sensor_on_set(self, key, value)
        else:
            raise TypeError(
                'Setting table key from outside of stream iteration')

    def on_key_del(self, key: Any) -> None:
        event = current_event()
        self._send_changelog(event, key, value=None, value_serializer='raw')
        if event is not None:
            partition = event.message.partition
            self._maybe_del_key_ttl(key, partition)
            self._sensor_on_del(self, key)
        else:
            raise TypeError(
                'Deleting table key from outside of stream iteration')

    def as_ansitable(self, title: str = '{table.name}',
                     **kwargs: Any) -> str:
        return dict_as_ansitable(
            self,
            title=title.format(table=self),
            **kwargs)
