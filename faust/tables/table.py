"""Table (key/value changelog stream)."""
import sys
from operator import itemgetter
from typing import Any, Callable, IO, Iterable, List, cast

from mode import Seconds
from mode.utils import text
from mode.utils.collections import ManagedUserDict

from faust import windows
from faust.streams import current_event
from faust.types.tables import TableT, WindowWrapperT
from faust.types.windows import WindowT
from faust.utils import terminal

from .base import Collection
from .wrappers import WindowWrapper

__all__ = ['Table']


class Table(TableT, Collection, ManagedUserDict):
    """Table (non-windowed)."""

    def using_window(self, window: WindowT) -> WindowWrapperT:
        self.window = window
        self._changelog_compacting = True
        self._changelog_deleting = True
        self._changelog_topic = None  # will reset on next property access
        return WindowWrapper(self)

    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.HoppingWindow(size, step, expires))

    def tumbling(self, size: Seconds,
                 expires: Seconds = None) -> WindowWrapperT:
        return self.using_window(windows.TumblingWindow(size, expires))

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
        self._send_changelog(key, value)
        event = current_event()
        if event is not None:
            partition = event.message.partition
            self._maybe_set_key_ttl(key, partition)
            self._sensor_on_set(self, key, value)
        else:
            raise TypeError(
                'Setting table key from outside of stream iteration')

    def on_key_del(self, key: Any) -> None:
        self._send_changelog(key, value=None, value_serializer='raw')
        event = current_event()
        if event is not None:
            partition = event.message.partition
            self._maybe_del_key_ttl(key, partition)
            self._sensor_on_del(self, key)
        else:
            raise TypeError(
                'Deleting table key from outside of stream iteration')

    def as_ansitable(self,
                     *,
                     key: str = 'Key',
                     value: str = 'Value',
                     sort: bool = False,
                     sortkey: Callable[[Any], Any] = itemgetter(0),
                     target: IO = sys.stdout,
                     title: str = '{table.name}') -> str:
        header = [text.title(key), text.title(value)]
        data = cast(Iterable[List[str]], dict(self).items())
        data = list(sorted(data, key=sortkey)) if sort else list(data)
        if sort:
            data = list(sorted(data, key=sortkey))
        return terminal.table(
            [header] + list(data),
            title=text.title(title.format(table=self)),
        ).table
