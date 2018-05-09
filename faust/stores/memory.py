"""In-memory table storage."""
from typing import (
    Any,
    Callable,
    Iterable,
    MutableMapping,
    Optional,
    Set,
    Tuple,
)
from mode.utils.collections import FastUserDict
from faust.types import EventT, TP
from . import base


class Store(base.Store, FastUserDict):
    """Table storage using an in-memory dictionary."""

    def on_init(self) -> None:
        self.data: MutableMapping = {}

    def _clear(self) -> None:
        self.data.clear()

    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        # default store does not do serialization, so we need
        # to convert these raw json serialized keys to proper structures
        # (E.g. regenerate tuples in WindowedKeys etc).
        to_delete: Set[Any] = set()
        delete_key = self.data.pop
        self.data.update(self._create_batch_iterator(
            to_delete.add, to_key, to_value, batch))
        for key in to_delete:
            delete_key(key, None)

    def _create_batch_iterator(
            self,
            mark_as_delete: Callable[[Any], None],
            to_key: Callable[[Any], Any],
            to_value: Callable[[Any], Any],
            batch: Iterable[EventT]) -> Iterable[Tuple[Any, Any]]:
        for event in batch:
            key = to_key(event.key)
            # to delete keys in the table we set the raw value to None
            if event.message.value is None:
                mark_as_delete(key)
                continue
            yield key, to_value(event.value)

    def persisted_offset(self, tp: TP) -> Optional[int]:
        return None

    def reset_state(self) -> None:
        ...
