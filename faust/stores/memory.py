from typing import Any, Callable, Iterable
from . import base
from ..types import EventT
from ..utils.collections import FastUserDict
from ..utils.logging import get_logger

logger = get_logger(__name__)


class Store(base.Store, FastUserDict):
    logger = logger

    def on_init(self) -> None:
        self.data = {}

    def _clear(self) -> None:
        self.data.clear()

    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        # default store does not do serialization, so we need
        # to convert these raw json serialized keys to proper structures
        # (E.g. regenerate tuples in WindowedKeys etc).
        self.data.update((
            (to_key(event.key), to_value(event.value))
            for event in batch
        ))
