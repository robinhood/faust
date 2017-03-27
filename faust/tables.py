"""Tables (changelog stream)."""
from typing import Type
from .streams import Stream
from .types import Event
from .utils.collections import FastUserDict

__all__ = ['Table']


class Table(Stream, FastUserDict):
    StateStore: Type = dict

    def on_init(self) -> None:
        assert not self._coroutines  # Table cannot have generator callback.
        self.data = self.StateStore()

    async def on_done(self, value: Event = None) -> None:
        self[value.req.key] = value
        super().on_done(value)
