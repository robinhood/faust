from typing import Any, MutableMapping
from .streams import Stream
from .types import K, Topic, V


class Table(Stream):

    #: This maintains the state of the stream (the K/V store).
    _state: MutableMapping

    def on_init(self) -> None:
        assert not self._coroutines  # Table cannot have generator callback.
        self._state = {}

    async def on_message(self, topic: Topic, key: K, value: V) -> None:
        self._state[key] = await self.process(key, value)

    def __getitem__(self, key: Any) -> Any:
        return self._state[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._state[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._state[key]
