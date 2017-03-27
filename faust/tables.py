"""Tables (changelog stream)."""
from typing import Any, Callable, Tuple, Type
from . import stores
from .streams import Stream
from .types import AppT, Event, K
from .utils.collections import FastUserDict

__all__ = ['Table', 'table']


KVMapper = Callable[[K, Event], Tuple[K, Event]]


class table:

    def __init__(self, *,
                 store: str = None,
                 **kwargs: Any) -> None:
        self.store = store
        self.kwargs = kwargs

    def __call__(self, fun: KVMapper) -> 'Table':
        return Table(
            store=self.store,
            mapper=fun,
            **self.kwargs)


class Table(Stream, FastUserDict):
    StateStore: Type = None

    mapper: KVMapper

    _store: str

    def __init__(self, *,
                 store: str = None,
                 mapper: KVMapper = None,
                 **kwargs: Any) -> None:
        self.mapper = mapper
        self._store = store
        super().__init__(**kwargs)

    def on_init(self) -> None:
        assert not self._coroutines  # Table cannot have generator callback.
        self.data = self.StateStore()

    def on_bind(self, app: AppT) -> None:
        if self.StateStore is None:
            self.data = self.StateStore(url=None, app=app)
        else:
            self.data = stores.from_url(self._store or self.app.store, app)

    async def on_done(self, value: Event = None) -> None:
        k: K = value.req.key
        v: V = value
        if self.mapper is not None:
            k, v = self.mapper(k, v)
        self[k] = v
        super().on_done(value)  # <-- original value
