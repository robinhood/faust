"""Tables (changelog stream)."""
from typing import Any, ClassVar, Type
from . import stores
from .streams import Stream
from .types import AppT, Event, Topic
from .utils.collections import ManagedUserDict

__all__ = ['Table']


class Table(Stream, ManagedUserDict):
    StateStore: ClassVar[Type] = None

    changelog_topic: Topic
    _store: str

    def __init__(self, store_name: str,
                 *,
                 store: str = None,
                 **kwargs: Any) -> None:
        self.store_name = store_name
        self._store = store
        super().__init__(**kwargs)

    def on_init(self) -> None:
        assert not self._coroutines  # Table cannot have generator callback.
        self.data = self.StateStore()

    def on_bind(self, app: AppT) -> None:
        if self.StateStore is not None:
            self.data = self.StateStore(url=None, app=app)
        else:
            url = self._store or self.app.store
            self.data = stores.by_url(url)(url, app, loop=self.loop)
        self.changelog_topic = self.derive_topic(self._changelog_topic_name())

    def on_key_set(self, key: Any, value: Any) -> None:
        self.app.send(self.changelog_topic, key=key, value=value)

    def on_key_del(self, key: Any) -> None:
        self.app.send(self.changelog_topic, key=key, value=None)

    async def on_done(self, value: Event = None) -> None:
        k = value.req.key
        v: Event = value
        self[k] = v
        super().on_done(value)  # <-- original value

    def _changelog_topic_name(self):
        return '{0.app.id}-{0.store_name}-changelog'
