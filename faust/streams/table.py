"""Tables (changelog stream)."""
from typing import Any, Callable, Mapping, cast
from .. import stores
from ..types import AppT
from ..types.stores import StoreT
from ..types.tables import TableT
from ..types.windows import WindowT
from ..utils.collections import ManagedUserDict
from .stream import Stream

__all__ = ['Table']


class Table(Stream, TableT, ManagedUserDict):

    _store: str

    def __init__(self, *,
                 table_name: str = None,
                 default: Callable[[], Any] = None,
                 store: str = 'memory://',
                 window: WindowT = None,
                 **kwargs: Any) -> None:
        self.table_name = table_name
        self.default = default
        self._store = store
        self.data = {}
        self.window = window
        assert not self._coroutines  # Table cannot have generator callback.
        Stream.__init__(self, **kwargs)

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app._tables mapping.
        return Stream.__hash__(self)

    def __missing__(self, key: Any) -> Any:
        if self.default is not None:
            value = self[key] = self.default()
            return value
        raise KeyError(key)

    def info(self) -> Mapping[str, Any]:
        # Used to recreate object in .clone()
        return {**super().info(), **{
            'table_name': self.table_name,
            'store': self._store,
            'default': self.default,
        }}

    def on_bind(self, app: AppT) -> None:
        if self.StateStore is not None:
            self.data = self.StateStore(url=None, app=app, loop=self.loop)
        else:
            url = self._store or self.app.store
            self.data = stores.by_url(url)(url, app, loop=self.loop)
        # Table.start() also starts Store
        self.add_dependency(cast(StoreT, self.data))
        self.changelog_topic = self.derive_topic(self._changelog_topic_name())
        app.add_table(self)

    def on_key_set(self, key: Any, value: Any) -> None:
        self.app.send_soon(self.changelog_topic, key=key, value=value)

    def on_key_del(self, key: Any) -> None:
        self.app.send_soon(self.changelog_topic, key=key, value=None)

    def _changelog_topic_name(self) -> str:
        return '{0.app.id}-{0.table_name}-changelog'

    def __repr__(self) -> str:
        return Stream.__repr__(self)

    @property
    def label(self) -> str:
        return '{}: {}@{}'.format(
            type(self).__name__, self.table_name, self._store,
        )
