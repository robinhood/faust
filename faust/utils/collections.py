"""Custom data structures."""
import threading
from collections import OrderedDict, UserDict, UserList
from typing import (
    Any, ContextManager, Dict, ItemsView, Iterator, KeysView,
    Mapping, MutableMapping, MutableSet, Tuple, TypeVar, ValuesView, cast,
)
from mode.utils.compat import DummyContext

__all__ = [
    'FastUserDict',
    'FastUserSet',
    'FastUserList',
    'LRUCache',
    'ManagedUserDict',
    'ManagedUserSet',
]

KT = TypeVar('KT')
VT = TypeVar('VT')


class FastUserDict(UserDict):
    """Proxy to dict.

    Like :class:`collection.UserDict` but reimplements some methods
    for better performance when the underlying dictionary is a real dict.
    """

    data: MutableMapping

    # Mypy forces us to redefine these, for some reason:

    def __getitem__(self, key: Any) -> Any:
        if not hasattr(self, '__missing__'):
            return self.data[key]
        if key in self.data:
            return self.data[key]
        return self.__missing__(key)  # type: ignore

    def __setitem__(self, key: Any, value: Any) -> None:
        self.data[key] = value

    def __delitem__(self, key: Any) -> None:
        del self.data[key]

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator:
        return iter(self.data)

    # Rest is fast versions of generic slow MutableMapping methods.

    def __contains__(self, key: Any) -> bool:
        return key in self.data

    def update(self, *args: Any, **kwargs: Any) -> None:
        self.data.update(*args, **kwargs)

    def clear(self) -> None:
        self.data.clear()

    def items(self) -> ItemsView:
        return cast(ItemsView, self.data.items())

    def keys(self) -> KeysView:
        return cast(KeysView, self.data.keys())

    def values(self) -> ValuesView:
        return self.data.values()


class FastUserSet(MutableSet):
    """Proxy to set."""

    data: MutableMapping[Any, bool]

    def __contains__(self, key: Any) -> bool:
        return key in self.data

    def __iter__(self) -> Iterator:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def add(self, key: Any) -> None:
        self.data[key] = True

    def discard(self, key: Any) -> None:
        self.data.pop(key, None)

    # Rest is fast versions of generic slow MutableSet methods.

    def clear(self) -> None:
        self.data.clear()


class FastUserList(UserList):
    """Proxy to list."""


class LRUCacheKeysView(KeysView):

    def __init__(self, store: 'LRUCache') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator:
        yield from self._mapping._keys()


class LRUCacheValuesView(ValuesView):

    def __init__(self, store: 'LRUCache') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator:
        yield from self._mapping._values()


class LRUCacheItemsView(ItemsView):

    def __init__(self, store: 'LRUCache') -> None:
        self._mapping = store

    def __iter__(self) -> Iterator[Tuple[KT, VT]]:
        yield from self._mapping._items()


class LRUCache(FastUserDict, MutableMapping[KT, VT]):
    """LRU Cache implementation using a doubly linked list to track access.

    Arguments:
        limit (int): The maximum number of keys to keep in the cache.
            When a new key is inserted and the limit has been exceeded,
            the *Least Recently Used* key will be discarded from the
            cache.
        thread_safety (bool): Enable if multiple OS threads are going
            to access/mutate the cache.
    """

    limit: int
    thread_safety: bool
    _mutex: ContextManager
    data: OrderedDict

    def __init__(self, limit: int = None,
                 *,
                 thread_safety: bool = False) -> None:
        self.limit = limit
        self.thread_safety = thread_safety
        self._mutex = self._new_lock()
        self.data: OrderedDict = OrderedDict()

    def __getitem__(self, key: KT) -> VT:
        with self._mutex:
            value = self[key] = self.data.pop(key)
            return value

    def update(self, *args: Any, **kwargs: Any) -> None:
        with self._mutex:
            data, limit = self.data, self.limit
            data.update(*args, **kwargs)
            if limit and len(data) > limit:
                # pop additional items in case limit exceeded
                for _ in range(len(data) - limit):
                    data.popitem(last=False)

    def popitem(self, *, last: bool = True) -> Tuple[KT, VT]:
        with self._mutex:
            return self.data.popitem(last)

    def __setitem__(self, key: KT, value: VT) -> None:
        # remove least recently used key.
        with self._mutex:
            if self.limit and len(self.data) >= self.limit:
                self.data.pop(next(iter(self.data)))
            self.data[key] = value

    def __iter__(self) -> Iterator:
        return iter(self.data)

    def keys(self) -> KeysView[KT]:
        return LRUCacheKeysView(self)

    def _keys(self) -> Iterator[KT]:
        # userdict.keys in py3k calls __getitem__
        with self._mutex:
            yield from self.data.keys()

    def values(self) -> ValuesView[VT]:
        return LRUCacheValuesView(self)

    def _values(self) -> Iterator[VT]:
        with self._mutex:
            for k in self:
                try:
                    yield self.data[k]
                except KeyError:  # pragma: no cover
                    pass

    def items(self) -> ItemsView[KT, VT]:
        return LRUCacheItemsView(self)

    def _items(self) -> Iterator[Tuple[KT, VT]]:
        with self._mutex:
            for k in self:
                try:
                    yield (k, self.data[k])
                except KeyError:  # pragma: no cover
                    pass

    def incr(self, key: KT, delta: int = 1) -> int:
        with self._mutex:
            # this acts as memcached does- store as a string, but return a
            # integer as long as it exists and we can cast it
            newval = int(self.data.pop(key)) + delta
            self[key] = cast(VT, str(newval))
            return newval

    def _new_lock(self) -> ContextManager:
        if self.thread_safety:
            return cast(ContextManager, threading.RLock())
        return DummyContext()

    def __getstate__(self) -> Mapping[str, Any]:
        d = dict(vars(self))
        d.pop('_mutex')
        return d

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__ = state
        self._mutex = self._new_lock()


class ManagedUserSet(FastUserSet):
    """A MutableSet that adds callbacks for when keys are get/set/deleted."""

    def on_key_get(self, key: Any) -> None:
        ...

    def on_key_set(self, key: Any) -> None:
        ...

    def on_key_del(self, key: Any) -> None:
        ...

    def on_clear(self) -> None:
        ...

    def __contains__(self, key: Any) -> bool:
        self.on_key_get(key)
        return super().__contains__(key)

    def add(self, key: Any) -> None:
        self.on_key_set(key)
        return super().add(key)

    def discard(self, key: Any) -> None:
        self.on_key_del(key)
        return super().discard(key)

    def clear(self) -> None:
        self.on_clear()
        return super().clear()

    def raw_update(self, *args: Any, **kwargs: Any) -> None:
        self.data.update(*args, **kwargs)


class ManagedUserDict(FastUserDict):
    """A UserDict that adds callbacks for when keys are get/set/deleted."""

    def on_key_get(self, key: Any) -> None:
        """Handle that key is being retrieved."""
        ...

    def on_key_set(self, key: Any, value: Any) -> None:
        """Handle that value for a key is being set."""
        ...

    def on_key_del(self, key: Any) -> None:
        """Handle that a key is deleted."""
        ...

    def on_clear(self) -> None:
        """Handle that the mapping is being cleared."""
        ...

    def __getitem__(self, key: Any) -> Any:
        self.on_key_get(key)
        return super().__getitem__(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        self.on_key_set(key, value)
        self.data[key] = value

    def __delitem__(self, key: Any) -> None:
        self.on_key_del(key)
        del self.data[key]

    def update(self, *args: Any, **kwargs: Any) -> None:
        for d in args:
            for key, value in d.items():
                self.on_key_set(key, value)
        for key, value in kwargs.items():
            self.on_key_set(key, value)
        self.data.update(*args, **kwargs)

    def clear(self) -> None:
        self.on_clear()
        self.data.clear()

    def raw_update(self, *args: Any, **kwargs: Any) -> None:
        self.data.update(*args, **kwargs)
