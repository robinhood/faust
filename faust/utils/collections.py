from collections import UserDict, UserList
from typing import (
    Any, Deque, ItemsView, Iterator, KeysView,
    MutableMapping, MutableSet, ValuesView, cast,
)

__all__ = [
    'FastUserDict',
    'FastUserSet',
    'FastUserList',
    'ManagedUserDict',
    'ManagedUserSet',
]


class FastUserDict(UserDict):
    """Like UserDict but reimplements some methods for better performance."""

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
    ...  # UserList is already fast.


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
        """Called when a key is retrieved."""
        ...

    def on_key_set(self, key: Any, value: Any) -> None:
        """Called when a key is set."""
        ...

    def on_key_del(self, key: Any) -> None:
        """Called when a key is deleted."""
        ...

    def on_clear(self) -> None:
        """Called when the dict is cleared."""
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


__flake8_Deque_is_used: Deque  # XXX flake8 bug
