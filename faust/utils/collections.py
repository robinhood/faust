from typing import (
    Any, ItemsView, Iterator, KeysView, MutableMapping, ValuesView, cast,
)
from collections import UserDict

__all__ = ['FastUserDict', 'ManagedUserDict']


class FastUserDict(UserDict):

    data: MutableMapping

    # Mypy forces us to redefine these, for some reason:

    def __getitem__(self, key: Any) -> Any:
        return self.data[key]

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


class ManagedUserDict(FastUserDict):

    def on_key_get(self, key: Any) -> None:
        ...

    def on_key_set(self, key: Any, value: Any) -> None:
        ...

    def on_key_del(self, key: Any) -> None:
        ...

    def on_clear(self) -> None:
        ...

    def __getitem__(self, key: Any) -> Any:
        self.on_key_get(key)
        return self.data[key]

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
