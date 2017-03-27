from typing import ItemsView, KeysView, MutableMapping, ValuesView
from collections import UserDict


class FastUserDict(UserDict):

    data: MutableMapping

    def update(self, *args, **kwargs) -> None:
        # MutableMapping.update is slow
        self.data.update(*args, **kwargs)

    def clear(self) -> None:
        # MutableMapping.clear is slow
        self.data.clear()

    def items(self) -> ItemsView:
        # MutableMapping.items is slow
        return self.data.items()

    def keys(self) -> KeysView:
        # MutableMapping.keys is slow
        return self.data.keys()

    def values(self) -> ValuesView:
        # MutableMapping.values is slow
        return self.data.values()
