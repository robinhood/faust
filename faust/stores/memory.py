from ..utils.collections import FastUserDict
from . import base


class Store(base.Store, FastUserDict):

    def on_init(self) -> None:
        self.data = {}
