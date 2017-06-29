from . import base
from ..utils.collections import FastUserDict
from ..utils.logging import get_logger

logger = get_logger(__name__)


class Store(base.Store, FastUserDict):
    logger = logger

    def on_init(self) -> None:
        self.data = {}
