from typing import Iterable
from faust.types.fixups import AppT, FixupT

__all__ = ['Fixup']


class Fixup(FixupT):

    def __init__(self, app: AppT) -> None:
        self.app = app

    def enabled(self) -> bool:
        return False

    def autodiscover_modules(self) -> Iterable[str]:
        return []

    def on_worker_init(self) -> None:
        ...
