"""Fixups - Base implementation."""
from typing import Iterable
from faust.types import AppT, FixupT

__all__ = ['Fixup']


class Fixup(FixupT):
    """Base class for fixups.

    Fixups are things that hook into Faust to make things
    work for other frameworks, such as Django.
    """

    def __init__(self, app: AppT) -> None:
        self.app = app

    def enabled(self) -> bool:
        return False

    def autodiscover_modules(self) -> Iterable[str]:
        return []

    def on_worker_init(self) -> None:
        ...
