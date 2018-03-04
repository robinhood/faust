"""Transport registry."""
from typing import Iterator, Type
from mode.utils.imports import FactoryMapping
from faust.types.fixups import AppT, FixupT

__all__ = ['by_name', 'by_url', 'fixups']

FIXUPS: FactoryMapping[Type[FixupT]] = FactoryMapping(
    django='faust.fixups.django:Fixup',
)
FIXUPS.include_setuptools_namespace('faust.fixups')
by_name = FIXUPS.by_name
by_url = FIXUPS.by_url


def fixups(app: AppT) -> Iterator[FixupT]:
    for Fixup in FIXUPS.iterate():
        fixup = Fixup(app)
        if fixup.enabled():
            yield fixup
