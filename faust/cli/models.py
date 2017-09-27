"""Program ``faust model`` used to list models.

.. program:: faust models
"""
from operator import attrgetter
from typing import Sequence, Type
import click
from .base import AppCommand
from ..models import registry
from ..types import ModelT

__all__ = ['models']


class models(AppCommand):
    """List available models."""

    title = 'Models'
    headers = ['name', 'help']
    sortkey = attrgetter('_options.namespace')

    options = [
        click.option('--builtins/--no-builtins', default=False),
    ]

    async def run(self, *, builtins: bool) -> None:
        self.say(self.tabulate(
            [self.model_to_row(model) for model in self.models(builtins)],
            headers=self.headers,
            title=self.title,
        ))

    def models(self, builtins: bool) -> Sequence[Type[ModelT]]:
        return [
            model
            for model in sorted(registry.values(), key=self.sortkey)
            if not model._options.namespace.startswith('@') or builtins
        ]

    def model_to_row(self, model: Type[ModelT]) -> Sequence[str]:
        return [
            self.bold_tail(self._name(model)),
            self.colored('autoblack', self._help(model)),
        ]

    def _name(self, model: Type[ModelT], short: str = '@') -> str:
        return self.abbreviate_fqdn(model._options.namespace)

    def _help(self, model: Type[ModelT]) -> str:
        return model.__doc__ or '<N/A>'
