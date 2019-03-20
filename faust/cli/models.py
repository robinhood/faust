"""Program ``faust models`` used to list models available."""
from operator import attrgetter
from typing import Any, Callable, Sequence, Type, cast
from faust.models import registry
from faust.types import ModelT
from .base import AppCommand, option

__all__ = ['models']


class models(AppCommand):
    """List all available models as a tabulated list."""

    title = 'Models'
    headers = ['name', 'help']
    sortkey = attrgetter('_options.namespace')

    options = [
        option('--builtins/--no-builtins', default=False),
    ]

    async def run(self, *, builtins: bool) -> None:
        self.say(
            self.tabulate(
                [self.model_to_row(model) for model in self.models(builtins)],
                headers=self.headers,
                title=self.title,
            ))

    def models(self, builtins: bool) -> Sequence[Type[ModelT]]:
        sortkey = cast(Callable[[Type[ModelT]], Any], self.sortkey)
        return [
            model for model in sorted(registry.values(), key=sortkey)
            if not model._options.namespace.startswith('@') or builtins
        ]

    def model_to_row(self, model: Type[ModelT]) -> Sequence[str]:
        return [
            self.bold_tail(self._name(model)),
            self.dark(self._help(model)),
        ]

    def _name(self, model: Type[ModelT]) -> str:
        return self.abbreviate_fqdn(model._options.namespace)

    def _help(self, model: Type[ModelT]) -> str:
        return model.__doc__ or '<N/A>'
