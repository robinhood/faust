"""Program ``faust model`` used to list details about a model."""
from datetime import datetime
from typing import Any, Sequence, Type

import click
from mode.utils import text

from faust.models import registry
from faust.types import FieldDescriptorT, ModelT
from faust.utils import terminal

from .base import AppCommand

__all__ = ['model']

#: Built-in types show as name only (e.g. str).
#: Other types will use full `repr()`.
BUILTIN_TYPES = frozenset({int, float, str, bytes, datetime})


class model(AppCommand):
    """Show model detail."""

    headers = ['field', 'type', 'default*']

    options = [
        click.argument('name'),
    ]

    async def run(self, name: str) -> None:
        try:
            model = registry[name]
        except KeyError:
            if '.' in name:
                raise self._unknown_model(name)
            if self.app.conf.origin:
                lookup = '.'.join([self.app.conf.origin, name])
            else:
                lookup = name
            try:
                model = registry[lookup]
            except KeyError:
                raise self._unknown_model(name, lookup=lookup)
        self.say(
            self.tabulate(
                self.model_fields(model),
                headers=[self.bold(h) for h in self.headers],
                title=self._name(model),
                wrap_last_row=False,
            ))

    def _unknown_model(self, name: str, *,
                       lookup: str = None) -> click.UsageError:
        lookup = lookup or name
        alt = text.didyoumean(
            registry,
            lookup,
            fmt_none=f'Please run `{self.prog_name} models` for a list.')
        return click.UsageError(f'No model {name!r}. {alt}')

    def model_fields(self, model: Type[ModelT]) -> terminal.TableDataT:
        return [self.field(getattr(model, k)) for k in model._options.fields]

    def field(self, field: FieldDescriptorT) -> Sequence[str]:
        return [
            field.field,
            self._type(field.type),
            self.dark('*' if field.required else repr(field.default)),
        ]

    def _type(self, typ: Any) -> str:
        return typ.__name__ if typ in BUILTIN_TYPES else repr(typ)

    def model_to_row(self, model: Type[ModelT]) -> Sequence[str]:
        return [
            self.bold_tail(self._name(model)),
            self.dark(self._help(model)),
        ]

    def _name(self, model: Type[ModelT]) -> str:
        return self.abbreviate_fqdn(model._options.namespace)

    def _help(self, model: Type[ModelT]) -> str:
        return model.__doc__ or '<N/A>'
