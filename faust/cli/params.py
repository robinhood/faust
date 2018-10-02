from typing import Any, Iterable, Optional
import click

__all__ = [
    'WritableDirectory',
    'WritableFilePath',
    'CaseInsensitiveChoice',
    'TCPPort',
]


WritableDirectory = click.Path(
    exists=False,      # create if needed,
    file_okay=False,   # must be directory,
    dir_okay=True,     # not file;
    writable=True,     # and read/write access.
    readable=True,     #
)

WritableFilePath = click.Path(
    exists=False,       # create if needed,
    file_okay=True,     # must be file,
    dir_okay=False,     # not directory;
    writable=True,      # and read/write access.
    readable=True,      #
)


class CaseInsensitiveChoice(click.Choice):
    """Case-insensitive version of :class:`click.Choice`."""

    def __init__(self, choices: Iterable[Any]) -> None:
        self.choices = [str(val).lower() for val in choices]

    def convert(self,
                value: str,
                param: Optional[click.Parameter],
                ctx: Optional[click.Context]) -> Any:
        if value.lower() in self.choices:
            return value
        return super().convert(value, param, ctx)


class TCPPort(click.IntRange):
    """CLI option: TCP Port (integer in range 1 - 65535)."""

    name = 'range[1-65535]'

    def __init__(self) -> None:
        super().__init__(1, 65535)
