"""Python :pypi:`click` parameter types."""
from typing import Optional
import click
from click.types import ParamType, StringParamType
from yarl import URL

__all__ = [
    'WritableDirectory',
    'WritableFilePath',
    'TCPPort',
    'URLParam',
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


class TCPPort(click.IntRange):
    """CLI option: TCP Port (integer in range 1 - 65535)."""

    name = 'range[1-65535]'

    def __init__(self) -> None:
        super().__init__(1, 65535)


class URLParam(ParamType):
    """URL :pypi:`click` parameter type.

    Converts any string URL to :class:`yarl.URL`.
    """

    name = 'URL'

    _string_param: StringParamType

    def __init__(self) -> None:
        self._string_param = StringParamType()

    def convert(self,
                value: str,
                param: Optional[click.Parameter],
                ctx: Optional[click.Context]) -> URL:
        """Convert :class:`str` argument to :class:`yarl.URL`."""
        text_value = self._string_param.convert(value, param, ctx)
        return URL(text_value)

    def __repr__(self) -> str:
        return 'URL'
