"""Transport registry."""
from typing import Mapping
from ..types import AppT
from ..utils.url import url_to_parts
from ..utils.imports import symbol_by_name
from .base import Transport

__all__ = ['TRANSPORTS', 'by_url', 'from_url']

#: This contains a mapping of transport aliases to class path.
TRANSPORTS: Mapping[str, str] = {
    'aiokafka': 'faust.transport.aiokafka:Transport'
}


def by_url(url: str) -> type:
    """Get the transport class associated with URL."""
    # we remove anything after ; so urlparse can recognize the url.
    scheme = url_to_parts(url.split(';', 1)[0]).scheme
    return symbol_by_name(scheme, aliases=TRANSPORTS)


def from_url(url: str, app: AppT, **kwargs) -> Transport:
    """Factory: Instantiate transport class associated with URL."""
    return by_url(url)(url, app, **kwargs)
