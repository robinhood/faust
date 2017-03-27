"""Transport registry."""
from typing import Any, Mapping, Type
from ..types import AppT, TransportT
from ..utils.urls import url_to_parts
from ..utils.imports import SymbolArg, symbol_by_name

__all__ = ['TRANSPORTS', 'by_name', 'by_url', 'from_url']

#: This contains a mapping of transport aliases to class path.
TRANSPORTS: Mapping[str, str] = {
    'aiokafka': 'faust.transport.aiokafka:Transport',
    'kafka': 'faust.transport.aiokafka:Transport',
}


def by_url(url: str) -> Type:
    """Get the transport class associated with URL."""
    # we remove anything after ; so urlparse can recognize the url.
    return by_name(url_to_parts(url.split(';', 1)[0]).scheme)


def by_name(name: SymbolArg) -> TransportT:
    return symbol_by_name(name, aliases=TRANSPORTS)


def from_url(url: str, app: AppT, **kwargs: Any) -> TransportT:
    """Factory: Instantiate transport class associated with URL."""
    return by_url(url)(url, app, **kwargs)
