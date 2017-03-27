"""State store registry."""
from typing import Any, Mapping, Type
from ..types import AppT, StoreT
from ..utils.urls import url_to_parts
from ..utils.imports import SymbolArg, symbol_by_name

__all__ = ['STORES', 'by_name', 'by_url', 'from_url']

#: This contains a mapping of transport aliases to class path.
STORES: Mapping[str, str] = {
    'memory': 'faust.stores.memory:Store',
}


def by_url(url: str) -> Type:
    """Get the storage class associated with URL."""
    # we remove anything after ; so urlparse can recognize the url.
    return by_name(url_to_parts(url.split(';', 1)[0]).scheme)


def by_name(name: SymbolArg) -> StoreT:
    return symbol_by_name(name, aliases=STORES)


def from_url(url: str, app: AppT, **kwargs: Any) -> StoreT:
    """Factory: Instantiate storage class associated with URL."""
    return by_url(url)(url, app, **kwargs)
