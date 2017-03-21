from typing import Mapping
from ..types import AppT
from ..utils.url import url_to_parts
from ..utils.imports import symbol_by_name
from .base import Transport

__all__ = ['TRANSPORTS', 'by_url', 'from_url']

TRANSPORTS: Mapping[str, str] = {
    'aiokafka': 'faust.transport.aiokafka:Transport'
}


def by_url(url: str) -> type:
    # we remove anything after ; so urlparse can recognize the url.
    scheme = url_to_parts(url.split(';', 1)[0]).scheme
    return symbol_by_name(scheme, aliases=TRANSPORTS)


def from_url(url: str, app: AppT, **kwargs) -> Transport:
    return by_url(url)(url, app, **kwargs)
