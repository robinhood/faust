"""Storage registry."""
from ..utils.imports import FactoryMapping

__all__ = ['by_name', 'by_url']

STORES = FactoryMapping(
    memory='faust.stores.memory:Store',
    rocksdb='faust.stores.rocksdb:Store',
)
by_name = STORES.by_name
by_url = STORES.by_url
