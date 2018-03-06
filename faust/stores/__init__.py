"""Storage registry."""
from typing import Type
from mode.utils.imports import FactoryMapping
from faust.types import StoreT

__all__ = ['by_name', 'by_url']

STORES: FactoryMapping[Type[StoreT]] = FactoryMapping(
    memory='faust.stores.memory:Store',
    rocksdb='faust.stores.rocksdb:Store',
)
STORES.include_setuptools_namespace('faust.stores')
by_name = STORES.by_name
by_url = STORES.by_url
