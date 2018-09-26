"""Cache backend registry."""
from typing import Type
from mode.utils.imports import FactoryMapping
from faust.types.web import CacheBackendT

__all__ = ['by_name', 'by_url']

BACKENDS: FactoryMapping[Type[CacheBackendT]] = FactoryMapping(
    memory='faust.web.cache.backends.memory:CacheBackend',
    redis='faust.web.cache.backends.redis:CacheBackend',
    rediscluster='faust.web.cache.backends.redis:CacheBackend',
)
BACKENDS.include_setuptools_namespace('faust.web.cache')
by_name = BACKENDS.by_name
by_url = BACKENDS.by_url
