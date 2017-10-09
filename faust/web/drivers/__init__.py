"""Web server driver registry."""
from typing import Type
from ..base import Web
from ...utils.imports import FactoryMapping

__all__ = ['by_name', 'by_url']

DRIVERS: FactoryMapping[Type[Web]] = FactoryMapping(
    aiohttp='faust.web.drivers.aiohttp:Web',
)
DRIVERS.include_setuptools_namespace('faust.web.drivers')
by_name = DRIVERS.by_name
by_url = DRIVERS.by_url
