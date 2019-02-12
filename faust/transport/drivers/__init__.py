"""Transport registry."""
from typing import Type
from mode.utils.imports import FactoryMapping
from faust.types import TransportT

__all__ = ['by_name', 'by_url']

TRANSPORTS: FactoryMapping[Type[TransportT]] = FactoryMapping(
    aiokafka='faust.transport.drivers.aiokafka:Transport',
    confluent='faust.transport.drivers.confluent:Transport',
    kafka='faust.transport.drivers.aiokafka:Transport',
    memory='faust.transport.drivers.memory:Transport',
)
TRANSPORTS.include_setuptools_namespace('faust.transports')
by_name = TRANSPORTS.by_name
by_url = TRANSPORTS.by_url
