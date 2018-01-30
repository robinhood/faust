"""Transport registry."""
from typing import Type
from ..types import TransportT
from ..utils.imports import FactoryMapping

__all__ = ['by_name', 'by_url']

TRANSPORTS: FactoryMapping[Type[TransportT]] = FactoryMapping(
    ckafka='faust.transport.ckafka:Transport',
    aiokafka='faust.transport.aiokafka:Transport',
    confluent='faust.transport.confluent:Transport',
    kafka='faust.transport.aiokafka:Transport',
    memory='faust.transport.memory:Transport',
)
TRANSPORTS.include_setuptools_namespace('faust.transports')
by_name = TRANSPORTS.by_name
by_url = TRANSPORTS.by_url
