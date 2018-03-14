"""Transport using aiokafka consumer and confluent-kafka producer."""
from typing import ClassVar, Type
from faust.types.transports import ConsumerT, ProducerT
from . import aiokafka
from . import confluent

__all__ = ['Consumer', 'Producer', 'Transport']

Consumer = aiokafka.Consumer
Producer = confluent.Producer


class Transport(aiokafka.Transport):
    """Transport using aiokafka consumer and confluent-kafka producer."""

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    driver_version = ' '.join([
        aiokafka.Transport.driver_version,
        confluent.Transport.driver_version,
    ])
