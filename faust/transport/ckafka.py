from typing import ClassVar, Type
from . import aiokafka
from . import confluent
from ..types.transports import ConsumerT, ProducerT

__all__ = ['Consumer', 'Producer', 'Transport']

Consumer = aiokafka.Consumer
Producer = aiokafka.Producer


class Transport(aiokafka.Transport):

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    driver_version = ' '.join([
        aiokafka.Transport.driver_version,
        confluent.Transport.driver_version,
    ])
