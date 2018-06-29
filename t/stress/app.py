from typing import Any, Callable, Iterator, List
import faust
from faust.types import RecordMetadata
from . import config
from . import producer

__all__ = ['ProducerFun', 'StressApp', 'create_stress_app']

ProducerFun = Callable[[int], Iterator[RecordMetadata]]


class StressApp(faust.App):
    stress_producers: List[ProducerFun]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stress_producers = []

    def register_stress_producer(self, fun: ProducerFun):
        self.stress_producers.append(fun)
        return fun


def create_stress_app(name, origin, **kwargs: Any) -> StressApp:
    app = StressApp(
        name,
        origin=origin,
        broker=config.broker,
        store=config.store,
        topic_partitions=config.topic_partitions,
        loghandlers=config.loghandlers(),
        autodiscover=True,
        **kwargs)
    producer.install_produce_command(app)
    return app
