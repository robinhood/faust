from typing import Any, Callable, Iterator, List
import faust
from faust.types import RecordMetadata
from . import config
from . import producer

__all__ = ['ProducerFun', 'StressApp', 'create_stress_app']

ProducerFun = Callable[[int], Iterator[RecordMetadata]]


class StressApp(faust.App):
    stress_producers: List[ProducerFun]
    count_received_events: int

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stress_producers = []
        self.count_received_events = 0

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

    @app.task
    async def report_progress(app):
        prev_count = 0
        times_failed = 0
        while not app.should_stop:
            severity = app.log.info
            await app._service.sleep(5.0)
            if app.count_received_events <= prev_count:
                times_failed += 1
                if times_failed > 6:
                    severity = app.log.error
                elif times_failed > 3:
                    severity = app.log.warn
                severity(f'{app.conf.id} not progressing: '
                         f'was {prev_count} now {app.count_received_events}')
            else:
                times_failed = 0
                severity(f'{app.conf.id} progressing: '
                         f'was {prev_count} now {app.count_received_events}')
            prev_count = app.count_received_events
    return app
