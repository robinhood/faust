import aiokafka
import asyncio
import faust
from typing import Pattern, Sequence
from .types import ConsumerCallback
from .utils.service import Service


class Consumer(Service):

    client_id = 'faust-{0}'.format(faust.__version__)
    _consumer: aiokafka.AIOKafkaConsumer

    def __init__(self,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Pattern = None,
                 callback: ConsumerCallback = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(loop=loop)
        assert callback is not None
        self.callback = callback
        self._consumer = aiokafka.AIOKafkaConsumer(
            topics=topics,
            pattern=pattern,
            loop=loop,
            client_id=self.client_id,
        )

    async def start(self) -> None:
        await self._consumer.start()
        self.add_poller(self.drain_events)

    async def stop(self) -> None:
        await self._consumer.stop()

    async def drain_events(self, *, timeout: float = 10.0) -> None:
        records = self._consumer.getmany(timeout_ms=timeout * 1000.0)
        callback = self.callback
        for tp, messages in records.items():
            for message in messages:
                await callback(tp.topic, tp.partition, message)
