import aiokafka
import asyncio
import faust
from .types import ConsumerCallback, Topic
from .utils.service import Service


class Consumer(Service):

    topic: Topic
    client_id = 'faust-{0}'.format(faust.__version__)
    _consumer: aiokafka.AIOKafkaConsumer

    def __init__(self,
                 *,
                 topic: Topic = None,
                 callback: ConsumerCallback = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(loop=loop)
        assert callback is not None
        self.callback = callback
        self.topic = topic
        if self.topic.topics and self.topic.pattern:
            raise TypeError('Topic can specify either topics or pattern')
        self._consumer = aiokafka.AIOKafkaConsumer(
            topics=self.topic.topics,
            pattern=self.topic.pattern,
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
