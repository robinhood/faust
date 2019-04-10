from typing import AsyncIterator
from faust.livecheck import Case, Signal
from ..app import livecheck
from .models import Order


@livecheck.case(warn_stalled_after=5.0, frequency=0.5, probability=0.5)
class test_order(Case):

    order_sent_to_db: Signal[Order]
    order_sent_to_kafka: Signal[None]
    order_cache_in_redis: Signal[None]
    order_executed: Signal[str]

    async def run(self, side: str) -> None:
        # 1) wait for order to be sent to database.
        order = await self.order_sent_to_db.wait(timeout=30.0)

        # contract:
        #   order id matches test execution id
        #   order.side matches test argument side.
        assert order.id == self.current_execution.test.id
        assert order.side == side

        # 2) wait for order to be sent to Kafka
        await self.order_sent_to_kafka.wait(timeout=30.0)

        # 3) wait for redis index to be updated.
        await self.order_cache_in_redis.wait(timeout=30.0)
        #  make sure it's now actually in redis
        assert await livecheck.cache.client.sismember(
            f'order.{order.user_id}.orders', order.id)

        # 4) wait for execution agent to execute the order.
        await self.order_executed.wait(timeout=30.0)

    async def make_fake_request(self) -> AsyncIterator:
        await self.get_url('http://localhost:6066/order/init/sell/?fake=1')
