from uuid import uuid4
import faust
from faust.livecheck import Livecheck


class Order(faust.Record):
    id: str
    user_id: str
    side: str
    quantity: float
    price: float


app = faust.App('orders')
livecheck = Livecheck()

orders_topic = app.topic('orders', value_type=Order)
execution_topic = app.topic('order-execution', value_type=Order)


@app.agent(orders_topic)
async def create_order(orders):
    async for order in orders:
        await test_order.order_sent_to_db.send(order.id, order)
        await execution_topic.send(key=order.id, value=order,
                                   callback=test_order.order_sent_to_kafka)
        await app.cache.client.sadd(f'order.{order.user_id}.orders', order.id)
        await test_order.order_cache_in_redis.send(order.id, True)


@app.agent(execution_topic)
async def execute_order(orders):
    async for order in orders:
        execution_id = str(uuid4())
        await test_order.order_executed.send(order.id, execution_id)


@livecheck.case(warn_empty_after=300.0, probability=0.2)
class test_order(livecheck.Case):
    warn_empty_after = 300.0
    probability = 0.2

    order_sent_to_db: livecheck.Signal[str, Order]
    order_sent_to_kafka: livecheck.Signal[str, bool]
    order_cache_in_redis: livecheck.Signal[str, bool]

    async def run(self, order_id: str, order: Order):
        assert order.id == order_id
        await self.order_sent_to_db.wait(key=order_id, timeout=30.0)
        # assert await Order.objects.get(order_id=order.id)

        await self.order_sent_to_kafka.wait(key=order_id, timeout=30.0)

        await self.order_cache_in_redis.wait(key=order_id, timeout=30.0)
        assert await app.cache.client.sismember(
            f'order.{order.user_id}.orders', order.id)
