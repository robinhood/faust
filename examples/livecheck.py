import asyncio
import faust
from faust.livecheck import LiveCheck
from faust.utils import uuid


class Order(faust.Record):
    id: str
    user_id: str
    side: str
    quantity: float
    price: float


app = faust.App(
    'orders',
    cache='redis://localhost:6379',
    origin='examples.livecheck',
    autodiscover=True,
)

livecheck = LiveCheck(
    'orders-livecheck',
    cache='redis://localhost:6379',
    origin='examples.livecheck',
    autodiscover=True,
)

orders_topic = app.topic('orders', value_type=Order)
execution_topic = app.topic('order-execution', value_type=Order)


@app.page('/order/')
async def order(web, request):
    order_id = uuid()
    user_id = uuid()
    order = Order(order_id, user_id, 'BUY', 1.0, 3.33)
    await test_order.maybe_trigger(order.id, order)
    await orders_topic.send(key=order_id, value=order)
    return web.json({'status': 'success'})


@app.agent(orders_topic)
async def create_order(orders):
    async for order in orders:
        print('RECEIVED ORDER')
        await test_order.order_sent_to_db.send(order.id, order)
        print('1. ORDER SENT TO DB')

        def on_order_sent(fut):
            print('2. ORDER SENT TO KAFKA')
            asyncio.ensure_future(
                test_order.order_sent_to_kafka.send(order.id, True))

        await execution_topic.send(key=order.id, value=order,
                                   callback=on_order_sent)
        print('ORDER SENT TO EXECUTION')
        await app.cache.client.sadd(f'order.{order.user_id}.orders', order.id)
        await test_order.order_cache_in_redis.send(order.id, True)
        print('3. ORDER CACHED IN REDIS')


@app.agent(execution_topic)
async def execute_order(orders):
    async for order in orders:
        execution_id = uuid()
        await test_order.order_executed.send(order.id, execution_id)
        print('4. ORDER EXECUTED')


@livecheck.case(warn_empty_after=300.0, probability=0.2)
class test_order(livecheck.Case):
    warn_empty_after = 300.0
    probability = 0.2

    order_sent_to_db: livecheck.Signal[str, Order]
    order_sent_to_kafka: livecheck.Signal[str, bool]
    order_cache_in_redis: livecheck.Signal[str, bool]
    order_executed: livecheck.Signal[str, str]

    async def run(self, order: Order):
        await self.order_sent_to_db.wait(timeout=30.0)

        await self.order_sent_to_kafka.wait(timeout=30.0)

        await self.order_cache_in_redis.wait(timeout=30.0)
        assert await livecheck.cache.client.sismember(
            f'order.{order.user_id}.orders', order.id)

        await self.order_executed.wait(timeout=30.0)


if __name__ == '__main__':
    app.main()
