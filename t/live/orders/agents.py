import asyncio
from faust import uuid
from faust.types import FutureMessage, StreamT
from ..app import app, livecheck
from .livechecks import test_order
from .models import Order
from .topics import execution_topic, orders_topic


@app.agent(orders_topic)
async def create_order(orders: StreamT[Order]) -> None:
    async for order in orders:
        test = livecheck.current_test
        if test is not None:
            assert test.id == order.id
        print('1. ORDER SENT TO DB')
        await test_order.order_sent_to_db.send(order)

        def on_order_sent(fut: FutureMessage) -> None:
            print('2. ORDER SENT TO KAFKA')
            asyncio.ensure_future(
                test_order.order_sent_to_kafka.send())

        await execution_topic.send(key=order.id, value=order,
                                   callback=on_order_sent)
        print('3. ORDER SENT TO EXECUTION AGENT')
        await app.cache.client.sadd(f'order.{order.user_id}.orders', order.id)
        await test_order.order_cache_in_redis.send()
        print('4. ORDER CACHED IN REDIS')


@app.agent(execution_topic)
async def execute_order(orders: StreamT[Order]) -> None:
    async for order in orders:
        execution_id = uuid()
        if not order.fake:
            # bla bla bla
            pass
        await test_order.order_executed.send(execution_id)
        print('5. ORDER EXECUTED BY EXECUTION AGENT')
