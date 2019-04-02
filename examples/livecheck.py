import asyncio
import faust
from faust import web
from faust.livecheck import LiveCheck, current_test
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

livecheck = LiveCheck.for_app(app)

orders_topic = app.topic('orders', value_type=Order)
execution_topic = app.topic('order-execution', value_type=Order)


orders = web.Blueprint('orders')


@orders.route('/init/', name='initiate')
class OrderView(web.View):

    # First clients do a GET on /order/init/
    # This endpoint will then do a POST to /order/create/

    async def get(self, request: web.Request) -> web.Response:
        order_id = uuid()
        user_id = uuid()

        # This will trigger our test_order case with 50% probability.
        # If executed we pass along LiveCheck-Test-* related headers
        # that can track the test as it progresses through the system.
        # All intermediate systems must pass along these headers,
        # be it through HTTP or Kafka.
        async with test_order.maybe_trigger(order_id) as test:
            next_url = self.url_for('orders:create', 'http://localhost:6066')
            data = {
                'order_id': order_id,
                'user_id': user_id,
                'did_execute_test': bool(test),
            }
            async with app.http_client.post(next_url, json=data) as response:
                assert response.status == 200
                return self.bytes(await response.read(),
                                  content_type='application/json')


@orders.route('/create/', name='create')
class CreateOrderView(web.View):

    async def post(self, request):
        payload = await request.json()
        order_id = payload['order_id']
        user_id = payload['user_id']
        did_execute_test = payload['did_execute_test']

        if did_execute_test:
            # LiveCheck read the HTTP headers passed in this request
            # and set up a current_test() environment.
            assert current_test() is not None
            # The id of the test execution should be the same as the order id.
            assert current_test().id == order_id

        order = Order(order_id, user_id, 'BUY', 1.0, 3.33)
        await orders_topic.send(key=order_id, value=order)
        return self.json({'status': 'success'})


app.web.blueprints.add('/order/', orders)


@app.agent(orders_topic)
async def create_order(orders):
    async for order in orders:
        test = current_test()
        if test is not None:
            assert test.id == order.id
        print('RECEIVED ORDER')
        await test_order.order_sent_to_db.send(order)
        print('1. ORDER SENT TO DB')

        def on_order_sent(fut):
            print('2. ORDER SENT TO KAFKA')
            asyncio.ensure_future(
                test_order.order_sent_to_kafka.send())

        await execution_topic.send(key=order.id, value=order,
                                   callback=on_order_sent)
        print('ORDER SENT TO EXECUTION')
        await app.cache.client.sadd(f'order.{order.user_id}.orders', order.id)
        await test_order.order_cache_in_redis.send()
        print('3. ORDER CACHED IN REDIS')


@app.agent(execution_topic)
async def execute_order(orders):
    async for order in orders:
        execution_id = uuid()
        await test_order.order_executed.send(execution_id)
        print('4. ORDER EXECUTED')


@livecheck.case(warn_empty_after=300.0, probability=0.5)
class test_order(livecheck.Case):

    order_sent_to_db: livecheck.Signal[str, Order]
    order_sent_to_kafka: livecheck.Signal[str, bool]
    order_cache_in_redis: livecheck.Signal[str, bool]
    order_executed: livecheck.Signal[str, str]

    async def run(self):
        order = await self.order_sent_to_db.wait(timeout=30.0)
        # order id matches test execution id
        assert order.id == self.execution.id

        await self.order_sent_to_kafka.wait(timeout=30.0)

        await self.order_cache_in_redis.wait(timeout=30.0)
        assert await livecheck.cache.client.sismember(
            f'order.{order.user_id}.orders', order.id)

        await self.order_executed.wait(timeout=30.0)


if __name__ == '__main__':
    app.main()
