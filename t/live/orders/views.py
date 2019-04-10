from faust import web
from faust import uuid
from ..app import app, livecheck
from .livechecks import test_order
from .models import Order
from .topics import orders_topic

blueprint = web.Blueprint('orders')

SIDE_SELL = 'sell'
SIDE_BUY = 'buy'
VALID_SIDES = {SIDE_SELL, SIDE_BUY}


@blueprint.route('/init/{side}/', name='init')
class OrderView(web.View):

    # First clients do a GET on /order/init/sell/
    # This endpoint will then do a POST to /order/create/

    async def get(self, request: web.Request, side: str) -> web.Response:
        order_id = uuid()
        user_id = uuid()
        side = side.lower()
        assert side in VALID_SIDES

        # This will trigger our test_order case with 50% probability.
        # If executed we pass along LiveCheck-Test-* related headers
        # that can track the test as it progresses through the system.
        # All intermediate systems must pass along these headers,
        # be it through HTTP or Kafka.

        # we pass the side here as a testing "contract"
        # the test will ensure that no system is changing the side
        # of this order from buy to sell.
        async with test_order.maybe_trigger(order_id, side=side) as test:
            fake = bool(int(request.query.get('fake', 0)))
            next_url = self.url_for('orders:create', 'http://localhost:6066')
            data = {
                'order_id': order_id,
                'user_id': user_id,
                'side': side,
                'did_execute_test': bool(test),
                'fake': fake,
            }
            async with app.http_client.post(next_url, json=data) as response:
                assert response.status == 200
                return self.bytes(await response.read(),
                                  content_type='application/json')


@blueprint.route('/create/', name='create')
class CreateOrderView(web.View):

    async def post(self, request: web.Request) -> web.Response:
        payload = await request.json()
        order_id = payload['order_id']
        user_id = payload['user_id']
        side = payload['side']
        fake = payload['fake']
        did_execute_test = payload['did_execute_test']

        if did_execute_test:
            # LiveCheck read the HTTP headers passed in this request
            # and set up a current_test environment.
            assert livecheck.current_test is not None
            # The id of the test execution should be the same as the order id.
            assert livecheck.current_test.id == order_id

        order = Order(order_id, user_id, side, 1.0, 3.33, fake=fake)
        await orders_topic.send(key=order_id, value=order)
        return self.json({'status': 'success'})
