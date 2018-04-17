import faust
import pytest


class Order(faust.Record, serializer='json'):
    account_id: str
    product_id: str
    amount: int
    price: float


app = faust.App('test-example')
orders_topic = app.topic('orders', value_type=Order)

# order count within the last hour (window is a 1-hour TumblingWindow).
orders_for_account = app.Table(
    'order-count-by-account', default=int,
).tumbling(3600).relative_to_stream()


@app.agent(orders_topic)
async def process_order(orders):
    async for order in orders.group_by(Order.account_id):
        orders_for_account[order.account_id] += 1
        yield order


@pytest.mark.asyncio()
async def test_process_order():
    app.finalize()
    app.conf.store = 'memory://'
    app.flow_control.resume()
    async with process_order.test_context() as agent:
        order = Order(account_id='1', product_id='2', amount=1, price=300)
        event = await agent.put(order)

        # windowed table: we select window relative to the current event
        assert orders_for_account['1'].current(event) == 1

        # in the window 3 hours ago there were no orders:
        assert not orders_for_account['1'].delta(3600 * 3, event)
